import logging
import asyncio

from aiohttp import ClientSession

from xxdb.engine.capped_array import CappedArray
from xxdb.engine.schema import Schema, SchemasConfig
from xxdb.http.pb import message_pb2 as pb

logger = logging.getLogger(__name__)


# TODO: add websocket_pool
class Client:
    HEARTBEAT_INTERVAL = 30

    def __init__(self, dsn: str, dbname: str):
        self._dbname = dbname
        # TODO: use regex to check dsn
        if dsn[-1] != '/':
            dsn += '/'
        self._dsn = dsn + "ws/" + dbname
        self._session = None
        self._ws_lock = asyncio.Lock()
        self._ws = None
        self._schema: None | Schema = None
        self._idle_cnt = 0
        self._heartbeat_task = None

    async def connect(self):
        await self.close()
        if self._session is None:
            self._session = ClientSession()

        ws = await self._session.ws_connect(self._dsn)
        pb_req = pb.AuthRequest(dbname=self._dbname, payload="test")

        await ws.send_bytes(pb_req.SerializeToString())
        msg = await ws.receive_bytes()
        pb_resp = pb.CommonResponse()

        try:
            pb_resp.ParseFromString(msg)
            if pb_resp.status == pb.CommonResponse.Status.OK:
                logger.info("auth success")
                self._schema = Schema(SchemasConfig.parse_raw(pb_resp.auth_payload))
                self._ws = ws
                logger.info("client connection opened")
                self._create_heartbeat_task()
            else:
                raise Exception(pb_resp.auth_payload)

        except Exception as exc:
            logger.error(exc)

    def _create_heartbeat_task(self):
        self._heartbeat_task = asyncio.create_task(self._send_heartbeat())

    async def _send_heartbeat(self) -> None:
        pb_req = pb.CommonRequest()
        pb_req.command = pb.CommonRequest.Command.HEARTBEAT
        payload = pb_req.SerializeToString()

        while True:
            try:
                if self._idle_cnt >= self.HEARTBEAT_INTERVAL:
                    assert self._ws is not None
                    async with self._ws_lock:
                        await self._ws.send_bytes(payload)
                        await self._ws.receive_bytes()
                await asyncio.sleep(1)
                self._idle_cnt += 1
            except asyncio.CancelledError:
                logger.error("heartbeat task cancelled")
                break
            except Exception as exc:
                logger.error(f'Failed to send heartbeat due to: {exc!r}')
            else:
                self._idle_cnt = 0
                logger.debug("sent heartbeat")

    async def close(self):
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            await self._heartbeat_task
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._schema = None
        logger.info("xxdb ws_client connection closed")

    async def _common_request(self, pb_req: pb.CommonRequest) -> pb.CommonResponse:
        ws = self._ws
        assert ws is not None

        payload = pb_req.SerializeToString()

        self._idle_cnt = 0
        for _ in range(2):
            try:
                async with self._ws_lock:
                    await ws.send_bytes(payload)
                    msg = await ws.receive_bytes()
                    break
            except Exception:
                logger.error("failed to send request, try to reconnect")
                await self.connect()
        else:
            raise Exception("failed to connect to server")

        pb_resp = pb.CommonResponse()
        pb_resp.ParseFromString(msg)
        return pb_resp

    async def get(self, key: int) -> list[dict] | None:
        assert self._schema is not None

        pb_req = pb.CommonRequest()
        pb_req.command = pb.CommonRequest.Command.GET
        pb_req.op_key = str(key)

        pb_resp = await self._common_request(pb_req)

        if pb_resp.status == pb.CommonResponse.Status.OK:
            data_list = [self._schema.unpack(data) for data in CappedArray.RetrieveFromRaw(pb_resp.get_payload)]
            return data_list
        else:
            logger.debug(pb_resp.status)
            logger.debug(pb_resp.error_payload)
            return None

    async def put(
        self,
        key: int,
        value: dict,
        *,
        schema: str,
    ) -> bool:
        pb_req = pb.CommonRequest()
        pb_req.command = pb.CommonRequest.Command.PUT
        pb_req.op_key = str(key)
        assert self._schema is not None
        pb_req.put_payload = self._schema.pack(value, schema=schema)

        pb_resp = await self._common_request(pb_req)

        if pb_resp.status == pb.CommonResponse.Status.OK:
            return True
        else:
            raise Exception(pb_resp.error_payload)

    async def bulk_put(self, kv_list: list[tuple[int, dict]], *, schema: str):
        assert self._schema is not None
        pb_req = pb.CommonRequest()
        pb_req.command = pb.CommonRequest.Command.BULK_PUT
        for k, v in kv_list:
            put_payload = pb_req.bulkput_payload.add()
            put_payload.key = str(k)
            put_payload.value = self._schema.pack(v, schema=schema)

        pb_resp = await self._common_request(pb_req)

        if pb_resp.status == pb.CommonResponse.Status.OK:
            return True
        else:
            raise Exception(pb_resp.error_payload)
