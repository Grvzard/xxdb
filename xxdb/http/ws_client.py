import logging
import asyncio
from contextlib import suppress, AsyncExitStack
from tenacity import retry, stop_after_attempt, wait_fixed

import websockets

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
        self._ws_lock = asyncio.Lock()
        self._ws = None
        self._stack = AsyncExitStack()
        self._schema: None | Schema = None
        self._idle_cnt = 0
        self._heartbeat_task = None

    async def connect(self):
        assert self._ws is None
        await self._reconnect()
        assert self._ws is not None

        pb_req = pb.AuthRequest(dbname=self._dbname, payload="test")

        await self._ws.send(pb_req.SerializeToString())
        msg = await self._ws.recv()
        pb_resp = pb.CommonResponse()

        try:
            pb_resp.ParseFromString(msg)
            if pb_resp.status == pb.CommonResponse.Status.OK:
                logger.info("auth success")
                self._schema = Schema(SchemasConfig.parse_raw(pb_resp.auth_payload))
                logger.info("client connection opened")
                self._create_heartbeat_task()
            else:
                raise Exception(pb_resp.auth_payload)

        except Exception as exc:
            logger.error(exc)

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(6), reraise=True)
    async def _reconnect(self):
        with suppress(Exception):
            await self._stack.aclose()
        try:
            ws = await self._stack.enter_async_context(websockets.connect(self._dsn))
            logger.info("reconnect success")
        except Exception as exc:
            logger.error(f"reconnect failed: {exc}")
            raise exc
        else:
            async with self._ws_lock:
                self._ws = ws
                self._idle_cnt = 0
            return

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
                        await self._ws.send(payload)
                        await self._ws.recv()
                    logger.debug("sent heartbeat")
                    self._idle_cnt = 0
                else:
                    self._idle_cnt += 1
            except asyncio.CancelledError:
                logger.error("heartbeat task cancelled")
                break
            except Exception as exc:
                logger.error(f'Failed to send heartbeat due to: {exc!r}')
            finally:
                await asyncio.sleep(1)

    async def close(self):
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                logger.info("closing heartbeat task")
                await self._heartbeat_task
        # if self._ws is not None:
        #     await self._ws.close()
        #     self._ws = None
        with suppress(Exception):
            await self._stack.aclose()
        self._schema = None
        logger.info("xxdb ws_client connection closed")

    async def _common_request(self, pb_req: pb.CommonRequest) -> pb.CommonResponse:
        payload = pb_req.SerializeToString()

        self._idle_cnt = 0
        for _ in range(2):
            try:
                async with self._ws_lock:
                    assert self._ws is not None
                    await self._ws.send(payload)
                    msg = await self._ws.recv()
            except Exception as exc:
                logger.error(f"failed to send request: {exc}")
                logger.error("try to reconnect")
                await self._reconnect()
            else:
                pb_resp = pb.CommonResponse()
                pb_resp.ParseFromString(msg)
                return pb_resp

        raise Exception("failed to send request")

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
