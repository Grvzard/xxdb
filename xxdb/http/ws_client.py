import logging
import asyncio

from aiohttp import ClientSession, WSMsgType

from xxdb.engine.capped_array import CappedArray
from xxdb.engine.schema import Schema, SchemaConfig
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
        self._dsn = dsn + "/ws" + dbname
        self._session = None
        self._ws_lock = asyncio.Lock()
        self._ws = None
        self._schema = None
        self._idle_cnt = 0

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
                self._schema = Schema(SchemaConfig.parse_raw(pb_resp.auth_payload))
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
        pb_req.command = "_heartbeat"
        payload = pb_req.SerializeToString()

        while True:
            assert self._ws is not None
            if self._idle_cnt >= self.HEARTBEAT_INTERVAL:
                async with self._ws_lock:
                    try:
                        await self._ws.send_bytes(payload)
                        await self._ws.receive_bytes()
                    except Exception as exc:
                        logger.error(f'Failed to send heartbeat due to: {exc!r}')
                    else:
                        self._idle_cnt = 0
                        logger.debug("sent heartbeat")
            await asyncio.sleep(1)
            self._idle_cnt += 1

    async def close(self):
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._schema = None
        logger.info("client connection closed")

    async def get(self, key: int) -> list[dict] | None:
        ws = self._ws
        schema = self._schema
        assert schema is not None
        assert ws is not None

        pb_req = pb.CommonRequest()
        pb_req.command = "get"
        pb_req.op_key = str(key)

        self._idle_cnt = 0
        for _ in range(2):
            try:
                async with self._ws_lock:
                    await ws.send_bytes(pb_req.SerializeToString())
                    msg = await ws.receive()
                    break
            except Exception:
                await self.connect()
        else:
            raise Exception("failed to connect to server")

        if msg.type == WSMsgType.BINARY:
            pb_resp = pb.CommonResponse()
            pb_resp.ParseFromString(msg.data)
            if pb_resp.status == pb.CommonResponse.Status.OK:
                data_list = [schema.unpack(data) for data in CappedArray.RetrieveFromRaw(pb_resp.get_payload)]
                return data_list
            else:
                logger.debug(pb_resp.status)
                logger.debug(pb_resp.error_payload)
                return None

        elif msg.type == WSMsgType.ERROR:
            logger.error('ws connection closed with exception %s' % ws.exception())
            await self.close()

        return None

    async def put(self, key: int, value: dict) -> bool:
        ws = self._ws
        schema = self._schema
        assert ws is not None
        assert schema is not None

        pb_req = pb.CommonRequest()
        pb_req.command = "put"
        pb_req.op_key = str(key)
        pb_req.put_payload = schema.pack(value)

        self._idle_cnt = 0
        # await self._ws_lock.acquire()
        # self._ws_lock.release()
        async with self._ws_lock:
            await ws.send_bytes(pb_req.SerializeToString())
            msg = await ws.receive()

        if msg.type == WSMsgType.BINARY:
            pb_resp = pb.CommonResponse()
            pb_resp.ParseFromString(msg.data)
            if pb_resp.status == pb.CommonResponse.Status.OK:
                return True
            else:
                raise Exception(pb_resp.error_payload)

        elif msg.type == WSMsgType.ERROR:
            print('ws connection closed with exception %s' % ws.exception())

        return False
