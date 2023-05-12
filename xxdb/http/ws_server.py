import logging
from functools import partial

from xxdb.engine.db import DB
from .pb import message_pb2 as pb

logger = logging.getLogger(__name__)


def auth_check(data: bytes) -> bool:
    return True


def getEndpoint(db: DB):
    return partial(ws_handler, db=db)


async def ws_handler(ws, db: DB):
    schema = db._meta.data_schema
    assert schema is not None
    await ws.accept()

    auth_msg = await ws.receive_bytes()
    auth_req = pb.AuthRequest()
    try:
        auth_req.ParseFromString(auth_msg)
    except Exception as exc:
        logger.error(f"parse auth request failed: {exc}, (payload): {auth_msg}")
        await ws.close()
        return

    auth_resp = pb.CommonResponse()
    if auth_check(auth_req.payload):
        auth_resp.status = pb.CommonResponse.Status.OK
        auth_resp.auth_payload = schema.json()
        await ws.send_bytes(auth_resp.SerializeToString())
    else:
        auth_resp.status = pb.CommonResponse.Status.FAILED
        auth_resp.auth_payload = "auth failed"
        await ws.send_bytes(auth_resp.SerializeToString())
        logger.info(f"auth failed: {auth_req.payload}")
        await ws.close()
        return

    async for msg in ws.iter_bytes():
        pb_resp = pb.CommonResponse()
        try:
            pb_req = pb.CommonRequest()
            pb_req.ParseFromString(msg)
        except Exception as exc:
            logger.error(f"parse request failed: {exc}, (payload): {msg}")
            pb_resp.status = pb.CommonResponse.Status.ERROR
            pb_resp.error_payload = "parse request failed"
        else:
            if pb_req.command == "put":
                key = int(pb_req.op_key)
                await db.put(key, pb_req.put_payload)
                pb_resp.status = pb.CommonResponse.Status.OK
            elif pb_req.command == "get":
                data = await db.get(int(pb_req.op_key), mode="raw")
                pb_resp.status = pb.CommonResponse.Status.OK
                pb_resp.get_payload = data
            elif pb_req.command == "_heartbeat":
                pb_resp.status = pb.CommonResponse.Status.OK
            else:
                pb_resp.status = pb.CommonResponse.Status.FAILED
                pb_resp.error_payload = "unknown command"

        await ws.send_bytes(pb_resp.SerializeToString())
