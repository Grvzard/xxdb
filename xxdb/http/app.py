from functools import partial
import asyncio
from pathlib import Path

from starlette.applications import Starlette
from starlette.responses import Response
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from xxdb.engine.db import DB, create as db_create, DbMeta
from .config import AppConfig as AppConfig
from .database import DATABASE
from .ws_server import getEndpoint
from .rest_server import router as rest_router

__all__ = ("create_app", "AppConfig")


async def close_db(db: DB):
    await db.close()


async def flush_db_periodically(db: DB, seconds: int):
    while 1:
        await asyncio.sleep(seconds)
        await db.flush()


async def ping(request):
    _ = request
    return Response("pong", media_type="text/plain")


def create_app(config: AppConfig) -> Starlette:
    app = Starlette()

    app.add_middleware(TrustedHostMiddleware, allowed_hosts=config.allowed_hosts)
    app.add_middleware(CORSMiddleware, allow_origins=config.cors_origins)

    app.add_route("/ping", ping, methods=["GET"])

    app.mount("/rest", rest_router)

    for db in config.databases:
        dat_path = Path(f"{db.path}/{db.name}.dat.xxdb")
        if not dat_path.exists():
            if not (config.auto_create and db.cfg):
                raise Exception()
            meta = DbMeta.parse_file(db.cfg)
            db_create(db.name, meta=meta, datadir=db.path)

        db_instance = DB(db.name, db.path, db.settings)
        DATABASE[db.name] = db_instance
        app.add_event_handler(
            "startup", partial(asyncio.create_task, flush_db_periodically(db_instance, db.flush_period))
        )
        app.add_event_handler("shutdown", partial(asyncio.create_task, close_db(db_instance)))
        assert db_instance.data_schemas is not None

        app.add_websocket_route(f"/ws/{db.name}", getEndpoint(db_instance))

        if (reg := db_instance.prom_registry) is not None:
            _metrics_app = make_asgi_app(reg)
            app.mount(f"/metrics/{db.name}", _metrics_app)

    return app
