from functools import partial
import asyncio
from pathlib import Path

from starlette.applications import Starlette
from starlette.responses import Response
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from xxdb.engine.db import DB, create as db_create
from .config import AppConfig as AppConfig
from .database import DATABASE
from .ws_server import getEndpoint
from .rest_server import router as rest_router

__all__ = ("create_app", "AppConfig")


bg_tasks = set()


async def close_db(db: DB):
    for bg_task in bg_tasks:
        bg_task.cancel()  # type: ignore
        try:
            await bg_task  # type: ignore
        except asyncio.CancelledError:
            ...
    await db.close()


def flush_db_periodically(db: DB, seconds: int):
    async def func():
        while 1:
            try:
                await asyncio.gather(
                    asyncio.sleep(seconds),
                    db.flush(),
                )
            except asyncio.CancelledError:
                break

    bg_tasks.add(asyncio.create_task(func()))


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
        if db.path:
            meta_dpath = Path(db.path)
            meta_fpath = meta_dpath / f"{db.name}.meta.xxdb"
            if not meta_fpath.exists():
                raise Exception()
        elif config.auto_create and db.cfg:
            cfg_fpath = Path(db.cfg)
            meta_dpath = db_create(db.name, cfg_fpath=cfg_fpath)
        else:
            raise Exception()

        db_instance = DB(db.name, meta_dpath, db.settings)
        DATABASE[db.name] = db_instance
        app.add_event_handler("startup", partial(flush_db_periodically, db_instance, db.flush_period))
        app.add_event_handler("shutdown", partial(close_db, db_instance))
        assert db_instance.data_schemas is not None

        app.add_websocket_route(f"/ws/{db.name}", getEndpoint(db_instance))

        if (reg := db_instance.prom_registry) is not None:
            _metrics_app = make_asgi_app(reg)
            app.mount(f"/metrics/{db.name}", _metrics_app)

    return app
