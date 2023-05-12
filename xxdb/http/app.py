from functools import partial
import asyncio

from starlette.applications import Starlette
from prometheus_client import make_asgi_app

from xxdb.engine.db import DB
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
    return "pong"


def create_app(config: AppConfig) -> Starlette:
    app = Starlette()
    app.add_route("/ping", ping, methods=["GET"])

    for db in config.databases:
        db_instance = DB(db.name, db.path, db.settings)
        DATABASE[db.name] = db_instance
        # TODO: move flush_db_periodically to DBInstance
        app.add_event_handler(
            "startup", partial(asyncio.create_task, flush_db_periodically(db_instance, db.flush_period))
        )
        app.add_event_handler("shutdown", partial(asyncio.create_task, close_db(db_instance)))
        assert db_instance.data_schema is not None

        app.add_websocket_route("/ws", getEndpoint(db_instance))
        app.mount("/rest", rest_router)

        if (reg := db_instance.prom_registry) is not None:
            _metrics_app = make_asgi_app(reg)
            app.mount(f"/metrics/{db.name}", _metrics_app)

    return app
