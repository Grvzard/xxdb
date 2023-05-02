from functools import partial
import asyncio

from fastapi import FastAPI
from prometheus_client import make_asgi_app

from xxdb.engine.db import DB
from .config import ApiConfig
from .database import database
from . import api

__all__ = ("create_app",)


async def close_db():
    for db in database.values():
        await db.close()


async def flush_db_periodically(db: DB, seconds: int):
    while 1:
        await asyncio.sleep(seconds)
        await db.flush()


def create_app(config: ApiConfig) -> FastAPI:
    app = FastAPI()

    app.include_router(api.router)

    for db in config.databases:
        db_instance = DB(db.name, db.path, db.settings)
        database[db.name] = db_instance
        app.add_event_handler(
            "startup", partial(asyncio.create_task, flush_db_periodically(db_instance, db.flush_period))
        )
        if reg := db_instance.prom_registry:
            _metrics_app = make_asgi_app(reg)
            app.mount(f"/metrics/{db.name}", _metrics_app)

    app.add_event_handler("shutdown", close_db)

    return app
