from functools import partial
import asyncio

from fastapi import FastAPI

from xxdb.engine.db import create, DB
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
        create(db.name, datadir=db.path)
        database[db.name] = DB(db.name, db.path, db.settings)
        app.add_event_handler(
            "startup", partial(asyncio.create_task, flush_db_periodically(database[db.name], db.flush_period))
        )

    app.add_event_handler("shutdown", close_db)

    return app
