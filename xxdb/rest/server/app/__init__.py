from fastapi import FastAPI

from xxdb.engine.db import create, DB
from .config import ApiConfig
from .database import database
from . import api

__all__ = ("create_app",)


async def close_db():
    for db in database.values():
        await db.close()


def create_app(config: ApiConfig) -> FastAPI:
    app = FastAPI()

    app.include_router(api.router)

    for db in config.endpoints:
        create(db.name, datadir=db.path)
        database[db.name] = DB(db.name, db.path, db.settings)

    app.add_event_handler("shutdown", close_db)

    return app
