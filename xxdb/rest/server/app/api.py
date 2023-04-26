# TODO: refactor api using starlette instead of fastapi

from typing import Annotated

from fastapi import APIRouter, Body

from .database import database

__all__ = ("router",)

router = APIRouter()


@router.put("/data/{db_name}/{key}")
async def put_data(db_name: str, key: int, value: Annotated[str, Body(media_type="plain/text")]):
    print(value)
    if db_name not in database:
        return {"ok": False, "detail": f"db: {db_name} not found"}

    try:
        await database[db_name].put(key, bytes(value, "ascii"))
        return {
            "ok": True,
        }

    except Exception as e:
        return {
            "ok": False,
            "detail": repr(e),
        }


@router.get("/data/{db_name}/{key}")
async def get_data(db_name: str, key: int):
    if db_name not in database:
        return {"ok": False, "detail": f"db: {db_name} not found"}

    return {"ok": True, "data": await database[db_name].get(key)}
