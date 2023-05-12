import json

from starlette.routing import Route, Router
from starlette.responses import Response

from .database import DATABASE

__all__ = ("rest_routes", "router")


# @router.put("/data/{dbname}/{key}")
async def put_data(request):
    resp = {}
    try:
        dbname = request.path_params['dbname']
        key = int(request.path_params['key'])
        value = await request.body()

        if dbname not in DATABASE:
            raise Exception(f"db: {dbname} not found")

        await DATABASE[dbname].put(key, value)
        resp = {
            "ok": True,
        }

    except Exception as e:
        resp = {
            "ok": False,
            "detail": repr(e),
        }

    return Response(json.dumps(resp), media_type="application/json")


# @router.get("/data/{dbname}/{key}")
async def get_data(request):
    resp = {}
    try:
        dbname = request.path_params['dbname']
        key = int(request.path_params['key'])

        if dbname not in DATABASE:
            raise Exception(f"db: {dbname} not found")

        resp = {"ok": True, "data": await DATABASE[dbname].get(key, mode="raw")}

    except Exception as e:
        resp = {
            "ok": False,
            "detail": repr(e),
        }

    return Response(json.dumps(resp), media_type="application/json")


async def get_schema(request):
    resp = {}
    try:
        dbname = request.path_params['dbname']
        if dbname not in DATABASE:
            raise Exception(f"db: {dbname} not found")

        schema_config = DATABASE[dbname].data_schema
        assert schema_config is not None
        resp = {"ok": True, "schema_config_str": schema_config.json()}

    except Exception as e:
        resp = {
            "ok": False,
            "detail": repr(e),
        }

    return Response(json.dumps(resp), media_type="application/json")


rest_routes = [
    Route("/data/{dbname:str}/{key:int}", put_data, methods=["PUT"]),
    Route("/data/{dbname:str}/{key:int}", get_data, methods=["GET"]),
    Route("/schema/{dbname:str}", get_schema, methods=["GET"]),
]

router = Router(rest_routes)
