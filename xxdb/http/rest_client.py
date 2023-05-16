from typing import Literal

from tenacity import retry, stop_after_attempt
import httpx

from xxdb.engine.schema import Schema, SchemasConfig


class Client:
    def __init__(self, dsn: str, dbname: str):
        if dsn[-1] == '/':
            dsn = dsn[:-1]
        self._dsn = dsn + f"/rest/{dbname}"
        self.dbname = dbname
        self._http_client = httpx.AsyncClient(base_url=self._dsn, timeout=10)
        self._schema = None

    async def _common_request(self, method, path, **kwargs):
        r = await self._http_client.request(method, path, **kwargs)
        r = r.json()
        if 'ok' not in r:
            raise Exception()
        elif not r['ok']:
            raise Exception(r['detail'])

        return r['data']

    async def connect(self):
        schemas_str = await self._common_request("get", "/schema")
        self._schema = Schema(SchemasConfig.parse_raw(schemas_str))

    async def close(self) -> None:
        await self._http_client.aclose()

    @retry(stop=stop_after_attempt(2), reraise=True)
    async def get(self, key) -> list[dict]:
        assert self._schema is not None

        data = await self._common_request("get", f"/data/{key}")

        return data

    async def put(self, key: int, value: dict, *, schema: str) -> Literal[True]:
        assert self._schema is not None

        ok = await self._common_request(
            "put",
            f"/data/{key}",
            content=self._schema.pack(value, schema=schema),
        )

        return ok
