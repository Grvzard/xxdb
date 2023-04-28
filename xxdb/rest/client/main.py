from tenacity import retry, stop_after_attempt
import httpx


class Client:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._http_client = httpx.AsyncClient(base_url=dsn, timeout=10)

    @retry(stop=stop_after_attempt(2), reraise=True)
    async def get(self, db, key) -> dict:
        r = await self._http_client.get(f"/data/{db}/{key}")
        r = r.json()
        if 'ok' not in r:
            raise Exception()
        elif not r['ok']:
            raise Exception(r['detail'])

        return r['data']

    async def put(self, db, key, value) -> bool:
        r = await self._http_client.put(f"/data/{db}/{key}", json={"value": value})
        r = r.json()
        if 'ok' not in r:
            raise Exception()

        return r['ok']

    async def close(self) -> None:
        await self._http_client.aclose()
