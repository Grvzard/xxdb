import logging
from pathlib import Path

from xxdb.engine.buffer import BufferPoolManager
from xxdb.engine.disk import DiskManager
from xxdb.engine.metrics import PrometheusClient
from xxdb.engine.hashtable import HashTable
from xxdb.engine.config import InstanceSettings, DbMeta
from xxdb.engine.schema import Schema
from .typing import DataGetModes

__all__ = ("DB", "create", "InstanceSettings", "DbMeta")

logger = logging.getLogger(__name__)


class DB:
    def __init__(
        self,
        name: str,
        datadir: str = ".",
        settings: InstanceSettings = InstanceSettings(),
    ):
        self._config = settings
        self._name = name

        datadir_path = Path(datadir)
        assert datadir_path.exists()

        dat_path = datadir_path / f"{self._name}.dat.xxdb"
        assert dat_path.exists()

        self._meta = DbMeta.parse_raw(DiskManager.read_meta(dat_path.open("rb")))
        self._schema = None
        if self._meta.data_schema and self._config.with_schema:
            self._schema = Schema(self._meta.data_schema)

        self._disk_mgr = DiskManager(datadir, self._name, self._meta.disk)
        self._bp_mgr = BufferPoolManager(self._disk_mgr, self._config.buffer_pool)

        idx_path = datadir_path / f"{self._name}.idx.xxdb"
        self._indices = HashTable(idx_path, self._meta.index)
        self._prom_client = None
        if self._config.prometheus.enable:
            self._prom_client = PrometheusClient(self._bp_mgr)

    async def close(self):
        await self.flush()

    async def get(self, key, mode: DataGetModes = "bytes") -> None | list[bytes] | list[dict] | bytes:
        pageid = self._indices[key]
        if pageid is None:
            return None

        async with self._bp_mgr.fetch_page(pageid) as page:
            if mode == "dict":
                if not self._schema:
                    raise Exception()
                return [self._schema.unpack(_) for _ in page.retrive()]

            elif mode == "bytes":
                return page.retrive()

            elif mode == "raw":
                return page.dumps_data()

        raise Exception()

    async def put(self, key, data: bytes | dict) -> None:
        if isinstance(data, dict):
            if not self._schema:
                raise Exception("db does not have a schema")
            data = self._schema.pack(data)

        pageid = self._indices[key]
        if pageid is not None:
            async with self._bp_mgr.fetch_page(pageid) as page:
                page.put(data)
        else:
            with self._bp_mgr.new_page() as page:
                self._indices[key] = page.id
                page.put(data)

    async def flush(self):
        logger.info("xxdb flushing...")
        await self._bp_mgr.flush_all()
        self._indices.flush()
        logger.info("xxdb flush done")

    @property
    def prom_registry(self):
        if self._prom_client:
            return self._prom_client.registry
        return None


# Return: True if created a new meta file, False if meta file already exists
def create(
    name: str,
    meta: DbMeta = DbMeta(),
    datadir: str = ".",
    exists_ok: bool = True,
) -> bool:
    datadir_path = Path(datadir)
    if not (datadir_path.exists() and datadir_path.is_dir()):
        raise Exception(f"datadir: {datadir} isn't exists or is not a directory")

    dat_path = datadir_path / f"{name}.dat.xxdb"

    if dat_path.exists():
        if not exists_ok:
            raise Exception()
        return False

    DiskManager.write_meta(dat_path.open("wb"), meta.json().encode())
    return True
