import logging
from pathlib import Path

from xxdb.engine.buffer import BufferPoolManager
from xxdb.engine.disk import DiskManager
from xxdb.engine.hashtable import HashTable
from xxdb.engine.config import InstanceSettings, DbMeta
from xxdb.engine.schema import Schema

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
        self._indices = HashTable(self._disk_mgr.read_htkeys())

    async def close(self):
        await self.flush()

    async def get(self, key) -> None | list[bytes] | list[dict]:
        data_list = []
        if key in self._indices:
            pageid = self._indices[key]
            async with self._bp_mgr.fetch_page(pageid) as page:
                data_list = page.retrive()

            if self._schema:
                try:
                    return [self._schema.unpack(_) for _ in data_list]
                except Exception as e:
                    _ = e

            return data_list

        return None

    async def put(self, key, data: bytes | dict) -> None:
        if isinstance(data, dict):
            if not self._schema:
                raise Exception("db does not have a schema")
            data = self._schema.pack(data)

        if key in self._indices:
            pageid = self._indices[key]
            async with self._bp_mgr.fetch_page(pageid) as page:
                page.put(data)
        else:
            async with self._bp_mgr.new_page() as page:
                self._indices[key] = page.id
                page.put(data)

    async def flush(self):
        logger.info("xxdb flushing...")
        self._bp_mgr.flush_all()
        self._disk_mgr.write_htkeys(self._indices.keys_ondisk)
        logger.info("xxdb flush done")


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
