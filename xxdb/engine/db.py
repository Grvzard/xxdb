import logging
from pathlib import Path
from typing import Literal, Union

from xxdb.engine.buffer import BufferPoolManager
from xxdb.engine.disk import getDisk
from xxdb.engine.meta import MetaManager
from xxdb.engine.metrics import PrometheusClient
from xxdb.engine.config import InstanceSettings, DbMeta
from xxdb.engine.schema import Schema, SchemasConfig
from xxdb.engine.hashtable import HashTable

__all__ = ("DB", "create", "InstanceSettings", "DbMeta")

logger = logging.getLogger(__name__)


class DB:
    def __init__(
        self,
        name: str,
        meta_dpath: Union[str, Path],
        settings: InstanceSettings,
    ):
        self._config = settings
        self._name = name

        if isinstance(meta_dpath, str):
            meta_dpath = Path(meta_dpath)
        meta_fpath = meta_dpath / f"{self._name}.meta.xxdb"
        try:
            self._meta = MetaManager.from_path(meta_fpath)
        except FileNotFoundError:
            raise Exception(f"{meta_fpath!r} not found")
        except Exception as e:
            raise Exception(f"read meta failed: {e}")

        self._schema = None
        if self._meta.schemas and self._config.with_schema:
            self._schema = Schema(self._meta.schemas)

        idx_fpath = meta_dpath / f"{self._name}.idx.xxdb"
        self._disk = getDisk(self._name, meta_dpath, self._meta.disk)
        self._idx = HashTable(idx_fpath, key_size=self._meta.disk.key_size, value_size=self._disk.pageid_size)
        self._buffer = BufferPoolManager(self._disk, self._config.buffer_pool)

        self._prom_client = None
        if self._config.prometheus.enable:
            self._prom_client = PrometheusClient(self._buffer, self._name)

    @property
    def data_schemas(self) -> None | SchemasConfig:
        return self._meta.schemas

    async def close(self):
        await self._buffer.close()
        await self._disk.close()
        self._idx.close()

    async def get(
        self,
        key: int,
        mode: Literal["bytes", "dict", "raw"] = "bytes",
    ) -> None | list[bytes] | list[dict] | bytes:
        pageid = self._idx[key]
        if pageid is None:
            return None
        async with self._buffer.fetch_page(pageid) as page:
            if mode == "dict":
                if not self._schema:
                    raise Exception()
                return [self._schema.unpack(_) for _ in page.retrieve()]

            elif mode == "bytes":
                return page.retrieve()

            elif mode == "raw":
                return page.dumps_data()

        raise Exception()

    async def put(self, key, data: bytes | dict, *, schema: str = '') -> None:
        if isinstance(data, dict):
            if not self._schema:
                raise Exception("db does not have a schema")
            elif schema == '':
                raise Exception("schema(name) is required for multi schema db")
            data = self._schema.pack(data, schema=schema)

        pageid = self._idx[key]
        if pageid is None:
            pageid = self._buffer.new_page()
            self._idx[key] = pageid

        async with self._buffer.fetch_page(pageid) as page:
            page.append(data)

    async def flush(self):
        logger.info("xxdb flushing...")
        await self._buffer.flush_all()
        self._idx.flush()
        logger.info("xxdb flush done")

    @property
    def prom_registry(self):
        if self._prom_client:
            return self._prom_client.registry
        return None


def create(
    name: str,
    cfg_fpath: Path,
    exists_ok: bool = True,
) -> Path:
    if not cfg_fpath.exists():
        raise Exception(f"config file not found: {cfg_fpath!r}")
    meta = DbMeta.parse_file(cfg_fpath)

    meta_dpath = cfg_fpath.parent / name
    meta_fpath = meta_dpath / f"{name}.meta.xxdb"

    if meta_dpath.exists():
        if not meta_dpath.is_dir():
            raise Exception(f"datadir: {meta_dpath!r} is not a directory")
    else:
        meta_dpath.mkdir(777)
        logger.info(f"created datadir: {meta_dpath!r}")

    if meta_fpath.exists():
        if not exists_ok:
            raise Exception("db meta file already exists")

    MetaManager.write_meta(meta_fpath.open("wb"), meta.json().encode("utf-8"))
    return meta_dpath
