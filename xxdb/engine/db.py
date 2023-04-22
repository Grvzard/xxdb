from typing import Optional
from pathlib import Path

from xxdb.engine.buffer import BufferPoolManager
from xxdb.engine.disk import DiskManager
from xxdb.engine.hashtable import HashTable
from xxdb.engine.configs import Settings, DiskSettings

__all__ = ("DB", "create", "open")


class DB:
    def __init__(self, disk_mgr, bp_mgr):
        self.disk_mgr = disk_mgr
        self.bp_mgr = bp_mgr
        self.index = HashTable(self.disk_mgr.read_htkeys())

    async def close(self):
        self.bp_mgr.flush_all()
        self.disk_mgr.write_htkeys(self.index.keys)

    async def get(self, key) -> Optional[list[bytes]]:
        if key in self.index:
            pageid = self.index[key]
            async with self.bp_mgr.fetch_page(pageid) as page:
                return page.retrive()

    async def put(self, key, data):
        if key in self.index:
            pageid = self.index[key]
            async with self.bp_mgr.fetch_page(pageid) as page:
                page.put(data)
        else:
            async with self.bp_mgr.new_page() as page:
                self.index[key] = page.id
                page.put(data)


def create(
    db_name: str,
    disk_settings: DiskSettings = DiskSettings(),
    datadir: Optional[str] = "data",
) -> None:
    datadir_path = datadir and Path(datadir) or Path('.')
    if not datadir_path.exists():
        raise Exception()

    i = 0
    try:
        for t in "meta data indx".split():
            (datadir_path / f"{db_name}.{t}.xxdb").touch(exist_ok=False)
            i += 1
    except Exception as e:
        for t in "meta data indx".split()[:i]:
            (datadir_path / f"{db_name}.{t}.xxdb").unlink()
        raise e

    with (datadir_path / f"{db_name}.meta.xxdb").open("w") as f_meta:
        f_meta.write(disk_settings.json())


def open(datadir: str, db_name: str, config_file: Optional[str] = '') -> DB:
    if config_file:
        config = Settings.parse_file(config_file)
    else:
        config = Settings()

    disk_mgr = DiskManager(Path(datadir), db_name)
    bp_mgr = BufferPoolManager(disk_mgr, config.buffer_pool)

    db = DB(disk_mgr, bp_mgr)
    return db
