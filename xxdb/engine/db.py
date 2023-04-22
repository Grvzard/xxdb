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


# Return: True if created a new meta file, False if meta file already exists
def create(
    db_name: str,
    disk_settings: DiskSettings = DiskSettings(),
    datadir: Optional[str] = "data",
    exists_ok: bool = True,
) -> bool:
    datadir_path = datadir and Path(datadir) or Path('.')
    if not datadir_path.exists():
        raise Exception()

    meta_path = datadir_path / f"{db_name}.meta.xxdb"

    if meta_path.exists():
        if not exists_ok:
            raise Exception()
        return False

    with meta_path.open("w") as f_meta:
        f_meta.write(disk_settings.json())
    return True


def open(db_name: str, datadir: str = "data", config_file: Optional[str] = '') -> DB:
    if config_file:
        config = Settings.parse_file(config_file)
    else:
        config = Settings()

    disk_mgr = DiskManager(Path(datadir), db_name)
    bp_mgr = BufferPoolManager(disk_mgr, config.buffer_pool)

    db = DB(disk_mgr, bp_mgr)
    return db
