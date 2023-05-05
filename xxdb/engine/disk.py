# Referenced by: DB
from pathlib import Path

from xxdb.engine.config import DiskSettings

__all__ = ("DiskManager",)


# TODO: using async read/write to improve the qps.
# it may require a deep thinking on how to make it coroutine-safe
class DiskManager:
    META_PAGE_SIZE = 16 * 1024

    def __init__(self, datadir: str | Path, db_name: str, settings: DiskSettings):
        datadir_path = Path(datadir)
        self.db_name = db_name
        self.config = settings

        # self._index_format = {
        #     4: "<IL",
        #     8: "<QL",
        # }[self.config.index_key_size]

        self._dat_path = datadir_path / f"{db_name}.dat.xxdb"
        # self._idx_path = datadir_path / f"{db_name}.idx.xxdb"

        self._dat_path.touch(exist_ok=True)
        # self._idx_path.touch(exist_ok=True)

        self._f_dat = self._dat_path.open('r+b')
        # self._f_idx = self._idx_path.open('r+b')

        self._next_pageid = (self.dat_file_size - self.META_PAGE_SIZE) // self.config.page_size

    @property
    def dat_file_size(self):
        return self._dat_path.stat().st_size

    def _calc_offset(self, pageid):
        return self.META_PAGE_SIZE + pageid * self.config.page_size

    # def _calc_offset(self, pageid):
    #     return pageid * self.config.page_size

    def close(self):
        # self._f_idx.close()
        self._f_dat.close()

    def new_page(self) -> tuple[bytes, int]:
        pageid = self._next_pageid
        self._next_pageid += 1
        return b'\x00' * self.config.page_size, pageid

    def read_page(self, pageid: int) -> bytes:
        offset = self._calc_offset(pageid)
        self._f_dat.seek(offset)
        return self._f_dat.read(self.config.page_size)

    def write_page(self, pageid: int, page_data: bytes) -> None:
        self._f_dat.seek(self._calc_offset(pageid))
        self._f_dat.write(page_data)
        self._f_dat.flush()

    @staticmethod
    def read_meta(fp) -> bytes:
        fp.seek(0)
        meta_bytes = fp.read(DiskManager.META_PAGE_SIZE)
        meta_len = int.from_bytes(meta_bytes[-2:], "little")
        return meta_bytes[:meta_len]

    @staticmethod
    def write_meta(fp, meta_bytes: bytes) -> None:
        meta_len = len(meta_bytes)
        meta_bytes += b'\x00' * (DiskManager.META_PAGE_SIZE - meta_len - 2)
        meta_bytes += meta_len.to_bytes(2, "little")
        fp.seek(0)
        fp.write(meta_bytes)

    # def read_htkeys(self) -> list[tuple[int, int]]:
    #     self._f_idx.seek(0)
    #     buffer = self._f_idx.read()
    #     keys = struct.iter_unpack(self._index_format, buffer)
    #     return list(keys)

    # def write_htkeys(self, keys: list[tuple[int, int]]) -> None:
    #     buffer = bytearray()
    #     for i in range(len(keys)):
    #         key, value = keys[i]
    #         buffer += struct.pack(self._index_format, key, value)

    #     self._f_idx.seek(0)
    #     self._f_idx.write(buffer)
    #     self._f_idx.flush()

    # def read_htval(self):
    #     ...

    # def write_htval(self):
    #     ...
