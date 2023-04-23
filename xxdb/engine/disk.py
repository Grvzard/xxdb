# Referenced by: DB
import struct
from pathlib import Path

from xxdb.engine.configs import DiskSettings

__all__ = ("DiskManager",)


# TODO: using async read/write to improve the qps.
# it may require a deep thinking on how to make it coroutine-safe
class DiskManager:
    def __init__(self, datadir_path: Path, db_name: str):
        _meta = (datadir_path / f"{db_name}.meta.xxdb").open("r").read()
        self.config = DiskSettings.parse_raw(_meta)

        self.datadir_path = datadir_path
        self.db_name = db_name

        self.data_path = datadir_path / f"{db_name}.data.xxdb"
        self.indx_path = datadir_path / f"{db_name}.indx.xxdb"

        self.data_path.touch(exist_ok=True)
        self.indx_path.touch(exist_ok=True)

        self.f_data = self.data_path.open('r+b')
        self.f_indx = self.indx_path.open('r+b')

        self.PAGE_SIZE = self.config.page_size
        self.BLANK_PAGE = b'\x00' * self.PAGE_SIZE
        self.next_pageid = self.data_path.stat().st_size // self.PAGE_SIZE

    # def _calc_offset(self, pageid):
    #     return self.META_PAGE_SIZE + pageid * self.PAGE_SIZE

    def _calc_offset(self, pageid):
        return pageid * self.config.page_size

    def close(self):
        self.f_indx.close()
        self.f_data.close()

    def new_page(self) -> tuple[bytes, int]:
        pageid = self.next_pageid
        self.next_pageid += 1
        return self.BLANK_PAGE, pageid

    def read_page(self, pageid) -> bytes:
        offset = self._calc_offset(pageid)
        self.f_data.seek(offset)
        return self.f_data.read(self.PAGE_SIZE)

    def write_page(self, pageid, page_data: bytes):
        self.f_data.seek(self._calc_offset(pageid))
        self.f_data.write(page_data)
        self.f_data.flush()

    # def read_meta(self) -> bytes:
    #     self.f_data.seek(0)
    #     buffer = self.f_data.read(DiskManager.META_PAGE_SIZE)
    #     meta_len = int.from_bytes(buffer[-2:], "little")
    #     return buffer[:meta_len]

    # def write_meta(self, meta_data: bytes):
    #     buffer = bytearray(DiskManager.BLANK_META_PAGE)
    #     buffer[:len(meta_data)] = meta_data
    #     buffer[-2:] = len(meta_data).to_bytes(2, "little")
    #     self.f_data.seek(0)
    #     self.f_data.write(buffer)

    def read_htkeys(self) -> list[tuple[int, int]]:
        # indices_format = '<QL'
        # key_size = struct.calcsize(indices_format)

        self.f_indx.seek(0)
        buffer = self.f_indx.read()
        keys = struct.iter_unpack(self.config.index_format, buffer)
        return list(keys)
        return [_ for _ in keys]

    def write_htkeys(self, keys: list[tuple[int, int]]):
        # indices_format = '<QL'
        # key_size = struct.calcsize(indices_format)

        buffer = bytearray()
        for i in range(len(keys)):
            key, value = keys[i]
            buffer += struct.pack(self.config.index_format, key, value)

        self.f_indx.seek(0)
        self.f_indx.write(buffer)
        self.f_indx.flush()

    # def read_htval(self):
    #     ...

    # def write_htval(self):
    #     ...
