from typing import Final
from pathlib import Path

__all__ = ("BlockIO",)


class BlockIO:
    META_PAGE_SIZE = 16 * 1024

    __slots__ = ("_fio", "_fpath", "_page_size", "_empty_block")

    def __init__(self, fpath: Path, page_size: int):
        self._page_size = page_size
        fpath.touch(exist_ok=True)
        self._fpath = fpath
        self._fio = fpath.open('r+b', buffering=0)

        self._empty_block: Final[bytes] = b'\x00' * self._page_size

    @property
    def page_len(self) -> int:
        return (self._fpath.stat().st_size - self.META_PAGE_SIZE) // self._page_size

    def _calc_offset(self, pageid):
        return self.META_PAGE_SIZE + pageid * self._page_size

    def flush(self):
        self._fio.flush()

    def close(self):
        self._fio.close()

    def seek(self, pageid: int) -> None:
        offset = self._calc_offset(pageid)
        self._fio.seek(offset)

    def read1(self) -> bytes:
        # page_bytes = await asyncio.to_thread(self._fio.read, self._page_size)
        page_bytes = self._fio.read(self._page_size)
        if len(page_bytes) != self._page_size:
            print(len(page_bytes))
            print(self.page_len)
            assert False
        return page_bytes
        # return self._fio.read(self._page_size)

    def write(self, block_bytes: bytes) -> int:
        if len(block_bytes) != self._page_size:
            print("write failed")
            # print(len(block_bytes))
            # print(block_bytes)
            # print(self._page_size)
            assert False
        # return await asyncio.to_thread(self._fio.write, block_bytes)
        return self._fio.write(block_bytes)
