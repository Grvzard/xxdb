import asyncio
from typing import Final
from pathlib import Path

__all__ = ("BlockIO",)


class BlockIO:
    META_PAGE_SIZE = 16 * 1024

    __slots__ = ("_fio", "_page_size", "_empty_block")

    def __init__(self, fpath: Path, page_size: int):
        self._page_size = page_size
        fpath.touch(exist_ok=True)
        self._fio = fpath.open('r+b', buffering=0)

        self._empty_block: Final[bytes] = b'\x00' * self._page_size

    def _calc_offset(self, pageid):
        return self.META_PAGE_SIZE + pageid * self._page_size

    def flush(self):
        self._fio.flush()

    def close(self):
        self._fio.close()

    # def new(self) -> bytes:
    #     return self._empty_block

    def seek(self, pageid: int) -> None:
        offset = self._calc_offset(pageid)
        self._fio.seek(offset)

    async def read1(self) -> bytes:
        return await asyncio.to_thread(self._fio.read, self._page_size)

    async def write(self, block_bytes: bytes) -> int:
        assert len(block_bytes) == self._page_size
        return await asyncio.to_thread(self._fio.write, block_bytes)
