# Referenced by: DiskManager, BufferPoolManager
import asyncio
from functools import partial

from xxdb.engine.capped_array import CappedArray
from xxdb.engine.config import PageConfig as PageConfig

__all__ = ("Page", "PageFactory", "PageConfig")


class PageFactory:
    def __init__(self, config: PageConfig):
        self._config = config
        self.Page = partial(Page, config=self._config)

    def __call__(self, page_bytes: bytes, id: int):
        return self.Page(page_bytes, id)


class Page(CappedArray):
    def __init__(self, page_bytes: bytes, id: int, config: PageConfig):
        self._id = id
        self._lock = asyncio.Lock()
        self._pin_cnt = 0
        self.is_dirty = False
        # self._config = config
        self.SIZE_COST = config.size_cost
        self.ID_COST = config.id_cost

        _curr_size = int.from_bytes(page_bytes[-self.SIZE_COST :], "little")
        _cap = len(page_bytes) - self.SIZE_COST
        super().__init__(page_bytes[:_curr_size], _cap)

    @property
    def evictable(self):
        return self._pin_cnt == 0

    def pin(self) -> None:
        self._pin_cnt += 1

    def unpin(self) -> None:
        self._pin_cnt -= 1

    @property
    def id(self):
        return self._id

    # @override
    def append(self, data: bytes):
        super().append(data)
        self.is_dirty = True

    def dumps_page(self) -> bytes:
        _raw_data = super().dumps_data()
        _curr_size = len(_raw_data)
        return _raw_data + b'\x00' * super().free_size + _curr_size.to_bytes(self.SIZE_COST, "little")
