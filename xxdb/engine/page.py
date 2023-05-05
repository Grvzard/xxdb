# Referenced by: BufferPoolManager
import asyncio

from xxdb.engine.capped_array import CappedArray


class Page(CappedArray):
    def __init__(self, data: bytes, id: int):
        super().__init__(data)
        self._id = id
        self._lock = asyncio.Lock()
        self._pin_cnt = 0
        self.is_dirty = False

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

    def put(self, data: bytes):
        super().append(data)
        self.is_dirty = True
