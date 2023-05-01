# Referenced by: BufferPoolManager

import asyncio

from xxdb.engine.capped_array import CappedArray


class Page:
    def __init__(self, data: bytes, id: int):
        self.array = CappedArray(data)
        self.id = id
        self.lock = asyncio.Lock()
        self.is_dirty = False
        self.pin_cnt = 0

    @property
    def evictable(self):
        return self.pin_cnt == 0

    def put(self, data: bytes):
        self.array.append(data)
        self.is_dirty = True

    def retrive(self) -> list[bytes]:
        return self.array.retrive()
