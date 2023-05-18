from xxdb.engine.capped_array import CappedArray
from xxdb.engine.buffer.replacer import Evictable

__all__ = ("Page",)


class Page(CappedArray, Evictable):
    LSN_COST = 8
    SIZE_COST = 4

    __slots__ = ("_id", "_pin_cnt", "is_dirty", "lsn")

    def __init__(self, page_bytes: bytes, id: int):
        self._id = id
        # self._lock = asyncio.Lock()
        self._pin_cnt = 0
        self.is_dirty = False

        page_bytes, self.lsn, _curr_size = self._process_meta(page_bytes)
        _cap = len(page_bytes)
        super().__init__(page_bytes[:_curr_size], _cap)

    def _process_meta(self, page_bytes: bytes):
        result = []
        for n in (self.LSN_COST, self.SIZE_COST):
            page_bytes, meta = page_bytes[:-n], page_bytes[-n:]
            result.append(int.from_bytes(meta, "little"))
        return page_bytes, *result

    @property
    # @override
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
        page_meta = self.lsn.to_bytes(self.LSN_COST, "little") + _curr_size.to_bytes(self.SIZE_COST, "little")

        return _raw_data + b'\x00' * super().free_size + page_meta
