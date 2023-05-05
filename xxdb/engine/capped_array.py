# Referenced by: Page

from typing import List


class CappedArray:
    DATA_SIZE_COST = 1

    def __init__(self, page_data: bytes):
        self.max_len = len(page_data) - 4
        self.curr_len = int.from_bytes(page_data[-4:], "little")
        self._data = bytearray(page_data[: self.curr_len])

    @property
    def free_len(self) -> int:
        return self.max_len - self.curr_len

    def append(self, data):
        data_size = len(data)
        self._ensure_space(self.DATA_SIZE_COST + data_size)
        self._append(data_size.to_bytes(self.DATA_SIZE_COST, "little"))
        self._append(data)

    def _ensure_space(self, size) -> None:
        while size > self.free_len:
            data_size = int.from_bytes(self._data[0 : self.DATA_SIZE_COST], "little")
            self._data = self._data[data_size + self.DATA_SIZE_COST :]
            self.curr_len -= data_size + self.DATA_SIZE_COST

    def _append(self, data):
        self._data[self.curr_len :] = data
        self.curr_len += len(data)

    def dumps(self) -> bytes:
        return self._data + b'\x00' * self.free_len + self.curr_len.to_bytes(4, "little")

    def dumps_data(self) -> bytes:
        return self._data[: self.curr_len]

    def retrive(self) -> List[bytes]:
        p = 0
        data_set = []
        while p < self.curr_len:
            data_size = int.from_bytes(self._data[p : p + self.DATA_SIZE_COST], "little")
            p += self.DATA_SIZE_COST
            data = self._data[p : p + data_size]
            data_set.append(bytes(data))
            p += data_size

        return data_set
