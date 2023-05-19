# Referenced by: Page, http.ws_client, http.rest_client

from typing import List


class CappedArray:
    # TODO: use varint instead of fixed size
    DATA_SIZE_COST = 2

    def __init__(self, array_data: bytes, capacity: int):
        self._data = bytearray(array_data)
        self._cap = capacity
        self._curr_size = len(array_data)

    @property
    def free_size(self) -> int:
        return self._cap - self._curr_size

    def append(self, data):
        data_size = len(data)
        self._ensure_space(self.DATA_SIZE_COST + data_size)
        self._append(data_size.to_bytes(self.DATA_SIZE_COST, "little"))
        self._append(data)

    def gonna_full(self, size) -> bool:
        return size > self.free_size

    def _ensure_space(self, size) -> None:
        while self.gonna_full(size):
            data_size = int.from_bytes(self._data[0 : self.DATA_SIZE_COST], "little")
            self._data = self._data[data_size + self.DATA_SIZE_COST :]
            self._curr_size -= data_size + self.DATA_SIZE_COST

    def _append(self, data):
        self._data[self._curr_size :] = data
        self._curr_size += len(data)

    def dumps_data(self) -> bytes:
        return bytes(self._data)

    def retrieve(self) -> List[bytes]:
        return self.RetrieveFromRaw(self._data)

    @staticmethod
    def RetrieveFromRaw(raw_data: bytes) -> List[bytes]:
        _curr_size = len(raw_data)
        p = 0
        data_list = []
        while p < _curr_size:
            data_size = int.from_bytes(raw_data[p : p + CappedArray.DATA_SIZE_COST], "little")
            p += CappedArray.DATA_SIZE_COST
            data_list.append(bytes(raw_data[p : p + data_size]))
            p += data_size

        return data_list
