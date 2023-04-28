# Referenced by: DB
from typing import Iterator


class HashTable:
    def __init__(self, keys: list[tuple[int, int]]):
        self.keys_ondisk = keys
        self.ht = dict(keys)
        self.len = len(keys)
        # self.indices = _resize_indices()

    # def _resize_indices(self):
    #     ...

    def __len__(self) -> int:
        return self.len

    def __iter__(self) -> Iterator[int]:
        return iter(self.ht.keys())

    def __contains__(self, key):
        return key in self.ht

    def __getitem__(self, key):
        return self.ht[key]

    def __setitem__(self, key, value):
        assert key not in self.ht

        self.keys_ondisk.append((key, value))
        self.ht[key] = value
        self.len += 1
