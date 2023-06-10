# Referenced by: DB
from typing import Iterator, Literal
from pathlib import Path
import struct

from .index import Index


# | key1 | value1 | key2 | value2 | ... | keyN | valueN |
class HashTable(Index):
    def __init__(self, idx_fpath: Path, key_size: Literal[4, 8], value_size: Literal[4, 8]):
        idx_fpath.touch(exist_ok=True)
        self._fp = open(idx_fpath, "r+b", buffering=0)
        self._len = idx_fpath.stat().st_size // (key_size + value_size)
        self._key_size = key_size
        self._value_size = value_size
        self._kv_size = self._key_size + self._value_size
        self._struct = {
            (4, 4): struct.Struct("<II"),
            (8, 4): struct.Struct("<QI"),
            (4, 8): struct.Struct("<IQ"),
            (8, 8): struct.Struct("<QQ"),
        }[
            (self._key_size, self._value_size)
        ]  # type: ignore

        if self._len == 0:
            self._table = {}
        else:
            self._table = dict(self._struct.iter_unpack(self._fp.read(self._len * self._kv_size)))

    def __len__(self) -> int:
        return self._len

    def __iter__(self) -> Iterator[int]:
        return iter(self._table.keys())

    def __contains__(self, key: int) -> bool:
        return key in self._table

    def __getitem__(self, key: int) -> int | None:
        return self._table.get(key, None)

    def __setitem__(self, key: int, value: int) -> None:
        assert key not in self._table

        self._fp.write(key.to_bytes(self._key_size, "little") + value.to_bytes(self._value_size, "little"))

        self._table[key] = value
        self._len += 1

    def flush(self):
        ...

    def close(self):
        self._fp.close()
        del self._table
