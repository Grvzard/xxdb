# Referenced by: DB
from typing import Iterator, Literal
from pathlib import Path
import mmap
import struct

from .index import Index


# | len(4 bytes) | key1 | value1 | key2 | value2 | ...
class HashTable(Index):
    def __init__(self, idx_fpath: Path, key_size: Literal[4, 8], value_size: Literal[4, 8]):
        idx_fpath.touch(exist_ok=True)
        self._fp = open(idx_fpath, "r+b")
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

        if (_file_size := idx_fpath.stat().st_size) == 0:
            _init_size = 16 * 1024  # 16KB
            self._fp.write(b'\x00' * _init_size)
            self._fp.seek(0)
            self._len = 0
            self._cap = (_init_size - 4) // self._kv_size
            self._table = {}
        else:
            self._len = int.from_bytes(self._fp.read(4), "little")
            self._cap = (_file_size - 4) // self._kv_size
            self._table = dict(self._struct.iter_unpack(self._fp.read(self._len * self._kv_size)))

        self._mm = mmap.mmap(self._fp.fileno(), 0)
        self._mm.seek(4 + self._len * self._kv_size)

    def __len__(self) -> int:
        return self._len

    def __iter__(self) -> Iterator[int]:
        return iter(self._table.keys())

    def __contains__(self, key) -> bool:
        return key in self._table

    def __getitem__(self, key) -> int | None:
        return self._table.get(key, None)

    def __setitem__(self, key, value) -> None:
        assert key not in self._table
        self._ensure_size()

        self._mm.write(key.to_bytes(self._key_size, "little") + value.to_bytes(4, "little"))

        self._table[key] = value
        self._len += 1
        self._mm[:4] = self._len.to_bytes(4, "little")

    def _ensure_size(self):
        if self._len >= self._cap:
            self._mm.resize(self._mm.size() * 2)
            self._cap = (self._mm.size() - 4) // self._kv_size

    def flush(self):
        self._mm.flush()

    def close(self):
        self.flush()
        self._mm.close()
        # self._fp.close()
