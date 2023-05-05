# Referenced by: DB
from typing import Iterator
from pathlib import Path
import mmap
import struct

from xxdb.engine.config import IndexSettings


class HashTable:
    def __init__(self, idx_path: Path, settings: IndexSettings):
        idx_path.touch(exist_ok=True)
        self._fp = open(idx_path, "r+b")
        self._key_size = settings.key_size
        self._value_size = 4
        self._kv_size = self._key_size + self._value_size
        self._struct_format = {
            4: "<IL",
            8: "<QL",
        }[self._key_size]

        if (_file_size := idx_path.stat().st_size) == 0:
            _init_size = 16 * 1024  # 16KB
            self._fp.write(b'\x00' * _init_size)
            self._fp.seek(0)
            self._len = 0
            self._cap = (_init_size - 4) // self._kv_size
            self._table = {}
        else:
            self._len = int.from_bytes(self._fp.read(4), "little")
            self._cap = (_file_size - 4) // self._kv_size
            self._table = dict(struct.iter_unpack(self._struct_format, self._fp.read(self._len * self._kv_size)))

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
        self._fp.close()
