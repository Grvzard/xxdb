from typing import Union
from pathlib import Path
import sqlite3 as sqlite

from .index import Index


class SQLite(Index):
    def __init__(self, idx_fpath: Union[str, Path], key_size, value_size) -> None:
        super().__init__()
        self._conn = sqlite.connect(Path(idx_fpath), isolation_level=None)
        # self._cur = self._conn.cursor()
        self._conn.execute("create table if not exists kv (key integer primary key, value integer);")
        self._conn.execute("create UNIQUE index if not exists kv_key on kv (key);")
        # self._conn.execute("PRAGMA journal_mode = MEMORY;")
        # self._conn.execute("PRAGMA synchronous = 1;")

    def __getitem__(self, key) -> int | None:
        cur = self._conn.execute("select value from kv where key = ?;", (key,))
        ret = cur.fetchone()
        if ret is None:
            return None
        else:
            return ret[0]

    def __setitem__(self, key, value) -> None:
        self._conn.execute("insert into kv values (?, ?);", (key, value))

    def __contains__(self, key) -> bool:
        return self[key] is not None

    def flush(self):
        ...

    def close(self):
        self._conn.close()
