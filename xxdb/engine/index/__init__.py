from pathlib import Path

from xxdb.engine.config import IndexConfig as IndexConfig
from .index import Index as Index

__all__ = ("getIndex", "Index", "IndexConfig")


def getIndex(db_name, idx_dpath, config) -> Index:
    idx_dpath = Path(idx_dpath)
    if config.typ == "hashtable":
        from .hashtable import HashTable

        return HashTable(idx_dpath / f"{db_name}.ht.idx.xxdb", config.key_size, config.value_size)
    elif config.typ == "sqlite":
        from .sqlite import SQLite

        return SQLite(idx_dpath / f"{db_name}.sqlite.idx.xxdb", config.key_size, config.value_size)
    else:
        raise Exception(f"unknown index type: {config.typ!r}")
