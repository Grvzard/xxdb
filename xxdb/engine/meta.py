from pathlib import Path

from xxdb.engine.config import DbMeta as DbMeta

__all__ = ("MetaManager", "DbMeta")


# | meta_bytes | padding | meta_len(2 bytes) |
class MetaManager:
    META_PAGE_SIZE = 16 * 1024
    META_LEN_COST = 2

    @staticmethod
    def read_meta(fp) -> DbMeta:
        fp.seek(0)
        meta_bytes = fp.read(MetaManager.META_PAGE_SIZE)
        meta_len = int.from_bytes(meta_bytes[-MetaManager.META_LEN_COST :], "little")
        return DbMeta.parse_raw(meta_bytes[:meta_len])

    @staticmethod
    def from_path(path: Path) -> DbMeta:
        with open(path, "rb") as fp:
            return MetaManager.read_meta(fp)

    @staticmethod
    def write_meta(fp, meta_bytes: bytes) -> None:
        meta_len = len(meta_bytes)
        meta_bytes += b'\x00' * (MetaManager.META_PAGE_SIZE - meta_len - MetaManager.META_LEN_COST)
        meta_bytes += meta_len.to_bytes(2, "little")
        fp.seek(0)
        fp.write(meta_bytes)
