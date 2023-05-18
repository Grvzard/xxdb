from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar


class Evictable:
    @property
    @abstractmethod
    def evictable(self) -> bool:
        ...


E = TypeVar('E', bound=Evictable)


class Replacer(ABC, Generic[E]):
    @abstractmethod
    def record_access(self, pageid: int):
        ...

    @abstractmethod
    def evict(self, pool: dict[int, E]) -> Optional[int]:
        ...


class FifoReplacer(Replacer):
    def __init__(self) -> None:
        self.record = {}

    def record_access(self, pageid: int):
        if pageid not in self.record:
            self.record[pageid] = True

    def evict(self, pool: dict[int, E]) -> Optional[int]:
        evicted = None
        for pageid in self.record:
            if pool[pageid].evictable:
                evicted = pageid
                break
        else:
            return None

        self.record.pop(evicted)
        return evicted


class LruReplacer(Replacer):
    def __init__(self) -> None:
        raise NotImplementedError

    def record_access(self, pageid: int):
        ...

    def evict(self, pool: dict[int, E]) -> Optional[int]:
        ...


class LrukReplacer(Replacer):
    def __init__(self, k: int) -> None:
        self.k = k
        raise NotImplementedError

    def record_access(self, pageid: int):
        ...

    def evict(self, pool: dict[int, E]) -> Optional[int]:
        ...


def getReplacer(typ: str) -> Replacer:
    typ = typ.lower()
    if typ == "fifo":
        return FifoReplacer()
    elif typ == "lru":
        return LruReplacer()
    elif typ.startswith("lru-"):
        try:
            k = int(typ[4:])
        except Exception:
            raise Exception("Unexpected buffer pool replacer type")
        else:
            return LrukReplacer(k)
    else:
        return FifoReplacer()
