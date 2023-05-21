from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar, Container, Mapping


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
        ...

    def record_access(self, pageid: int):
        ...

    def evict(self, pool: Mapping[int, E], wait_list: Container) -> Optional[int]:
        for pageid, page in pool.items():
            if page.evictable and pageid not in wait_list:
                return pageid
        return None


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
