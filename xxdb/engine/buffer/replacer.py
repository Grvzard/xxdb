from abc import ABC, abstractmethod
from typing import Optional

from ..page import Page


class Replacer(ABC):
    @abstractmethod
    def record_access(self, pageid: int):
        ...

    @abstractmethod
    def evict(self, pool: dict[int, Page]) -> Optional[int]:
        ...


class FifoReplacer(Replacer):
    def __init__(self) -> None:
        self.record = {}

    def record_access(self, pageid: int):
        if pageid not in self.record:
            self.record[pageid] = True

    def evict(self, pool: dict[int, Page]) -> Optional[int]:
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

    def evict(self, pool: dict[int, Page]) -> Optional[int]:
        ...


class LrukReplacer(Replacer):
    def __init__(self, k: int) -> None:
        self.k = k
        raise NotImplementedError

    def record_access(self, pageid: int):
        ...

    def evict(self, pool: dict[int, Page]) -> Optional[int]:
        ...


def getReplacer(config: str) -> Replacer:
    if config == "fifo":
        return FifoReplacer()
    elif config == "lru":
        return LruReplacer()
    elif config.startswith("lru-"):
        try:
            return LrukReplacer(int(config[4:]))
        except Exception as e:
            _ = e
            raise Exception("Unexpected config")
    else:
        return FifoReplacer()
