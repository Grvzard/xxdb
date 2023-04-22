from abc import ABC, abstractmethod
from typing import Optional


class Replacer(ABC):
    def record_access(self, pageid: int):
        ...

    @abstractmethod
    def evict(self) -> Optional[int]:
        ...

    @abstractmethod
    def pin(self, pageid: int):
        ...

    @abstractmethod
    def unpin(self, pageid: int):
        ...
