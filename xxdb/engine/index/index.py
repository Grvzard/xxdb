from abc import abstractmethod, ABC

from xxdb.engine.config import IndexConfig as IndexConfig

__all__ = ("Index", "IndexConfig")


class Index(ABC):
    __slots__ = ()

    @abstractmethod
    def __getitem__(self, key) -> int | None:
        ...

    @abstractmethod
    def __setitem__(self, key, value) -> None:
        ...

    def flush(self):
        ...

    def close(self):
        self.flush()
        ...
