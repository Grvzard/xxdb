# Referenced by: DB
from abc import abstractmethod
from typing import Literal

from .page import Page

__all__ = ("Disk",)


class Disk:
    @property
    @abstractmethod
    def pageid_size(self) -> Literal[4, 8]:
        ...

    @abstractmethod
    def new_page(self) -> Page:
        ...

    @abstractmethod
    def read_page(self, pageid: int) -> Page:
        ...

    @abstractmethod
    def write_page(self, page: Page) -> None:
        ...

    async def init(self):
        ...

    async def flush(self):
        ...

    async def close(self):
        ...
