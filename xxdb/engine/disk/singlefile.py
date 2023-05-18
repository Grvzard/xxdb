from concurrent.futures import ThreadPoolExecutor
from functools import partial

from .page import Page
from .blockio import BlockIO
from .disk import Disk

__all__ = ("SingleFile",)


class SingleFile(Disk):
    PAGEID_SIZE = 4

    def __init__(self, name, data_dpath, config):
        self._name = name
        self._config = config
        self._page_size = self._config.page_size

        self.Page = partial(Page)
        self._th_executor = ThreadPoolExecutor()

        self._dat_fpath = data_dpath / f"{name}.dat.xxdb"

        self._bio = BlockIO(self._dat_fpath, self._page_size)

        self._next_pageid = self.dat_file_size // self._page_size
        self.EMPTY_PAGE = b'\x00' * self._page_size

    @property
    def pageid_size(self):
        return self.PAGEID_SIZE

    @property
    def dat_file_size(self):
        return self._dat_fpath.stat().st_size

    async def flush(self):
        self._bio.flush()

    async def close(self):
        self._bio.close()

    async def new_page(self) -> Page:
        pageid = self._next_pageid
        self._next_pageid += 1
        page_data = self.EMPTY_PAGE
        return self.Page(page_data, pageid)

    async def read_page(self, pageid: int) -> Page:
        self._bio.seek(pageid)
        return self.Page(await self._bio.read1(), pageid)

    async def write_page(self, page: Page) -> None:
        pageid = page.id
        assert pageid is not None
        self._bio.seek(pageid)
        await self._bio.write(page.dumps_page())
