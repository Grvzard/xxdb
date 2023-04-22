from contextlib import asynccontextmanager

from xxdb.engine.page import Page
from xxdb.engine.configs import BufferPoolSettings
from .replacer import getReplacer


class BufferPoolManager:
    def __init__(self, disk_mgr, config: BufferPoolSettings):
        self.disk_mgr = disk_mgr
        self.pool = {}
        self.config = config
        self.replacer = getReplacer(config.replacer)

    async def on_stop(self):
        ...

    @asynccontextmanager
    async def new_page(self):
        page_data, pageid = self.disk_mgr.new_page()
        page = Page(page_data, pageid)
        self.pool[pageid] = page

        self.replacer.record_access(pageid)
        page.pin_cnt += 1
        try:
            yield page
        finally:
            page.is_dirty = True
            page.pin_cnt -= 1

    @asynccontextmanager
    async def fetch_page(self, pageid):
        if pageid not in self.pool:
            if len(self.pool) >= self.config.max_page_num and not self.try_evict():
                raise Exception()

            page_data = self.disk_mgr.read_page(pageid)
            self.pool[pageid] = Page(page_data, pageid)

        page = self.pool[pageid]

        self.replacer.record_access(pageid)
        page.pin_cnt += 1
        try:
            yield page
        finally:
            page.pin_cnt -= 1

    def flush_all(self) -> None:
        for pageid, page in self.pool.items():
            self.flush_page(page)

    def flush_page(self, page: Page) -> None:
        if page.is_dirty:
            self.disk_mgr.write_page(page.id, page.array.dumps())
            page.is_dirty = False

    def try_evict(self) -> bool:
        if pageid_evicted := self.replacer.evict(self.pool):
            page = self.pool.pop(pageid_evicted)
            self.flush_page(page)
            return True

        return False
