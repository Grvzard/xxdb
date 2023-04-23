from contextlib import asynccontextmanager

from xxdb.engine.page import Page
from xxdb.engine.configs import BufferPoolSettings
from xxdb.engine.disk import DiskManager
from .replacer import getReplacer


class BufferPoolManager:
    def __init__(self, disk_mgr: DiskManager, config: BufferPoolSettings):
        self.disk_mgr = disk_mgr
        self.pool: dict[int, Page] = {}
        self.config = config
        self.replacer = getReplacer(config.replacer)
        self.dirty_pageids: set[int] = set()

    # async def on_stop(self):
    #     ...

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
            self.dirty_pageids.add(page.id)
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
            if page.is_dirty:
                self.dirty_pageids.add(page.id)
            page.pin_cnt -= 1

    def flush_all(self) -> None:
        while 1:
            if not self.dirty_pageids:
                break
            pageid = self.dirty_pageids.pop()
            # with page_lock(pageid):
            self.flush_page(self.pool[pageid])

    def flush_page(self, page: Page) -> None:
        if page.is_dirty:
            self.disk_mgr.write_page(page.id, page.array.dumps())
            page.is_dirty = False
            self.dirty_pageids.discard(page.id)

    def try_evict(self) -> bool:
        if pageid_evicted := self.replacer.evict(self.pool):
            page = self.pool.pop(pageid_evicted)
            self.flush_page(page)
            return True

        return False
