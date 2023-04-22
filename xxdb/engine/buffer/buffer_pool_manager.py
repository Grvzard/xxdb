from contextlib import asynccontextmanager

from xxdb.engine.page import Page
from xxdb.engine.configs import BufferPoolSettings


class BufferPoolManager:
    def __init__(self, disk_mgr, config: BufferPoolSettings):
        self.disk_mgr = disk_mgr
        self.pool = {}
        self.config = config

    async def on_stop(self):
        ...

    @asynccontextmanager
    async def new_page(self):
        page_data, pageid = self.disk_mgr.new_page()
        page = Page(page_data, pageid)
        self.pool[pageid] = page

        # await page.lock.acquire()
        page.pin_cnt += 1
        try:
            yield page
        finally:
            page.is_dirty = True
            page.pin_cnt -= 1
            # page.lock.release()

    @asynccontextmanager
    async def fetch_page(self, pageid):
        if pageid not in self.pool:
            if len(self.pool) >= self.config.max_page_num:
                if not self.evict_page():
                    raise Exception()

            page_data = self.disk_mgr.read_page(pageid)
            self.pool[pageid] = Page(page_data, pageid)

        # TODO: record access
        page = self.pool[pageid]
        # await page.lock.acquire()
        page.pin_cnt += 1
        try:
            yield page
        finally:
            page.pin_cnt -= 1
            # page.lock.release()

    def flush_all(self):
        for pageid, page in self.pool.items():
            self.flush_page(page)

    def flush_page(self, page: Page):
        if page.is_dirty:
            self.disk_mgr.write_page(page.id, page.array.dumps())

    # TODO: using replacer instead
    async def evict_page(self) -> bool:
        pageid_evicted = None
        for pageid, page in self.pool.items():
            if page.pin_cnt == 0:
                pageid_evicted = pageid
                break
        else:
            return False

        page = self.pool.pop(pageid_evicted)
        self.flush_page(page)
        return True
