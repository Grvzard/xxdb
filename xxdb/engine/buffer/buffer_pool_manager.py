import logging
from contextlib import asynccontextmanager, contextmanager

from xxdb.engine.page import Page
from xxdb.engine.config import BufferPoolSettings
from xxdb.engine.disk import DiskManager
from .replacer import getReplacer
from .event import BufferPoolEventEmitter


logger = logging.getLogger(__name__)


class BufferPoolManager(BufferPoolEventEmitter):
    def __init__(self, disk_mgr: DiskManager, config: BufferPoolSettings):
        super().__init__()
        self.disk_mgr = disk_mgr
        self.config = config

        self.pool: dict[int, Page] = {}
        self.replacer = getReplacer(self.config.replacer)
        self.dirty_pageids: set[int] = set()
        self._max_page_amount = self.config.max_size // self.disk_mgr.config.page_size

    @contextmanager
    def new_page(self):
        page_data, pageid = self.disk_mgr.new_page()
        page = Page(page_data, pageid)
        self.pool[pageid] = page

        self.replacer.record_access(pageid)
        page.pin()
        try:
            yield page
        finally:
            page.is_dirty = True
            self.dirty_pageids.add(page.id)
            page.unpin()

    @asynccontextmanager
    async def fetch_page(self, pageid):
        if pageid not in self.pool:
            if len(self.pool) >= self._max_page_amount and not await self.try_evict():
                raise Exception()

            page_data = self.disk_mgr.read_page(pageid)
            self.pool[pageid] = Page(page_data, pageid)

        page = self.pool[pageid]

        self.replacer.record_access(pageid)
        page.pin()
        try:
            yield page
        finally:
            if page.is_dirty:
                self.dirty_pageids.add(page.id)
            page.unpin()

    async def flush_all(self) -> None:
        logger.info("buffer pool flush all pages...")
        flush_cnt = 0
        while 1:
            if not self.dirty_pageids:
                break
            pageid = self.dirty_pageids.pop()
            # with page_lock(pageid):
            self.flush_page(self.pool[pageid])
            flush_cnt += 1

        await self._emit("bufferpool_flush_all", flush_cnt)

    def flush_page(self, page: Page) -> None:
        if page.is_dirty:
            logger.debug(f"flush dirty page: {page.id}")
            self.disk_mgr.write_page(page.id, page.dumps())
            page.is_dirty = False
            self.dirty_pageids.discard(page.id)

    async def try_evict(self) -> bool:
        if pageid_evicted := self.replacer.evict(self.pool):
            page = self.pool.pop(pageid_evicted)
            self.flush_page(page)
            await self._emit("bufferpool_evict")
            return True

        return False
