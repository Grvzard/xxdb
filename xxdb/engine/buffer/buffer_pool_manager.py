from concurrent.futures import ThreadPoolExecutor
import asyncio
import logging
from contextlib import asynccontextmanager
from weakref import WeakValueDictionary

from xxdb.engine.config import BufferPoolConfig as BufferPoolConfig
from xxdb.engine.disk import Page, Disk
from xxdb.utils.event import EventEmitter
from .replacer import getReplacer

__all__ = ("BufferPoolManager", "BufferPoolConfig")

logger = logging.getLogger(__name__)


class BufferPoolManager(EventEmitter):
    def __init__(self, disk: Disk, config: BufferPoolConfig):
        super().__init__()
        self.disk = disk
        self.config = config

        self.pool: dict[int, Page] = {}
        self.replacer = getReplacer(self.config.replacer)
        self.dirty_pageids: set[int] = set()
        self._max_page_amount = self.config.max_pages
        self._thread_pool = ThreadPoolExecutor()
        self._flushing_pages: dict[int, asyncio.Event] = WeakValueDictionary()  # type: ignore
        self._fetching_pages: dict[int, asyncio.Event] = WeakValueDictionary()  # type: ignore

    def new_page(self) -> int:  # pageid
        page = self.disk.new_page()
        page.is_dirty = True
        self.dirty_pageids.add(page.id)
        self.pool[page.id] = page
        return page.id

    @asynccontextmanager
    async def fetch_page(self, pageid):
        if len(self.pool) >= self._max_page_amount and not await self.try_evict():
            raise Exception()

        page = self.pool.get(pageid, None)

        if page is None:
            if pageid in self._flushing_pages:
                flush_event = self._flushing_pages[pageid]
                await flush_event.wait()

            if pageid in self._fetching_pages:
                fetch_event = self._fetching_pages[pageid]
                await fetch_event.wait()
                page = self.pool[pageid]
            else:
                fetch_event = asyncio.Event()
                self._fetching_pages[pageid] = fetch_event
                page = await self.disk.read_page(pageid)
                fetch_event.set()
                self.pool[pageid] = page

            # page = await asyncio.to_thread(self.disk.read_page, pageid)

            # loop = asyncio.get_running_loop()
            # page = await loop.run_in_executor(self._thread_pool, self.disk.read_page, pageid)

        self.replacer.record_access(pageid)
        page.pin()
        try:
            yield page
        finally:
            if page.is_dirty:
                self.dirty_pageids.add(page.id)
            page.unpin()

    async def flush_all(self) -> None:
        while 1:
            if not self.dirty_pageids:
                break
            pageid = self.dirty_pageids.pop()
            # with page_lock(pageid):
            await self._flush_page(self.pool[pageid])

        await self.disk.flush()

    async def _flush_page(self, page: Page) -> None:
        if page.is_dirty:
            # logger.debug(f"flush dirty page: {page.id}")
            page.is_dirty = False
            self.dirty_pageids.discard(page.id)
            # await asyncio.to_thread(self.disk.write_page, page)
            await self.disk.write_page(page)
            await self._emit("flush")

    async def try_evict(self) -> bool:
        pageid_evicted = self.replacer.evict(self.pool, self._fetching_pages)  # type: ignore
        if pageid_evicted is not None:
            page = self.pool.pop(pageid_evicted)
            flush_event = asyncio.Event()
            self._flushing_pages[pageid_evicted] = flush_event
            await self._flush_page(page)
            flush_event.set()
            # logger.debug(f"evict page: {pageid_evicted}")
            await self._emit("evict")
            return True

        return False

    async def close(self) -> None:
        await self.flush_all()
        self._thread_pool.shutdown()
