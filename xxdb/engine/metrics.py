from functools import partial
from prometheus_client import Counter, CollectorRegistry, Gauge

from xxdb.engine.buffer.event import BufferPoolEventListener


class PrometheusClient(BufferPoolEventListener):
    def __init__(self, bp_mgr, labelname: str = '') -> None:
        super().__init__()
        self._reg = CollectorRegistry()
        _ = labelname

        Counter_ = partial(Counter, registry=self._reg)
        Gauge_ = partial(Gauge, registry=self._reg)

        bp_mgr.add_listener(self)
        self._bufferpool_evict_cnt = Counter_("bufferpool_evict", "")
        self._bufferpool_size = Gauge_("bufferpool_size", "")
        self._bufferpool_size.set_function(lambda: len(bp_mgr.pool))
        self._bufferpool_dirtys_cnt = Gauge_("bufferpool_dirtys_cnt", "")
        self._bufferpool_dirtys_cnt.set_function(lambda: len(bp_mgr.dirty_pageids))
        self._bufferpool_flush_cnt = Counter_("bufferpool_flush_cnt", "")

    @property
    def registry(self):
        return self._reg

    # @override
    async def on_bufferpool_evict(self):
        self._bufferpool_evict_cnt.inc()

    # @override
    async def on_bufferpool_flush_all(self, flush_cnt):
        self._bufferpool_flush_cnt.inc(flush_cnt)
