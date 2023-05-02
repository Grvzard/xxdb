from functools import partial
from prometheus_client import Counter, CollectorRegistry

from xxdb.engine.buffer.event import BufferPoolEventListener


class PrometheusClient(BufferPoolEventListener):
    def __init__(self, bp_mgr) -> None:
        super().__init__()
        self._reg = CollectorRegistry()

        Counter_ = partial(Counter, registry=self._reg)

        bp_mgr.add_listener(self)
        self._bufferpool_evict_cnt = Counter_("bufferpool_evict", "")

    @property
    def registry(self):
        return self._reg

    async def on_bufferpool_evict(self):
        self._bufferpool_evict_cnt.inc()

    # async def on_
