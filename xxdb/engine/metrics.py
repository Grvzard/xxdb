from functools import partial
from prometheus_client import Counter, CollectorRegistry, Gauge


class PrometheusClient:
    def __init__(self, bp_mgr, dbname: str = '') -> None:
        self._reg = CollectorRegistry()

        Counter_ = partial(Counter, registry=self._reg, labelnames=["dbname"])
        Gauge_ = partial(Gauge, registry=self._reg, labelnames=["dbname"])

        self._bufferpool_evict_cnt = Counter_("bufferpool_evict", "")

        @bp_mgr.on("evict")
        def on_bufferpool_evict():
            self._bufferpool_evict_cnt.labels(dbname).inc()

        self._bufferpool_size = Gauge_("bufferpool_size", "")
        self._bufferpool_size.labels(dbname).set_function(lambda: len(bp_mgr.pool))

        self._bufferpool_dirtys_cnt = Gauge_("bufferpool_dirtys_cnt", "")
        self._bufferpool_dirtys_cnt.labels(dbname).set_function(lambda: len(bp_mgr.dirty_pageids))

        self._bufferpool_flush_cnt = Counter_("bufferpool_flush_cnt", "")

        @bp_mgr.on("flush")
        def on_bufferpool_flush():
            self._bufferpool_flush_cnt.labels(dbname).inc()

    @property
    def registry(self):
        return self._reg
