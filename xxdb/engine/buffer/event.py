from xxdb.utils.event_emitter import EventEmitter, EventListener


class BufferPoolEventListener(EventListener):
    def on_bufferpool_evict(self):
        ...

    def on_bufferpool_flush_all(self, flush_cnt):
        ...


class BufferPoolEventEmitter(EventEmitter[BufferPoolEventListener]):
    ...
