from xxdb.utils.event_emitter import EventEmitter, EventListener


class BufferPoolEventListener(EventListener):
    async def on_bufferpool_evict(self):
        ...

    async def on_bufferpool_flush_all(self, flush_cnt):
        ...


class BufferPoolEventEmitter(EventEmitter[BufferPoolEventListener]):
    ...
