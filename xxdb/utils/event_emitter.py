from abc import ABC
import asyncio
import functools
from typing import Any, Generic, TypeVar

__all__ = 'EventListener', 'EventEmitter'


class EventListener(ABC):
    ...


_T = TypeVar('_T', bound=EventListener)


class EventEmitter(Generic[_T]):
    def __init__(self) -> None:
        super().__init__()
        self._listeners: set[_T] = set()

    def add_listener(self, listener: _T) -> None:
        self._listeners.add(listener)

    def remove_listener(self, listener: _T) -> None:
        self._listeners.discard(listener)

    async def _emit(self, event_name: str, *args: Any, **kwargs: Any) -> None:
        for listener in self._listeners:
            handle_func = getattr(listener, 'on_' + event_name)
            if is_async_callable(handle_func):
                await handle_func(*args, **kwargs)
            else:
                handle_func(*args, **kwargs)


# copy paste from https://github.com/encode/starlette/blob/master/starlette/_utils.py
def is_async_callable(obj: Any) -> bool:
    while isinstance(obj, functools.partial):
        obj = obj.func

    return asyncio.iscoroutinefunction(obj) or (callable(obj) and asyncio.iscoroutinefunction(obj.__call__))
