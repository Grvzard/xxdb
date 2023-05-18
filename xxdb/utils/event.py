import asyncio
from functools import partial
from typing import Any, Union, Coroutine
from collections import defaultdict
from collections.abc import Callable

__all__ = ('EventEmitter',)


class EventHandler:
    __slots__ = ("func", "is_async")

    def __init__(self, func: Union[Callable, Coroutine, partial]) -> None:
        self.func = func
        self.is_async = is_async_callable(func)


# a mixin class
class EventEmitter:
    __slots__ = ("_event_handlers",)

    def __init__(self) -> None:
        self._event_handlers = defaultdict(set)

    def on(self, event_name: str):
        def decorator(func):
            self._event_handlers[event_name].add(EventHandler(func))
            return func

        return decorator

    async def _emit(self, event_name: str, *args: Any, **kwargs: Any) -> None:
        for handler in self._event_handlers[event_name]:
            if handler.is_async:
                await handler.func(*args, **kwargs)
            else:
                handler.func(*args, **kwargs)


# copy paste from https://github.com/encode/starlette/blob/master/starlette/_utils.py
def is_async_callable(obj: Any) -> bool:
    while isinstance(obj, partial):
        obj = obj.func

    return asyncio.iscoroutinefunction(obj) or (callable(obj) and asyncio.iscoroutinefunction(obj.__call__))
