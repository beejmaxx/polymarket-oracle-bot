from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from .storage import Storage


class AsyncStorageWriter:
    def __init__(self, storage: Storage, queue_max: int = 20000) -> None:
        self.storage = storage
        self._queue: asyncio.Queue[tuple[Callable[..., Any], tuple[Any, ...]] | None] = (
            asyncio.Queue(maxsize=max(1, queue_max))
        )
        self._task: asyncio.Task[None] | None = None
        self.dropped_writes = 0

    async def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is None:
            return
        await self._queue.join()
        await self._queue.put(None)
        await self._task
        self._task = None

    def submit(self, fn: Callable[..., Any], *args: Any) -> None:
        try:
            self._queue.put_nowait((fn, args))
        except asyncio.QueueFull:
            self.dropped_writes += 1

    async def _run(self) -> None:
        while True:
            item = await self._queue.get()
            try:
                if item is None:
                    return
                fn, args = item
                await asyncio.to_thread(fn, *args)
            finally:
                self._queue.task_done()

