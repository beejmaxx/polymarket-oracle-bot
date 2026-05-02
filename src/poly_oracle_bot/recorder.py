from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from .config import TelemetryConfig

_TIMEOUT = object()


class JsonlRecorder:
    def __init__(self, cfg: TelemetryConfig) -> None:
        self.cfg = cfg
        self._queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(
            maxsize=max(1, cfg.queue_max)
        )
        self._task: asyncio.Task[None] | None = None
        self.dropped_events = 0

    async def start(self) -> None:
        if not self.cfg.enabled or self._task is not None:
            return
        Path(self.cfg.path).parent.mkdir(parents=True, exist_ok=True)
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is None:
            return
        await self._queue.put(None)
        await self._task
        self._task = None

    def record(self, event_type: str, payload: dict[str, Any]) -> None:
        if not self.cfg.enabled:
            return
        event = {
            "ts_ms": int(time.time() * 1000),
            "perf_ns": time.perf_counter_ns(),
            "event_type": event_type,
            "payload": _jsonable(payload),
        }
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped_events += 1

    async def _run(self) -> None:
        batch: list[dict[str, Any]] = []
        while True:
            try:
                item = await asyncio.wait_for(
                    self._queue.get(),
                    timeout=max(0.05, self.cfg.flush_interval_seconds),
                )
            except asyncio.TimeoutError:
                item = _TIMEOUT

            if item is None:
                if batch:
                    await asyncio.to_thread(_append_jsonl, self.cfg.path, batch)
                    batch.clear()
                return

            if item is _TIMEOUT:
                if batch:
                    await asyncio.to_thread(_append_jsonl, self.cfg.path, batch)
                    batch.clear()
                continue

            batch.append(item)
            if len(batch) >= self.cfg.batch_size:
                await asyncio.to_thread(_append_jsonl, self.cfg.path, batch)
                batch.clear()


class LatencyTrace:
    def __init__(self) -> None:
        self._start_ns = time.perf_counter_ns()
        self._last_ns = self._start_ns
        self.spans: dict[str, float] = {}

    def mark(self, name: str) -> None:
        now = time.perf_counter_ns()
        self.spans[f"{name}_ms"] = (now - self._last_ns) / 1_000_000.0
        self._last_ns = now

    def finish(self) -> dict[str, float]:
        now = time.perf_counter_ns()
        self.spans["total_ms"] = (now - self._start_ns) / 1_000_000.0
        return dict(self.spans)


def _append_jsonl(path: str, batch: list[dict[str, Any]]) -> None:
    with Path(path).open("a", encoding="utf-8") as handle:
        for event in batch:
            handle.write(json.dumps(event, separators=(",", ":"), sort_keys=True))
            handle.write("\n")


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
