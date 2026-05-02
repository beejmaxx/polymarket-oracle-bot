from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from typing import Any

from .config import PolymarketConfig
from .models import PriceTick

TickCallback = Callable[[PriceTick], Awaitable[None]]
MessageCallback = Callable[[dict[str, Any]], Awaitable[None]]


class ChainlinkRTDSFeed:
    def __init__(
        self,
        cfg: PolymarketConfig,
        symbols_by_asset: dict[str, str],
        callback: TickCallback,
    ) -> None:
        self.cfg = cfg
        self.symbols_by_asset = {symbol.lower(): asset for asset, symbol in symbols_by_asset.items()}
        self.callback = callback

    async def run(self) -> None:
        import websockets

        backoff = 1.0
        while True:
            try:
                async with websockets.connect(self.cfg.rtds_ws_url, ping_interval=None) as ws:
                    await ws.send(json.dumps(self._subscription_message()))
                    ping_task = asyncio.create_task(_text_ping_loop(ws, "PING", 5.0))
                    try:
                        async for raw in ws:
                            await self._handle_raw(raw)
                    finally:
                        ping_task.cancel()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.8, 30.0)

    def _subscription_message(self) -> dict[str, Any]:
        return {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": "",
                }
            ],
        }

    async def _handle_raw(self, raw: str | bytes) -> None:
        if raw in ("PONG", b"PONG"):
            return
        try:
            message = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return
        payload = message.get("payload") or {}
        symbol = str(payload.get("symbol") or "").lower()
        asset = self.symbols_by_asset.get(symbol)
        if not asset:
            return
        if isinstance(payload.get("data"), list):
            for item in payload["data"]:
                if not isinstance(item, dict):
                    continue
                await self._emit_payload_item(asset, symbol, item)
            return
        await self._emit_payload_item(asset, symbol, payload)

    async def _emit_payload_item(self, asset: str, symbol: str, payload: dict[str, Any]) -> None:
        try:
            price = float(payload["value"])
            feed_ts_ms = int(payload["timestamp"])
        except (KeyError, TypeError, ValueError):
            return
        await self.callback(
            PriceTick(
                asset=asset,
                symbol=symbol,
                price=price,
                feed_ts_ms=feed_ts_ms,
                received_ts_ms=int(time.time() * 1000),
            )
        )


class PolymarketMarketStream:
    def __init__(self, cfg: PolymarketConfig, callback: MessageCallback) -> None:
        self.cfg = cfg
        self.callback = callback
        self._asset_ids: set[str] = set()
        self._updates: asyncio.Queue[set[str]] = asyncio.Queue()

    async def set_asset_ids(self, asset_ids: set[str]) -> None:
        clean = {str(asset_id) for asset_id in asset_ids if asset_id}
        self._asset_ids = clean
        await self._updates.put(clean)

    async def run(self) -> None:
        import websockets

        backoff = 1.0
        while True:
            try:
                async with websockets.connect(self.cfg.market_ws_url, ping_interval=None) as ws:
                    subscribed: set[str] = set()
                    if self._asset_ids:
                        await ws.send(json.dumps(_market_subscribe(self._asset_ids)))
                        subscribed = set(self._asset_ids)
                    ping_task = asyncio.create_task(
                        _market_ping_loop(ws, self.cfg.market_ws_ping_payload, 10.0)
                    )
                    update_task = asyncio.create_task(self._subscription_loop(ws, subscribed))
                    try:
                        async for raw in ws:
                            await self._handle_raw(raw)
                    finally:
                        ping_task.cancel()
                        update_task.cancel()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.8, 30.0)

    async def _subscription_loop(self, ws: Any, subscribed: set[str]) -> None:
        while True:
            wanted = await self._updates.get()
            new_ids = wanted - subscribed
            if new_ids:
                await ws.send(json.dumps({"operation": "subscribe", "assets_ids": sorted(new_ids)}))
                subscribed |= new_ids

    async def _handle_raw(self, raw: str | bytes) -> None:
        if raw in ("PONG", b"PONG"):
            return
        try:
            message = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return
        if isinstance(message, list):
            for item in message:
                if isinstance(item, dict):
                    await self.callback(item)
            return
        if isinstance(message, dict):
            await self.callback(message)


def _market_subscribe(asset_ids: set[str]) -> dict[str, Any]:
    return {
        "assets_ids": sorted(asset_ids),
        "type": "market",
        "custom_feature_enabled": True,
    }


async def _text_ping_loop(ws: Any, payload: str, interval_seconds: float) -> None:
    while True:
        await asyncio.sleep(interval_seconds)
        await ws.send(payload)


async def _market_ping_loop(ws: Any, payload: str, interval_seconds: float) -> None:
    while True:
        await asyncio.sleep(interval_seconds)
        if payload.strip().startswith("{"):
            await ws.send(payload)
        else:
            await ws.send(payload)
