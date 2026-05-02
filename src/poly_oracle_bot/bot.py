from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig
from .dashboard import Dashboard, DashboardSnapshot
from .execution import executor_for_config
from .feeds import ChainlinkRTDSFeed, PolymarketMarketStream
from .models import MarketWindow, Position, PriceTick
from .orderbook import OrderBookState
from .polymarket import GammaClient
from .recorder import JsonlRecorder, LatencyTrace
from .risk import RiskManager, risk_day_start_ms
from .settlement import gamma_winning_outcome, local_winning_outcome, position_settlement_pnl
from .signal import SignalEngine
from .storage import Storage
from .storage_writer import AsyncStorageWriter
from .telegram import TelegramNotifier


class Bot:
    def __init__(self, cfg: AppConfig, db_path: Path) -> None:
        self.cfg = cfg
        self.storage = Storage(db_path)
        self.storage_writer = AsyncStorageWriter(self.storage, cfg.telemetry.storage_queue_max)
        self._risk_day_start_ms = risk_day_start_ms(
            datetime.now(timezone.utc), cfg.risk.risk_day_timezone
        )
        self.risk = RiskManager(cfg.risk, self.storage.realized_pnl_since(self._risk_day_start_ms))
        self.signal_engine = SignalEngine(cfg.risk)
        self.executor = executor_for_config(cfg)
        self.telegram = TelegramNotifier(cfg.telegram)
        self.recorder = JsonlRecorder(cfg.telemetry)
        self.dashboard = Dashboard()
        self.orderbook = OrderBookState()
        self.market_stream = PolymarketMarketStream(cfg.polymarket, self.on_market_message)
        self.latest_ticks: dict[str, PriceTick] = {}
        self.recent_ticks: dict[str, list[PriceTick]] = {}
        self.markets: dict[str, MarketWindow] = {}
        self.positions: dict[str, Position] = {
            f"{position.asset}:{position.start_ts}": position
            for position in self.storage.load_open_positions()
        }
        self._last_logged_signal: dict[tuple[str, str, str, str], int] = {}

    async def run(self) -> None:
        await self.recorder.start()
        await self.storage_writer.start()
        self.storage_writer.submit(
            self.storage.log_event,
            "INFO",
            "startup",
            "bot starting",
            {"mode": self.cfg.trading.mode},
        )
        await self.telegram.send(f"Polymarket oracle bot starting in {self.cfg.trading.mode} mode")
        symbols = {asset.symbol.upper(): asset.chainlink_symbol for asset in self.cfg.enabled_assets}
        price_feed = ChainlinkRTDSFeed(self.cfg.polymarket, symbols, self.on_tick)
        tasks = [
            asyncio.create_task(price_feed.run()),
            asyncio.create_task(self.market_stream.run()),
            asyncio.create_task(self.market_poll_loop()),
            asyncio.create_task(self.signal_loop()),
            asyncio.create_task(self.settlement_loop()),
            asyncio.create_task(self.heartbeat_loop()),
        ]
        if self.cfg.trading.dashboard_interval_seconds > 0:
            tasks.append(asyncio.create_task(self.dashboard_loop()))
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await self.recorder.stop()
            await self.storage_writer.stop()
            self.storage.close()

    async def on_tick(self, tick: PriceTick) -> None:
        self.latest_ticks[tick.asset] = tick
        ticks = self.recent_ticks.setdefault(tick.asset, [])
        ticks.append(tick)
        if len(ticks) > 2000:
            del ticks[:1000]
        if self.cfg.telemetry.record_ticks:
            self.recorder.record(
                "chainlink_tick",
                {
                    "asset": tick.asset,
                    "symbol": tick.symbol,
                    "price": tick.price,
                    "feed_ts_ms": tick.feed_ts_ms,
                    "received_ts_ms": tick.received_ts_ms,
                    "feed_lag_ms": tick.received_ts_ms - tick.feed_ts_ms,
                },
            )
        self.storage_writer.submit(self.storage.insert_tick, tick)
        for market in self.markets.values():
            if market.asset != tick.asset or market.price_to_beat is not None:
                continue
            feed_ts = tick.feed_ts_ms / 1000.0
            if market.start_ts <= feed_ts <= market.start_ts + self.cfg.trading.opening_capture_grace_seconds:
                market.price_to_beat = tick.price
                self.storage_writer.submit(self.storage.upsert_market, market)
                self.storage_writer.submit(
                    self.storage.log_event,
                    "INFO",
                    "price_to_beat",
                    f"captured {market.asset} opening price",
                    {"slug": market.slug, "price_to_beat": tick.price},
                )
                self.recorder.record(
                    "price_to_beat",
                    {
                        "asset": market.asset,
                        "slug": market.slug,
                        "start_ts": market.start_ts,
                        "price_to_beat": tick.price,
                        "feed_ts_ms": tick.feed_ts_ms,
                    },
                )

    async def on_market_message(self, message: dict[str, Any]) -> None:
        quotes = self.orderbook.update_from_message(message)
        if self.cfg.telemetry.record_orderbook and quotes:
            event_ts = _int_or_none(message.get("timestamp"))
            self.recorder.record(
                "clob_quote_update",
                {
                    "event_type": message.get("event_type"),
                    "timestamp": event_ts,
                    "lag_ms": int(time.time() * 1000) - event_ts if event_ts else None,
                    "quotes": [
                        {
                            "token_id": quote.token_id,
                            "best_bid": quote.best_bid,
                            "best_ask": quote.best_ask,
                            "bid_size": quote.bid_size,
                            "ask_size": quote.ask_size,
                            "ts_ms": quote.ts_ms,
                        }
                        for quote in quotes
                    ],
                },
            )

    async def market_poll_loop(self) -> None:
        async with GammaClient(self.cfg.polymarket) as gamma:
            while True:
                now_ts = int(time.time())
                try:
                    windows = await gamma.discover_windows(
                        self.cfg.enabled_assets,
                        self.cfg.trading.timeframe,
                        now_ts,
                        self.cfg.trading.lookback_windows,
                        self.cfg.trading.lookahead_windows,
                    )
                    for market in windows:
                        prior = self.markets.get(market.key)
                        if prior and prior.price_to_beat is not None and market.price_to_beat is None:
                            market.price_to_beat = prior.price_to_beat
                        if market.price_to_beat is None:
                            self._try_backfill_price_to_beat(market)
                        self.markets[market.key] = market
                        self.storage_writer.submit(self.storage.upsert_market, market)
                        self.recorder.record(
                            "market_window",
                            {
                                "asset": market.asset,
                                "slug": market.slug,
                                "condition_id": market.condition_id,
                                "start_ts": market.start_ts,
                                "end_ts": market.end_ts,
                                "active": market.active,
                                "closed": market.closed,
                                "accepting_orders": market.accepting_orders,
                                "tick_size": market.tick_size,
                                "min_order_size": market.min_order_size,
                                "neg_risk": market.neg_risk,
                                "price_to_beat": market.price_to_beat,
                                "up_token_id": market.tokens["Up"],
                                "down_token_id": market.tokens["Down"],
                            },
                        )
                    asset_ids = {
                        token_id
                        for market in self.markets.values()
                        if not market.closed
                        for token_id in market.tokens.values()
                    }
                    await self.market_stream.set_asset_ids(asset_ids)
                except Exception as exc:
                    self.storage_writer.submit(self.storage.log_event, "ERROR", "market_poll", str(exc))
                await asyncio.sleep(self.cfg.trading.market_poll_interval_seconds)

    async def signal_loop(self) -> None:
        while True:
            try:
                await self._evaluate_once()
            except Exception as exc:
                self.storage_writer.submit(self.storage.log_event, "ERROR", "signal_loop", str(exc))
            await asyncio.sleep(self.cfg.trading.signal_interval_seconds)

    async def _evaluate_once(self) -> None:
        self._refresh_risk_day()
        now_ms = int(time.time() * 1000)
        for market in list(self.markets.values()):
            if market.key in self.positions:
                continue
            tick = self.latest_ticks.get(market.asset)
            if tick is None:
                continue
            outcome = "Up" if market.price_to_beat is not None and tick.price >= market.price_to_beat else "Down"
            token_id = market.tokens.get(outcome)
            quote = self.orderbook.quote(token_id) if token_id else None
            if quote is None:
                continue
            trace = LatencyTrace()
            signal = self.signal_engine.evaluate(market, tick, quote, now_ms=now_ms)
            trace.mark("signal_eval")
            if signal is None:
                continue
            accepted_signal = signal.reason == "accepted"
            if self._should_log_signal(signal.asset, signal.slug, signal.outcome, signal.reason, now_ms):
                self.storage_writer.submit(self.storage.insert_signal, signal, accepted_signal)
                if accepted_signal or self.cfg.telemetry.record_rejected_signals:
                    self.recorder.record(
                        "signal",
                        {
                            "asset": signal.asset,
                            "slug": signal.slug,
                            "condition_id": signal.condition_id,
                            "outcome": signal.outcome,
                            "token_id": signal.token_id,
                            "start_ts": signal.start_ts,
                            "end_ts": signal.end_ts,
                            "price_to_beat": signal.price_to_beat,
                            "observed_price": signal.observed_price,
                            "distance_bps": signal.distance_bps,
                            "estimated_prob": signal.estimated_prob,
                            "ask_price": signal.ask_price,
                            "edge": signal.edge,
                            "reason": signal.reason,
                            "accepted": accepted_signal,
                            "latency": trace.finish(),
                        },
                    )
            if not accepted_signal:
                continue
            open_exposure = sum(position.cost_usd for position in self.positions.values())
            trades_last_hour = self.storage.trades_opened_since(now_ms - 3_600_000)
            size = self.risk.size_for_signal(signal, market, open_exposure, trades_last_hour)
            trace.mark("risk_size")
            if not size.accepted:
                rejected = self._replace_signal_reason(signal, size.reason)
                self.storage_writer.submit(self.storage.insert_signal, rejected, False)
                self.recorder.record(
                    "signal_sizing_reject",
                    {
                        "asset": rejected.asset,
                        "slug": rejected.slug,
                        "outcome": rejected.outcome,
                        "reason": rejected.reason,
                        "sized_cost_usd": size.cost_usd,
                        "kelly_fraction": size.kelly_fraction,
                        "latency": trace.finish(),
                    },
                )
                continue
            result = await self.executor.submit(signal, size, market)
            trace.mark("order_submit")
            if not result.filled:
                rejected = self._replace_signal_reason(signal, result.message)
                self.storage_writer.submit(self.storage.insert_signal, rejected, False)
                self.recorder.record(
                    "order_reject",
                    {
                        "asset": rejected.asset,
                        "slug": rejected.slug,
                        "outcome": rejected.outcome,
                        "reason": rejected.reason,
                        "cost_usd": size.cost_usd,
                        "shares": size.shares,
                        "latency": trace.finish(),
                    },
                )
                continue
            position = Position(
                trade_id=str(uuid.uuid4()),
                mode=self.cfg.trading.mode,
                asset=signal.asset,
                slug=signal.slug,
                condition_id=signal.condition_id,
                outcome=signal.outcome,
                token_id=signal.token_id,
                start_ts=signal.start_ts,
                end_ts=signal.end_ts,
                shares=result.shares,
                entry_price=result.fill_price,
                cost_usd=result.fill_price * result.shares,
                order_id=result.order_id,
                price_to_beat=signal.price_to_beat,
                entry_oracle_price=signal.observed_price,
                opened_at_ms=now_ms,
            )
            self.positions[market.key] = position
            self.storage_writer.submit(self.storage.open_trade, position)
            self.recorder.record(
                "position_open",
                {
                    "trade_id": position.trade_id,
                    "mode": position.mode,
                    "asset": position.asset,
                    "slug": position.slug,
                    "outcome": position.outcome,
                    "shares": position.shares,
                    "entry_price": position.entry_price,
                    "cost_usd": position.cost_usd,
                    "order_id": position.order_id,
                    "latency": trace.finish(),
                },
            )
            await self.telegram.send(
                f"OPEN {position.mode} {position.asset} {position.outcome} "
                f"{position.shares:.4f} @ {position.entry_price:.3f} "
                f"cost={position.cost_usd:.2f} slug={position.slug}"
            )

    async def settlement_loop(self) -> None:
        async with GammaClient(self.cfg.polymarket) as gamma:
            while True:
                try:
                    await self._settle_once(gamma)
                except Exception as exc:
                    self.storage_writer.submit(self.storage.log_event, "ERROR", "settlement_loop", str(exc))
                await asyncio.sleep(self.cfg.trading.settlement_poll_interval_seconds)

    async def _settle_once(self, gamma: GammaClient) -> None:
        self._refresh_risk_day()
        now_ts = time.time()
        for key, position in list(self.positions.items()):
            if now_ts < position.end_ts + self.cfg.trading.settlement_grace_seconds:
                continue
            tick = self._settlement_tick_for(position)
            if tick is None:
                continue
            winning = None
            source = "local_chainlink"
            if position.mode == "live":
                event = await gamma.fetch_event_by_slug(position.slug)
                if event:
                    winning = gamma_winning_outcome(event)
                    if winning:
                        source = "gamma"
            if winning is None:
                winning = local_winning_outcome(position.price_to_beat, tick.price)
            pnl = position_settlement_pnl(position, winning)
            closed_at_ms = int(time.time() * 1000)
            self.storage_writer.submit(
                self.storage.close_trade,
                position.trade_id,
                closed_at_ms,
                tick.price,
                winning,
                pnl,
                {"settlement_source": source},
            )
            self.risk.apply_realized_pnl(pnl)
            del self.positions[key]
            self.recorder.record(
                "position_close",
                {
                    "trade_id": position.trade_id,
                    "mode": position.mode,
                    "asset": position.asset,
                    "slug": position.slug,
                    "outcome": position.outcome,
                    "winning_outcome": winning,
                    "entry_price": position.entry_price,
                    "shares": position.shares,
                    "cost_usd": position.cost_usd,
                    "exit_oracle_price": tick.price,
                    "realized_pnl": pnl,
                    "settlement_source": source,
                },
            )
            await self.telegram.send(
                f"CLOSE {position.mode} {position.asset} {position.outcome} "
                f"winner={winning} pnl={pnl:.2f} source={source} slug={position.slug}"
            )

    def _settlement_tick_for(self, position: Position) -> PriceTick | None:
        end_ms = position.end_ts * 1000
        candidates = [tick for tick in self.recent_ticks.get(position.asset, []) if tick.feed_ts_ms >= end_ms]
        if candidates:
            return min(candidates, key=lambda tick: tick.feed_ts_ms)
        latest = self.latest_ticks.get(position.asset)
        if latest and latest.feed_ts_ms >= end_ms:
            return latest
        return None

    def _try_backfill_price_to_beat(self, market: MarketWindow) -> None:
        start_ms = market.start_ts * 1000
        end_ms = (market.start_ts + self.cfg.trading.opening_capture_grace_seconds) * 1000
        candidates = [
            tick
            for tick in self.recent_ticks.get(market.asset, [])
            if start_ms <= tick.feed_ts_ms <= end_ms
        ]
        if candidates:
            market.price_to_beat = min(candidates, key=lambda tick: tick.feed_ts_ms).price

    async def dashboard_loop(self) -> None:
        while True:
            self.dashboard.render(
                DashboardSnapshot(
                    ticks=dict(self.latest_ticks),
                    markets=dict(self.markets),
                    quotes=dict(self.orderbook.quotes),
                    positions=dict(self.positions),
                    realized_pnl_today=self.risk.realized_pnl_today,
                    drawdown_blocked=self.risk.drawdown_blocked,
                )
            )
            await asyncio.sleep(self.cfg.trading.dashboard_interval_seconds)

    async def heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.telegram.heartbeat_interval_seconds)
            await self.telegram.send(
                f"HEARTBEAT mode={self.cfg.trading.mode} "
                f"positions={len(self.positions)} pnl_today={self.risk.realized_pnl_today:.2f}"
            )

    def _should_log_signal(
        self,
        asset: str,
        slug: str,
        outcome: str,
        reason: str,
        now_ms: int,
    ) -> bool:
        key = (asset, slug, outcome, reason)
        prior = self._last_logged_signal.get(key, 0)
        if now_ms - prior < 2000:
            return False
        self._last_logged_signal[key] = now_ms
        return True

    def _replace_signal_reason(self, signal: Any, reason: str) -> Any:
        from dataclasses import replace

        return replace(signal, reason=reason)

    def _refresh_risk_day(self) -> None:
        current = risk_day_start_ms(datetime.now(timezone.utc), self.cfg.risk.risk_day_timezone)
        if current == self._risk_day_start_ms:
            return
        self._risk_day_start_ms = current
        self.risk.realized_pnl_today = self.storage.realized_pnl_since(current)
        self.storage_writer.submit(
            self.storage.log_event,
            "INFO",
            "risk_day_reset",
            "risk day rolled; reloaded realized pnl",
            {"realized_pnl_today": self.risk.realized_pnl_today},
        )


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None
