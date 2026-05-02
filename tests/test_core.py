from __future__ import annotations

import tempfile
import unittest
import asyncio
import json
from pathlib import Path

from poly_oracle_bot.config import AppConfig, TradingConfig
from poly_oracle_bot.execution import _is_immediate_fill
from poly_oracle_bot.feeds import ChainlinkRTDSFeed
from poly_oracle_bot.models import MarketWindow, Position, PriceTick, Quote
from poly_oracle_bot.orderbook import OrderBookState
from poly_oracle_bot.polymarket import parse_market_event
from poly_oracle_bot.recorder import JsonlRecorder
from poly_oracle_bot.risk import RiskManager
from poly_oracle_bot.settlement import gamma_winning_outcome, local_winning_outcome, position_settlement_pnl
from poly_oracle_bot.signal import SignalEngine
from poly_oracle_bot.storage import Storage
from poly_oracle_bot.timeframes import candidate_window_starts, slug_for


class CoreTests(unittest.TestCase):
    def test_slug_generation(self) -> None:
        self.assertEqual(slug_for("btc", 1777738500), "btc-updown-15m-1777738500")
        self.assertEqual(
            candidate_window_starts(1777738948, "15m", 1, 1),
            [1777737600, 1777738500, 1777739400],
        )

    def test_live_mode_requires_three_gates(self) -> None:
        cfg = AppConfig(trading=TradingConfig(mode="live"))
        with self.assertRaisesRegex(ValueError, "live mode refused"):
            cfg.validate()

    def test_parse_market_event(self) -> None:
        asset = AppConfig().assets[0]
        event = {
            "id": "1",
            "slug": "btc-updown-15m-1777738500",
            "active": True,
            "closed": False,
            "markets": [
                {
                    "id": "2",
                    "conditionId": "0xabc",
                    "eventStartTime": "2026-05-02T16:15:00Z",
                    "endDate": "2026-05-02T16:30:00Z",
                    "outcomes": '["Up","Down"]',
                    "clobTokenIds": '["up-token","down-token"]',
                    "orderPriceMinTickSize": 0.01,
                    "orderMinSize": 5,
                    "active": True,
                    "closed": False,
                    "acceptingOrders": True,
                }
            ],
        }
        market = parse_market_event(asset, event)
        self.assertIsNotNone(market)
        assert market is not None
        self.assertEqual(market.start_ts, 1777738500)
        self.assertEqual(market.tokens["Up"], "up-token")

    def test_orderbook_best_quote(self) -> None:
        state = OrderBookState()
        state.update_from_message(
            {
                "event_type": "book",
                "asset_id": "up-token",
                "bids": [{"price": "0.40", "size": "10"}, {"price": "0.42", "size": "5"}],
                "asks": [{"price": "0.45", "size": "7"}, {"price": "0.44", "size": "9"}],
                "timestamp": "1777738500000",
            }
        )
        quote = state.quote("up-token")
        self.assertIsNotNone(quote)
        assert quote is not None
        self.assertEqual(quote.best_bid, 0.42)
        self.assertEqual(quote.best_ask, 0.44)

    def test_signal_and_size(self) -> None:
        cfg = AppConfig()
        market = _market()
        market.price_to_beat = 100.0
        tick = PriceTick("BTC", "btc/usd", 101.0, 1777738510000, 1777738510000)
        quote = Quote("up-token", best_ask=0.55)
        signal = SignalEngine(cfg.risk).evaluate(market, tick, quote, now_ms=1777738510000)
        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.outcome, "Up")
        self.assertEqual(signal.reason, "accepted")
        size = RiskManager(cfg.risk).size_for_signal(signal, market)
        self.assertTrue(size.accepted)

    def test_settlement(self) -> None:
        pos = Position(
            trade_id="t",
            mode="paper",
            asset="BTC",
            slug="btc-updown-15m-1",
            condition_id="0xabc",
            outcome="Up",
            token_id="up-token",
            start_ts=1,
            end_ts=901,
            shares=10.0,
            entry_price=0.55,
            cost_usd=5.5,
            order_id=None,
            price_to_beat=100.0,
            entry_oracle_price=101.0,
            opened_at_ms=1000,
        )
        self.assertEqual(local_winning_outcome(100.0, 100.0), "Up")
        self.assertAlmostEqual(position_settlement_pnl(pos, "Up"), 4.5)
        self.assertAlmostEqual(position_settlement_pnl(pos, "Down"), -5.5)
        self.assertEqual(
            gamma_winning_outcome(
                {"markets": [{"umaResolutionStatus": "resolved", "outcomes": '["Up","Down"]', "outcomePrices": '["0","1"]'}]}
            ),
            "Down",
        )

    def test_storage_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "bot.sqlite3")
            market = _market()
            storage.upsert_market(market)
            storage.insert_tick(PriceTick("BTC", "btc/usd", 100.0, 1, 2))
            self.assertEqual(storage.realized_pnl_since(0), 0.0)
            storage.close()

    def test_jsonl_recorder(self) -> None:
        async def run(path: Path) -> None:
            cfg = AppConfig().telemetry
            cfg.path = str(path)
            cfg.flush_interval_seconds = 0.05
            recorder = JsonlRecorder(cfg)
            await recorder.start()
            recorder.record("test_event", {"asset": "BTC", "value": 1})
            await recorder.stop()

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            asyncio.run(run(path))
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(rows[0]["event_type"], "test_event")
            self.assertEqual(rows[0]["payload"]["asset"], "BTC")

    def test_chainlink_snapshot_parser(self) -> None:
        ticks = []

        async def run() -> None:
            async def cb(tick: PriceTick) -> None:
                ticks.append(tick)

            feed = ChainlinkRTDSFeed(AppConfig().polymarket, {"BTC": "btc/usd"}, cb)
            await feed._handle_raw(
                json.dumps(
                    {
                        "topic": "crypto_prices",
                        "type": "subscribe",
                        "payload": {
                            "symbol": "btc/usd",
                            "data": [
                                {"timestamp": 10, "value": 100.0},
                                {"timestamp": 11, "value": 101.0},
                            ],
                        },
                    }
                )
            )

        asyncio.run(run())
        self.assertEqual([tick.price for tick in ticks], [100.0, 101.0])

    def test_live_order_fill_detection(self) -> None:
        self.assertTrue(_is_immediate_fill({"success": True, "status": "matched"}))
        self.assertFalse(_is_immediate_fill({"success": True, "status": "live"}))
        self.assertFalse(_is_immediate_fill({"success": False, "status": "matched"}))


def _market() -> MarketWindow:
    return MarketWindow(
        asset="BTC",
        slug="btc-updown-15m-1777738500",
        event_id="1",
        market_id="2",
        condition_id="0xabc",
        start_ts=1777738500,
        end_ts=1777739400,
        tokens={"Up": "up-token", "Down": "down-token"},
        tick_size=0.01,
        min_order_size=5.0,
        neg_risk=False,
        active=True,
        closed=False,
        accepting_orders=True,
    )


if __name__ == "__main__":
    unittest.main()
