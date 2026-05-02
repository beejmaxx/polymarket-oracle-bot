from __future__ import annotations

import tempfile
import unittest
import asyncio
import json
from pathlib import Path
from unittest.mock import patch

from poly_oracle_bot.config import AppConfig, RiskConfig, TradingConfig
from poly_oracle_bot.execution import LiveExecutor, _OrderApi, _is_immediate_fill, live_executor_dry_run
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

    def test_signal_rejects_stale_oracle_tick(self) -> None:
        cfg = AppConfig()
        market = _market()
        market.price_to_beat = 100.0
        tick = PriceTick("BTC", "btc/usd", 101.0, 1777738500000, 1777738500000)
        quote = Quote("up-token", best_ask=0.55)
        signal = SignalEngine(cfg.risk).evaluate(market, tick, quote, now_ms=1777738510000)
        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.reason, "oracle tick stale")

    def test_signal_records_distance_rejection(self) -> None:
        cfg = AppConfig()
        market = _market()
        market.price_to_beat = 100.0
        tick = PriceTick("BTC", "btc/usd", 100.01, 1777738510000, 1777738510000)
        quote = Quote("up-token", best_ask=0.55)
        signal = SignalEngine(cfg.risk).evaluate(market, tick, quote, now_ms=1777738510000)
        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.reason, "distance below threshold")

    def test_risk_limits_open_exposure_and_trade_rate(self) -> None:
        cfg = RiskConfig(max_open_exposure_usd=10.0, max_trades_per_hour=2)
        manager = RiskManager(cfg)
        market = _market()
        market.price_to_beat = 100.0
        tick = PriceTick("BTC", "btc/usd", 101.0, 1777738510000, 1777738510000)
        quote = Quote("up-token", best_ask=0.55)
        signal = SignalEngine(cfg).evaluate(market, tick, quote, now_ms=1777738510000)
        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(
            manager.size_for_signal(signal, market, open_exposure_usd=10.0).reason,
            "max_open_exposure_usd limit active",
        )
        self.assertEqual(
            manager.size_for_signal(signal, market, trades_opened_last_hour=2).reason,
            "max_trades_per_hour limit active",
        )

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

    def test_storage_load_open_positions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "bot.sqlite3")
            position = Position(
                trade_id="trade-1",
                mode="paper",
                asset="BTC",
                slug="btc-updown-15m-1777738500",
                condition_id="0xabc",
                outcome="Up",
                token_id="up-token",
                start_ts=1777738500,
                end_ts=1777739400,
                shares=10.0,
                entry_price=0.55,
                cost_usd=5.5,
                order_id="paper-order",
                price_to_beat=100.0,
                entry_oracle_price=101.0,
                opened_at_ms=1777738510000,
            )
            storage.open_trade(position)
            recovered = storage.load_open_positions()
            self.assertEqual(len(recovered), 1)
            self.assertEqual(recovered[0].trade_id, "trade-1")
            self.assertEqual(recovered[0].start_ts, 1777738500)
            self.assertEqual(recovered[0].end_ts, 1777739400)
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

    def test_executor_dry_run_requires_credentials_before_client(self) -> None:
        api = _fake_order_api()
        with (
            patch.dict("os.environ", {}, clear=True),
            patch("poly_oracle_bot.execution._import_order_api", return_value=api),
            patch.object(LiveExecutor, "_create_v2_client", side_effect=AssertionError("should not build client")),
        ):
            result = live_executor_dry_run(AppConfig(), _market())
        self.assertFalse(result.ok)
        self.assertIn("missing POLYMARKET_PRIVATE_KEY", result.message)
        self.assertIn("no order submitted", result.message)

    def test_executor_dry_run_signs_without_posting(self) -> None:
        class FakeClient:
            def get_balance_allowance(self, _params: object) -> dict[str, str]:
                return {"balance": "1000000", "allowance": "1000000"}

            def create_market_order(self, order_args: object, options: object | None = None) -> dict[str, bool]:
                return {"signed": True}

            def post_order(self, *_args: object, **_kwargs: object) -> None:
                raise AssertionError("dry-run must not post")

        env = {
            "POLYMARKET_PRIVATE_KEY": "0x" + "1" * 64,
            "POLYMARKET_API_KEY": "api-key",
            "POLYMARKET_API_SECRET": "api-secret",
            "POLYMARKET_API_PASSPHRASE": "api-passphrase",
        }
        with (
            patch.dict("os.environ", env, clear=True),
            patch("poly_oracle_bot.execution._import_order_api", return_value=_fake_order_api()),
            patch.object(LiveExecutor, "_create_v2_client", return_value=FakeClient()),
        ):
            result = live_executor_dry_run(AppConfig(), _market())
        self.assertTrue(result.ok)
        self.assertIn("signed_type=dict", result.message)
        self.assertIn("no order submitted", result.message)


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


def _fake_order_api() -> _OrderApi:
    class FakeOrderType:
        FOK = "FOK"

    class FakeOrderArgs:
        def __init__(self, **_kwargs: object) -> None:
            pass

    class FakeOptions:
        def __init__(self, **_kwargs: object) -> None:
            pass

    class FakeBalanceAllowanceParams:
        def __init__(self, **_kwargs: object) -> None:
            pass

    class FakeAssetType:
        COLLATERAL = "COLLATERAL"

    return _OrderApi(
        variant="v2",
        market_order_args=FakeOrderArgs,
        partial_options=FakeOptions,
        order_type=FakeOrderType,
        buy_side="BUY",
        balance_allowance_params=FakeBalanceAllowanceParams,
        asset_type=FakeAssetType,
    )


if __name__ == "__main__":
    unittest.main()
