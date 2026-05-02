#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from poly_oracle_bot.config import load_config
from poly_oracle_bot.models import MarketWindow, Position, PriceTick, Quote
from poly_oracle_bot.settlement import position_settlement_pnl
from poly_oracle_bot.signal import SignalEngine


@dataclass(slots=True)
class ReplayResult:
    events: int = 0
    markets_seen: int = 0
    ticks_seen: int = 0
    quote_updates_seen: int = 0
    recorded_signals: int = 0
    replayed_signals: int = 0
    matched_signals: int = 0
    mismatched_signals: int = 0
    unverifiable_signals: int = 0
    opened_positions: int = 0
    closed_positions: int = 0
    settlement_matches: int = 0
    settlement_mismatches: int = 0
    notes: list[str] | None = None

    @property
    def ok(self) -> bool:
        return self.mismatched_signals == 0 and self.settlement_mismatches == 0


def replay(events_path: Path, config_path: Path | None = None) -> ReplayResult:
    cfg = load_config(config_path if config_path and config_path.exists() else None)
    engine = SignalEngine(cfg.risk)
    result = ReplayResult(notes=[])
    markets: dict[tuple[str, str], MarketWindow] = {}
    latest_ticks: dict[str, PriceTick] = {}
    quotes: dict[str, Quote] = {}
    positions: dict[str, Position] = {}

    for event in read_events(events_path):
        result.events += 1
        event_type = str(event.get("event_type") or "")
        payload = event.get("payload") or {}
        if event_type == "market_window":
            market = _market_from_payload(payload)
            markets[(market.asset, market.slug)] = market
            result.markets_seen += 1
            continue
        if event_type == "price_to_beat":
            _apply_price_to_beat(markets, payload)
            continue
        if event_type == "chainlink_tick":
            tick = _tick_from_payload(payload)
            latest_ticks[tick.asset] = tick
            result.ticks_seen += 1
            continue
        if event_type == "clob_quote_update":
            for quote in _quotes_from_payload(payload):
                quotes[quote.token_id] = quote
                result.quote_updates_seen += 1
            continue
        if event_type == "signal":
            result.recorded_signals += 1
            market = markets.get((str(payload.get("asset")), str(payload.get("slug"))))
            tick = latest_ticks.get(str(payload.get("asset")))
            quote = quotes.get(str(payload.get("token_id")))
            if market is None or tick is None or quote is None:
                result.unverifiable_signals += 1
                continue
            signal = engine.evaluate(market, tick, quote, now_ms=int(payload.get("created_at_ms") or event["ts_ms"]))
            if signal is None:
                result.mismatched_signals += 1
                continue
            result.replayed_signals += 1
            if _signal_matches(payload, signal):
                result.matched_signals += 1
            else:
                result.mismatched_signals += 1
            continue
        if event_type == "position_open":
            position = _position_from_payload(payload, markets, int(event.get("ts_ms") or 0))
            if position is not None:
                positions[position.trade_id] = position
                result.opened_positions += 1
            continue
        if event_type == "position_close":
            trade_id = str(payload.get("trade_id") or "")
            position = positions.pop(trade_id, None)
            if position is None:
                continue
            result.closed_positions += 1
            winning = str(payload.get("winning_outcome") or "")
            try:
                recorded_pnl = float(payload.get("realized_pnl"))
            except (TypeError, ValueError):
                result.settlement_mismatches += 1
                continue
            replayed_pnl = position_settlement_pnl(position, winning)  # type: ignore[arg-type]
            if abs(replayed_pnl - recorded_pnl) <= 1e-9:
                result.settlement_matches += 1
            else:
                result.settlement_mismatches += 1

    if result.recorded_signals == 0 and result.notes is not None:
        result.notes.append("No recorded signal events were present; replay validated ingestion state only.")
    return result


def read_events(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise SystemExit(f"{path}:{line_no}: invalid JSON: {exc}") from exc
    return events


def format_result(result: ReplayResult) -> str:
    lines = [
        "# Replay Result",
        "",
        f"ok: {str(result.ok).lower()}",
        f"events: {result.events}",
        f"markets_seen: {result.markets_seen}",
        f"ticks_seen: {result.ticks_seen}",
        f"quote_updates_seen: {result.quote_updates_seen}",
        f"recorded_signals: {result.recorded_signals}",
        f"replayed_signals: {result.replayed_signals}",
        f"matched_signals: {result.matched_signals}",
        f"mismatched_signals: {result.mismatched_signals}",
        f"unverifiable_signals: {result.unverifiable_signals}",
        f"opened_positions: {result.opened_positions}",
        f"closed_positions: {result.closed_positions}",
        f"settlement_matches: {result.settlement_matches}",
        f"settlement_mismatches: {result.settlement_mismatches}",
    ]
    for note in result.notes or []:
        lines.append(f"note: {note}")
    return "\n".join(lines)


def _market_from_payload(payload: dict[str, Any]) -> MarketWindow:
    return MarketWindow(
        asset=str(payload["asset"]),
        slug=str(payload["slug"]),
        event_id="",
        market_id="",
        condition_id=str(payload.get("condition_id") or ""),
        start_ts=int(payload["start_ts"]),
        end_ts=int(payload["end_ts"]),
        tokens={"Up": str(payload["up_token_id"]), "Down": str(payload["down_token_id"])},
        tick_size=float(payload.get("tick_size") or 0.01),
        min_order_size=float(payload.get("min_order_size") or 5.0),
        neg_risk=bool(payload.get("neg_risk")),
        active=bool(payload.get("active")),
        closed=bool(payload.get("closed")),
        accepting_orders=bool(payload.get("accepting_orders")),
        price_to_beat=_float_or_none(payload.get("price_to_beat")),
    )


def _apply_price_to_beat(markets: dict[tuple[str, str], MarketWindow], payload: dict[str, Any]) -> None:
    market = markets.get((str(payload.get("asset")), str(payload.get("slug"))))
    if market is not None:
        market.price_to_beat = _float_or_none(payload.get("price_to_beat"))


def _tick_from_payload(payload: dict[str, Any]) -> PriceTick:
    return PriceTick(
        asset=str(payload["asset"]),
        symbol=str(payload["symbol"]),
        price=float(payload["price"]),
        feed_ts_ms=int(payload["feed_ts_ms"]),
        received_ts_ms=int(payload["received_ts_ms"]),
    )


def _quotes_from_payload(payload: dict[str, Any]) -> list[Quote]:
    quotes = []
    for raw in payload.get("quotes") or []:
        quotes.append(
            Quote(
                token_id=str(raw["token_id"]),
                best_bid=_float_or_none(raw.get("best_bid")),
                best_ask=_float_or_none(raw.get("best_ask")),
                bid_size=_float_or_none(raw.get("bid_size")),
                ask_size=_float_or_none(raw.get("ask_size")),
                ts_ms=int(raw["ts_ms"]) if raw.get("ts_ms") is not None else None,
            )
        )
    return quotes


def _signal_matches(payload: dict[str, Any], signal: Any) -> bool:
    checks = [
        str(payload.get("reason")) == signal.reason,
        str(payload.get("outcome")) == signal.outcome,
        str(payload.get("token_id")) == signal.token_id,
        bool(payload.get("accepted")) == (signal.reason == "accepted"),
        _near(float(payload.get("ask_price")), signal.ask_price),
        _near(float(payload.get("edge")), signal.edge),
    ]
    return all(checks)


def _position_from_payload(
    payload: dict[str, Any],
    markets: dict[tuple[str, str], MarketWindow],
    opened_at_ms: int,
) -> Position | None:
    market = markets.get((str(payload.get("asset")), str(payload.get("slug"))))
    if market is None or market.price_to_beat is None:
        return None
    return Position(
        trade_id=str(payload["trade_id"]),
        mode=str(payload.get("mode") or "paper"),  # type: ignore[arg-type]
        asset=market.asset,
        slug=market.slug,
        condition_id=market.condition_id,
        outcome=str(payload["outcome"]),  # type: ignore[arg-type]
        token_id=market.token_for(str(payload["outcome"])),  # type: ignore[arg-type]
        start_ts=market.start_ts,
        end_ts=market.end_ts,
        shares=float(payload["shares"]),
        entry_price=float(payload["entry_price"]),
        cost_usd=float(payload["cost_usd"]),
        order_id=payload.get("order_id"),
        price_to_beat=market.price_to_beat,
        entry_oracle_price=market.price_to_beat,
        opened_at_ms=opened_at_ms,
    )


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _near(left: float, right: float, tolerance: float = 1e-9) -> bool:
    return abs(left - right) <= tolerance


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay JSONL telemetry and validate recorded decisions")
    parser.add_argument("events", type=Path, nargs="?", default=Path("data/events.jsonl"))
    parser.add_argument("--config", type=Path, default=Path("config.toml"))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    result = replay(args.events, args.config)
    if args.json:
        print(json.dumps(asdict(result), indent=2, sort_keys=True))
    else:
        print(format_result(result))
    raise SystemExit(0 if result.ok else 1)


if __name__ == "__main__":
    main()
