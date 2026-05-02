#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from analyze_telemetry import percentile
from replay_telemetry import replay


def generate(
    events_path: Path,
    db_path: Path,
    output_path: Path | None = None,
    config_path: Path | None = None,
) -> str:
    events = read_events(events_path)
    if not events:
        raise SystemExit(f"{events_path} has no telemetry events")

    counts = Counter(str(event.get("event_type") or "") for event in events)
    payloads = [(str(event.get("event_type") or ""), event.get("payload") or {}) for event in events]
    first_ts = min(int(event["ts_ms"]) for event in events)
    last_ts = max(int(event["ts_ms"]) for event in events)
    duration_s = max(0.0, (last_ts - first_ts) / 1000.0)

    chainlink_lags = [
        float(payload["feed_lag_ms"])
        for event_type, payload in payloads
        if event_type == "chainlink_tick" and payload.get("feed_lag_ms") is not None
    ]
    chainlink_lags_under_5s = [lag for lag in chainlink_lags if lag < 5_000.0]
    clob_lags = [
        float(payload["lag_ms"])
        for event_type, payload in payloads
        if event_type == "clob_quote_update" and payload.get("lag_ms") is not None
    ]
    signals = [payload for event_type, payload in payloads if event_type == "signal"]
    accepted_signals = [payload for payload in signals if payload.get("accepted")]
    rejected_reasons = Counter(str(payload.get("reason")) for payload in signals if not payload.get("accepted"))
    position_opens = [payload for event_type, payload in payloads if event_type == "position_open"]
    position_closes = [payload for event_type, payload in payloads if event_type == "position_close"]
    market_payloads = [payload for event_type, payload in payloads if event_type == "market_window"]
    latest_markets = {str(payload.get("slug")): payload for payload in market_payloads}
    capture_end_ts = last_ts / 1000.0
    current_markets = [
        payload
        for payload in latest_markets.values()
        if _float_or_zero(payload.get("start_ts")) <= capture_end_ts < _float_or_zero(payload.get("end_ts"))
    ]
    ready_markets = [
        payload
        for payload in current_markets
        if payload.get("active")
        and not payload.get("closed")
        and payload.get("accepting_orders")
        and payload.get("price_to_beat") is not None
    ]
    replay_result = replay(events_path, config_path or Path("config.toml"))
    db = read_db_summary(db_path)

    lines = [
        "# Paper Run Report",
        "",
        f"- telemetry: `{events_path}`",
        f"- database: `{db_path}`",
        f"- generated_at_utc: `{datetime.now(timezone.utc).isoformat()}`",
        f"- capture_start_utc: `{_iso(first_ts)}`",
        f"- capture_end_utc: `{_iso(last_ts)}`",
        f"- duration_seconds: `{duration_s:.1f}`",
        "",
        "## Executive Summary",
        "",
        _summary_sentence(counts, accepted_signals, position_opens, position_closes, replay_result.ok),
        "",
        "## Ingestion",
        "",
        "| source | count | p50 ms | p90 ms | p99 ms | max ms |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
        _percentile_row("chainlink_feed", chainlink_lags),
        _percentile_row("chainlink_feed_under_5s", chainlink_lags_under_5s),
        _percentile_row("clob_quote", clob_lags),
        "",
        "## Market Readiness",
        "",
        f"- unique latest markets: `{len(latest_markets)}`",
        f"- current-window markets: `{len(current_markets)}`",
        f"- current active accepting markets with price_to_beat: `{len(ready_markets)}`",
        f"- price_to_beat captures: `{counts.get('price_to_beat', 0)}`",
        "",
        "## Signals And Paper Trades",
        "",
        f"- recorded signals: `{len(signals)}`",
        f"- accepted signals: `{len(accepted_signals)}`",
        f"- position opens: `{len(position_opens)}`",
        f"- position closes: `{len(position_closes)}`",
        f"- sqlite trades: `{db['trade_count']}`",
        f"- sqlite open trades: `{db['open_trades']}`",
        f"- sqlite closed trades: `{db['closed_trades']}`",
        f"- realized paper pnl: `{db['realized_pnl']:.4f}`",
        f"- worst single closed trade: `{db['worst_trade_pnl']:.4f}`",
        f"- max closed-trade drawdown: `{db['max_drawdown']:.4f}`",
        "",
        "## Rejected Signal Reasons",
        "",
    ]
    if rejected_reasons:
        for reason, count in rejected_reasons.most_common():
            lines.append(f"- `{reason}`: `{count}`")
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            "## Replay Check",
            "",
            f"- ok: `{str(replay_result.ok).lower()}`",
            f"- recorded_signals: `{replay_result.recorded_signals}`",
            f"- replayed_signals: `{replay_result.replayed_signals}`",
            f"- matched_signals: `{replay_result.matched_signals}`",
            f"- mismatched_signals: `{replay_result.mismatched_signals}`",
            f"- unverifiable_signals: `{replay_result.unverifiable_signals}`",
            f"- settlement_matches: `{replay_result.settlement_matches}`",
            f"- settlement_mismatches: `{replay_result.settlement_mismatches}`",
            "",
            "## Client Readiness Notes",
            "",
            "- Paper mode ran without live order submission.",
            "- Live execution remains blocked by config gates and credential preflight.",
            "- `--executor-preflight` should be run with real Polymarket credentials before any live order test.",
            "- A no-trade paper run is still useful for validating feeds, market discovery, telemetry, and replay plumbing.",
        ]
    )
    if not position_opens:
        lines.append(
            "- No simulated fills occurred under the default risk settings, so live-like settlement was not exercised in this capture."
        )
    for note in replay_result.notes or []:
        lines.append(f"- Replay note: {note}")

    report = "\n".join(lines) + "\n"
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report, encoding="utf-8")
    return report


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


def read_db_summary(path: Path) -> dict[str, float | int]:
    if not path.exists():
        return {
            "trade_count": 0,
            "open_trades": 0,
            "closed_trades": 0,
            "realized_pnl": 0.0,
            "worst_trade_pnl": 0.0,
            "max_drawdown": 0.0,
        }
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        trade_count = _scalar(conn, "SELECT COUNT(*) FROM trades")
        open_trades = _scalar(conn, "SELECT COUNT(*) FROM trades WHERE status = 'open'")
        closed_rows = conn.execute(
            """
            SELECT closed_at_ms, realized_pnl
            FROM trades
            WHERE status = 'closed'
            ORDER BY closed_at_ms ASC
            """
        ).fetchall()
    finally:
        conn.close()
    pnls = [float(row["realized_pnl"] or 0.0) for row in closed_rows]
    return {
        "trade_count": int(trade_count),
        "open_trades": int(open_trades),
        "closed_trades": len(pnls),
        "realized_pnl": sum(pnls),
        "worst_trade_pnl": min(pnls) if pnls else 0.0,
        "max_drawdown": _max_drawdown(pnls),
    }


def _scalar(conn: sqlite3.Connection, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    return int(row[0] if row else 0)


def _max_drawdown(pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


def _summary_sentence(
    counts: Counter[str],
    accepted_signals: list[dict[str, Any]],
    position_opens: list[dict[str, Any]],
    position_closes: list[dict[str, Any]],
    replay_ok: bool,
) -> str:
    return (
        f"The capture recorded {counts.get('chainlink_tick', 0)} Chainlink ticks, "
        f"{counts.get('market_window', 0)} market window observations, "
        f"{counts.get('clob_quote_update', 0)} CLOB quote updates, "
        f"{len(accepted_signals)} accepted signals, {len(position_opens)} paper opens, "
        f"and {len(position_closes)} paper closes. Replay status: {str(replay_ok).lower()}."
    )


def _percentile_row(name: str, values: list[float]) -> str:
    return (
        f"| {name} | {len(values)} | {_fmt(percentile(values, 0.50))} | "
        f"{_fmt(percentile(values, 0.90))} | {_fmt(percentile(values, 0.99))} | "
        f"{_fmt(max(values) if values else None)} |"
    )


def _fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.3f}"


def _float_or_zero(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, timezone.utc).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a client-facing paper run report")
    parser.add_argument("--events", type=Path, default=Path("data/events.jsonl"))
    parser.add_argument("--db", type=Path, default=Path("data/paper_run.sqlite3"))
    parser.add_argument("--config", type=Path, default=Path("config.toml"))
    parser.add_argument("--output", type=Path, default=Path("docs/PAPER_RUN_REPORT.md"))
    args = parser.parse_args()
    print(generate(args.events, args.db, args.output, args.config))


if __name__ == "__main__":
    main()
