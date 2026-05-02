#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = (len(ordered) - 1) * pct
    low = int(idx)
    high = min(low + 1, len(ordered) - 1)
    weight = idx - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def summarize(path: Path) -> str:
    events = read_events(path)
    counts = Counter(event.get("event_type") for event in events)
    payloads = [(event.get("event_type"), event.get("payload") or {}) for event in events]

    chainlink_lags = [
        float(payload["feed_lag_ms"])
        for event_type, payload in payloads
        if event_type == "chainlink_tick" and payload.get("feed_lag_ms") is not None
    ]
    clob_lags = [
        float(payload["lag_ms"])
        for event_type, payload in payloads
        if event_type == "clob_quote_update" and payload.get("lag_ms") is not None
    ]
    chainlink_lags_under_5s = [lag for lag in chainlink_lags if lag < 5_000.0]

    signal_reasons = Counter()
    signal_accepts = Counter()
    latency_by_span: dict[str, list[float]] = defaultdict(list)
    signal_asks = []
    signal_edges = []
    for event_type, payload in payloads:
        if event_type != "signal":
            continue
        signal_reasons[str(payload.get("reason"))] += 1
        signal_accepts["accepted" if payload.get("accepted") else "rejected"] += 1
        if payload.get("ask_price") is not None:
            signal_asks.append(float(payload["ask_price"]))
        if payload.get("edge") is not None:
            signal_edges.append(float(payload["edge"]))
        for span, value in (payload.get("latency") or {}).items():
            latency_by_span[str(span)].append(float(value))

    latest_markets: dict[str, dict[str, Any]] = {}
    for event_type, payload in payloads:
        if event_type == "market_window":
            latest_markets[str(payload.get("slug"))] = payload
    latest_ts = max((int(event.get("ts_ms") or 0) for event in events), default=0) / 1000.0
    current_markets = [
        market
        for market in latest_markets.values()
        if _float_or_zero(market.get("start_ts")) <= latest_ts < _float_or_zero(market.get("end_ts"))
    ]
    missing_price_to_beat = [
        market
        for market in current_markets
        if market.get("active") and not market.get("closed") and market.get("price_to_beat") is None
    ]

    lines = [
        f"# Telemetry Summary: {path}",
        "",
        f"events: {len(events)}",
        "",
        "## Event Counts",
    ]
    for event_type, count in counts.most_common():
        lines.append(f"- {event_type}: {count}")
    lines.extend(
        [
            "",
            "## Lag Percentiles (ms)",
            "| source | count | p50 | p90 | p99 | max |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
            percentile_row("chainlink_feed", chainlink_lags),
            percentile_row("chainlink_feed_under_5s", chainlink_lags_under_5s),
            percentile_row("clob_quote", clob_lags),
            "",
            "## Signals",
            f"- accepted: {signal_accepts.get('accepted', 0)}",
            f"- rejected: {signal_accepts.get('rejected', 0)}",
        ]
    )
    for reason, count in signal_reasons.most_common():
        lines.append(f"- reason `{reason}`: {count}")

    lines.extend(
        [
            "",
            "## Signal Price/Edge",
            percentile_line("ask_price", signal_asks),
            percentile_line("edge", signal_edges),
            "",
            "## Signal Latency Percentiles (ms)",
        ]
    )
    if latency_by_span:
        lines.extend(["| span | count | p50 | p90 | p99 | max |", "| --- | ---: | ---: | ---: | ---: | ---: |"])
        for span in sorted(latency_by_span):
            lines.append(percentile_row(span, latency_by_span[span]))
    else:
        lines.append("n/a")

    lines.extend(
        [
            "",
            "## Market Readiness",
            f"- latest markets: {len(latest_markets)}",
            f"- current-window markets: {len(current_markets)}",
            f"- current active open markets missing price_to_beat: {len(missing_price_to_beat)}",
        ]
    )
    for market in missing_price_to_beat[:20]:
        lines.append(f"  - {market.get('asset')} {market.get('slug')}")

    return "\n".join(lines)


def read_events(path: Path) -> list[dict[str, Any]]:
    events = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise SystemExit(f"{path}:{line_no}: invalid JSON: {exc}") from exc
    return events


def percentile_row(name: str, values: list[float]) -> str:
    return (
        f"| {name} | {len(values)} | {fmt(percentile(values, 0.50))} | "
        f"{fmt(percentile(values, 0.90))} | {fmt(percentile(values, 0.99))} | "
        f"{fmt(max(values) if values else None)} |"
    )


def percentile_line(name: str, values: list[float]) -> str:
    return (
        f"- {name}: count={len(values)} p50={fmt(percentile(values, 0.50))} "
        f"p90={fmt(percentile(values, 0.90))} p99={fmt(percentile(values, 0.99))} "
        f"max={fmt(max(values) if values else None)}"
    )


def fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.3f}"


def _float_or_zero(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize bot JSONL telemetry")
    parser.add_argument("path", type=Path, nargs="?", default=Path("data/events.jsonl"))
    args = parser.parse_args()
    print(summarize(args.path))


if __name__ == "__main__":
    main()
