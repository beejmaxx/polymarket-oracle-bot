# Paper Run Report

- telemetry: `data/events.jsonl`
- database: `data/paper_run.sqlite3`
- generated_at_utc: `2026-05-02T18:32:40.669224+00:00`
- capture_start_utc: `2026-05-02T18:14:59.346000+00:00`
- capture_end_utc: `2026-05-02T18:32:12.209000+00:00`
- duration_seconds: `1032.9`

## Executive Summary

The capture recorded 4082 Chainlink ticks, 4032 market window observations, 95959 CLOB quote updates, 0 accepted signals, 0 paper opens, and 0 paper closes. Replay status: true.

## Ingestion

| source | count | p50 ms | p90 ms | p99 ms | max ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| chainlink_feed | 4082 | 1207.000 | 1585.900 | 1977.190 | 2637.000 |
| chainlink_feed_under_5s | 4082 | 1207.000 | 1585.900 | 1977.190 | 2637.000 |
| clob_quote | 95959 | 25.000 | 49.000 | 123.000 | 11274.000 |

## Market Readiness

- unique latest markets: `24`
- current-window markets: `4`
- current active accepting markets with price_to_beat: `4`
- price_to_beat captures: `8`

## Signals And Paper Trades

- recorded signals: `1456`
- accepted signals: `0`
- position opens: `0`
- position closes: `0`
- sqlite trades: `0`
- sqlite open trades: `0`
- sqlite closed trades: `0`
- realized paper pnl: `0.0000`
- worst single closed trade: `0.0000`
- max closed-trade drawdown: `0.0000`

## Rejected Signal Reasons

- `quote outside configured price band`: `1133`
- `edge below threshold`: `323`

## Replay Check

- ok: `true`
- recorded_signals: `1456`
- replayed_signals: `1456`
- matched_signals: `1456`
- mismatched_signals: `0`
- unverifiable_signals: `0`
- settlement_matches: `0`
- settlement_mismatches: `0`

## Client Readiness Notes

- Paper mode ran without live order submission.
- Live execution remains blocked by config gates and credential preflight.
- `--executor-preflight` should be run with real Polymarket credentials before any live order test.
- A no-trade paper run is still useful for validating feeds, market discovery, telemetry, and replay plumbing.
- No simulated fills occurred under the default risk settings, so live-like settlement was not exercised in this capture.
