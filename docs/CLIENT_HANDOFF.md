# Client Handoff

## What This Project Demonstrates

- Async Python ingestion of Chainlink RTDS crypto ticks.
- Polymarket Gamma discovery for 15 minute UP/DOWN markets.
- CLOB market websocket quote ingestion.
- Configurable oracle-lag signal evaluation.
- Kelly-derived sizing with hard caps.
- Paper execution and settlement simulation.
- Live execution gated behind config flags and credential checks.
- SQLite audit trail plus JSONL telemetry for replay and reporting.

## Current Recommendation

Keep Python as the primary implementation until live executor measurements prove
otherwise. The synthetic hot path is already microseconds in Python; current
external feed/orderbook latencies are materially larger. Rust remains a fallback
for a measured hot-path problem, not the next default rewrite.

## Evidence To Provide

- `docs/PAPER_RUN_REPORT.md` generated from a current paper capture.
- `scripts/replay_telemetry.py` output showing zero mismatches.
- `--executor-preflight` output showing credential, balance/allowance,
  signing, and timing checks with no order submission.
- Test output from Python unit tests and Rust workspace tests.

## Known Limits

- Public Polymarket websocket snapshots can be empty for thin or newly opened
  markets; the bot needs quote events before it can evaluate a signal.
- Chainlink RTDS currently uses an all-symbol subscription and filters locally;
  per-symbol Chainlink subscriptions returned historical snapshots but did not
  stream reliably in local testing.
- Live settlement tracking should be observed at minimum size before increasing
  risk caps.
- Profitability is not asserted by this repo. The repo is execution,
  validation, and operational infrastructure.

## Highest-Value Next Work

- Run multi-hour paper captures across volatile market periods.
- Add direct order submit timing once credentials are available.
- Compare VPS regions with the same telemetry report.
- Build a replay corpus of representative winning, losing, rejected, and
  settlement cases.
