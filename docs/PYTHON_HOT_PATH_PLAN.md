# Python Hot-Path Plan

Python is the primary runtime path for now. The goal is to keep the system simple
while making the hot path measurable and disciplined.

## Current Direction

- Keep asyncio as the core concurrency model.
- Keep paper/live safety gates.
- Keep DB/file work behind bounded async queues.
- Use JSONL telemetry to measure real feed and execution latency before deeper
  optimization.
- Keep the Rust skeleton as a reference path, not the active implementation.

## Immediate Work

- Record normalized Chainlink ticks, CLOB quote updates, Gamma markets, signals,
  order responses, and position lifecycle events.
- Record timing spans for signal evaluation, sizing, order submission, and total
  signal-to-position latency.
- Use recorded JSONL to build replay/calibration tooling.
- Keep SQLite writes off the feed/signal callbacks via the async storage writer.

## Next Optimization Pass

- Replace sync `py-clob-client` hot-path calls with a direct async REST executor
  if live measurements show the thread hop or client overhead is material.
- Add `uvloop` as an optional production runtime on Linux.
- Add auth/balance/order preflight before any live mode test.
