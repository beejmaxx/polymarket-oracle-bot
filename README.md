# Polymarket Oracle Bot

Standalone async bot for Polymarket 15 minute crypto UP/DOWN markets. It uses
Polymarket RTDS Chainlink crypto ticks, Gamma market discovery, the CLOB market
websocket for quotes, SQLite logging, Telegram alerts, paper settlement
simulation, and a gated live executor.

The code defaults to paper mode. Live orders are refused unless `mode = "live"`
and all three live confirmation flags are true.

## Install

```bash
cd ~/polymarket-oracle-bot
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -e .
```

For live execution:

```bash
python3 -m pip install -e '.[live]'
```

## Run

```bash
cp config.example.toml config.toml
poly-oracle-bot --config config.toml --db data/bot.sqlite3
```

Preflight checks config, writable paths, Gamma discovery, RTDS, and CLOB market
websocket access without placing orders:

```bash
poly-oracle-bot --config config.toml --db data/bot.sqlite3 --preflight
```

To also test the live executor import/client/signing path without submitting an
order, install the live extra and provide Polymarket credentials, then run:

```bash
poly-oracle-bot --config config.toml --db data/bot.sqlite3 --executor-preflight
```

After a paper run, summarize telemetry:

```bash
scripts/analyze_telemetry.py data/events.jsonl
scripts/replay_telemetry.py --config config.toml data/events.jsonl
scripts/generate_paper_report.py --config config.toml --events data/events.jsonl --db data/paper_run.sqlite3
```

To compare local Python and Rust signal-loop overhead:

```bash
PYTHONPATH=src python3 scripts/bench_hot_path.py --iterations 200000
cargo run --release -q -p oracle-bot-rs --bin bench_hot_path -- --iterations 200000
```

The dashboard prints prices, active markets, open positions, and recent trades.
SQLite tables record markets, Chainlink ticks, signals, trades, and operational
events.

The Python runtime is the primary implementation path. It also writes optional
off-hot-path JSONL telemetry to `data/events.jsonl`, including normalized ticks,
quote updates, markets, signals, positions, and latency spans around the
signal-to-order path.

## Live Credentials

Live mode expects these environment variables:

```bash
POLYMARKET_PRIVATE_KEY=...
POLYMARKET_API_KEY=...
POLYMARKET_API_SECRET=...
POLYMARKET_API_PASSPHRASE=...
POLYMARKET_FUNDER_ADDRESS=...
POLYMARKET_SIGNATURE_TYPE=1
```

`POLYMARKET_SIGNATURE_TYPE` defaults to `1`, matching Polymarket proxy wallets.
Use the value appropriate for the account.

## Notes

- The bot only arms a window after it captures a Chainlink tick close to the
  window start. If it starts mid-window and cannot determine the opening
  reference price, it waits for the next window.
- Paper settlement pays $1 per winning share and $0 per losing share, using the
  captured Chainlink opening price and the final Chainlink tick after market
  expiry.
- Live settlement tracking first checks Gamma resolution/outcome prices, then
  falls back to local Chainlink settlement only for provisional tracking.
- Risk controls include max daily drawdown, max position notional, max open
  exposure, max trades per hour, stale tick age, and max Chainlink feed lag.
- This is execution infrastructure, not a profitability claim.

Client-facing operational docs live in `docs/RUNBOOK.md` and
`docs/CLIENT_HANDOFF.md`.

## Rust Hot-Path Skeleton

The Rust service currently runs in paper/no-exec mode and shares `config.example.toml` with the Python prototype. It ingests Polymarket RTDS Chainlink ticks, discovers deterministic 15m Gamma markets, subscribes to CLOB market websocket token IDs, maintains an in-memory quote cache, and emits paper signal logs.

```bash
cargo run -p oracle-bot-rs -- --config config.example.toml
```

Validation:

```bash
cargo test --workspace
```

Live order signing/submission is intentionally not implemented in Rust yet. The
current plan is to keep Python as the primary runtime and use the Rust skeleton
only as a reference/backup path unless latency measurements prove Python cannot
meet the execution target.
