# Runbook

This runbook is for paper validation and controlled live-readiness checks.
Live trading stays blocked unless config gates and credential checks are both
explicitly satisfied.

## Install

```bash
cd ~/polymarket-oracle-bot
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -e '.[live]'
```

## Configure

```bash
cp config.example.toml config.toml
cp .env.example .env
```

For paper mode, leave `trading.mode = "paper"` and keep all live gates false.

For live preflight, export the values from `.env` into the shell before running
checks. Do not switch `trading.mode` to `live` until paper reports and executor
preflight have been reviewed.

## Preflight

Basic feed/config preflight:

```bash
poly-oracle-bot --config config.toml --db data/preflight.sqlite3 --preflight --preflight-timeout 10
```

No-submit executor preflight:

```bash
poly-oracle-bot --config config.toml --db data/preflight.sqlite3 --executor-preflight --preflight-timeout 10
```

The executor preflight may fetch balance/allowance and market metadata, and it
may sign a local payload. It must not post or submit an order.

## Paper Run

```bash
poly-oracle-bot --config config.toml --db data/paper_run.sqlite3 --no-dashboard
```

Stop with `Ctrl-C` after a useful capture window. For the 15 minute markets, a
minimum useful paper capture is 30-60 minutes so at least two full windows can
settle.

## Analyze

```bash
scripts/analyze_telemetry.py data/events.jsonl
scripts/replay_telemetry.py --config config.toml data/events.jsonl
scripts/generate_paper_report.py \
  --config config.toml \
  --events data/events.jsonl \
  --db data/paper_run.sqlite3 \
  --output docs/PAPER_RUN_REPORT.md
```

Review `docs/PAPER_RUN_REPORT.md` before making live changes.

The Chainlink RTDS client subscribes to all Chainlink crypto symbols and filters
locally. This is intentional: in local testing, per-symbol Chainlink
subscriptions returned historical snapshots but did not stream live updates
reliably.

## Go-Live Checklist

- Basic preflight passes.
- Executor preflight passes with real Polymarket credentials.
- Paper report has no replay mismatches.
- Paper report includes acceptable feed and quote lag for the target market.
- Open-trade recovery has been tested by restarting paper mode with an open
  position.
- Risk gates are configured: daily drawdown, max position, max open exposure,
  max trades per hour, stale tick age, and max feed lag.
- Live config gates are intentionally set:
  - `trading.mode = "live"`
  - `trading.live_enable_orders = true`
  - `trading.live_confirm_real_money = true`
  - `trading.live_confirm_polymarket_terms = true`
- Start with minimum notional and observe order response, reconciliation, and
  settlement before increasing caps.

## Emergency Stop

Stop the process with `Ctrl-C` or kill the process on the host. The bot will not
recover any new position after it is stopped, but already-open Polymarket orders
and positions remain external exchange state and must be checked manually in the
Polymarket account.
