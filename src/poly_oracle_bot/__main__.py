from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from .bot import Bot
from .config import load_config
from .preflight import format_results, run_preflight


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket 15m oracle bot")
    parser.add_argument("--config", type=Path, default=Path("config.toml"))
    parser.add_argument("--db", type=Path, default=Path("data/bot.sqlite3"))
    parser.add_argument("--no-dashboard", action="store_true")
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument(
        "--executor-preflight",
        action="store_true",
        help="run live executor import/client/signing dry-run without submitting orders",
    )
    parser.add_argument("--preflight-timeout", type=float, default=10.0)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    cfg = load_config(args.config if args.config.exists() else None)
    if args.no_dashboard:
        cfg.trading.dashboard_interval_seconds = 0.0
    if args.preflight or args.executor_preflight:
        results = asyncio.run(
            run_preflight(
                cfg,
                args.db,
                args.preflight_timeout,
                executor_preflight=args.executor_preflight,
            )
        )
        print(format_results(results))
        raise SystemExit(0 if all(result.ok for result in results) else 1)
    try:
        asyncio.run(Bot(cfg, args.db).run())
    except KeyboardInterrupt:
        print("stopped")


if __name__ == "__main__":
    main()
