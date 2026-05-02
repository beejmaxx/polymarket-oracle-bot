from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from .models import MarketWindow, Position, PriceTick, Signal


class Storage:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._init_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS price_ticks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    price REAL NOT NULL,
                    feed_ts_ms INTEGER NOT NULL,
                    received_ts_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_price_ticks_asset_ts
                    ON price_ticks(asset, feed_ts_ms);

                CREATE TABLE IF NOT EXISTS markets (
                    asset TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    start_ts INTEGER NOT NULL,
                    end_ts INTEGER NOT NULL,
                    up_token_id TEXT NOT NULL,
                    down_token_id TEXT NOT NULL,
                    tick_size REAL NOT NULL,
                    min_order_size REAL NOT NULL,
                    price_to_beat REAL,
                    active INTEGER NOT NULL,
                    closed INTEGER NOT NULL,
                    accepting_orders INTEGER NOT NULL,
                    raw_json TEXT NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (asset, slug)
                );

                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at_ms INTEGER NOT NULL,
                    asset TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    price_to_beat REAL NOT NULL,
                    observed_price REAL NOT NULL,
                    distance_bps REAL NOT NULL,
                    estimated_prob REAL NOT NULL,
                    ask_price REAL NOT NULL,
                    edge REAL NOT NULL,
                    reason TEXT NOT NULL,
                    accepted INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_signals_created
                    ON signals(created_at_ms);

                CREATE TABLE IF NOT EXISTS trades (
                    trade_id TEXT PRIMARY KEY,
                    opened_at_ms INTEGER NOT NULL,
                    closed_at_ms INTEGER,
                    mode TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    start_ts INTEGER,
                    end_ts INTEGER,
                    shares REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    cost_usd REAL NOT NULL,
                    order_id TEXT,
                    status TEXT NOT NULL,
                    price_to_beat REAL NOT NULL,
                    entry_oracle_price REAL NOT NULL,
                    exit_oracle_price REAL,
                    winning_outcome TEXT,
                    realized_pnl REAL,
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_trades_status
                    ON trades(status);

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at_ms INTEGER NOT NULL,
                    level TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    message TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                );
                """
            )
            self._ensure_column_locked("trades", "start_ts", "INTEGER")
            self._ensure_column_locked("trades", "end_ts", "INTEGER")

    def _ensure_column_locked(self, table: str, column: str, declaration: str) -> None:
        rows = self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        if any(str(row["name"]) == column for row in rows):
            return
        self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")

    def insert_tick(self, tick: PriceTick) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO price_ticks(asset, symbol, price, feed_ts_ms, received_ts_ms)
                VALUES (?, ?, ?, ?, ?)
                """,
                (tick.asset, tick.symbol, tick.price, tick.feed_ts_ms, tick.received_ts_ms),
            )
            self._conn.commit()

    def upsert_market(self, market: MarketWindow) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO markets(
                    asset, slug, condition_id, start_ts, end_ts, up_token_id,
                    down_token_id, tick_size, min_order_size, price_to_beat,
                    active, closed, accepting_orders, raw_json, updated_at_ms
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset, slug) DO UPDATE SET
                    condition_id=excluded.condition_id,
                    start_ts=excluded.start_ts,
                    end_ts=excluded.end_ts,
                    up_token_id=excluded.up_token_id,
                    down_token_id=excluded.down_token_id,
                    tick_size=excluded.tick_size,
                    min_order_size=excluded.min_order_size,
                    price_to_beat=COALESCE(excluded.price_to_beat, markets.price_to_beat),
                    active=excluded.active,
                    closed=excluded.closed,
                    accepting_orders=excluded.accepting_orders,
                    raw_json=excluded.raw_json,
                    updated_at_ms=excluded.updated_at_ms
                """,
                (
                    market.asset,
                    market.slug,
                    market.condition_id,
                    market.start_ts,
                    market.end_ts,
                    market.tokens["Up"],
                    market.tokens["Down"],
                    market.tick_size,
                    market.min_order_size,
                    market.price_to_beat,
                    int(market.active),
                    int(market.closed),
                    int(market.accepting_orders),
                    json.dumps(market.raw, separators=(",", ":"), sort_keys=True),
                    int(time.time() * 1000),
                ),
            )
            self._conn.commit()

    def insert_signal(self, signal: Signal, accepted: bool) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO signals(
                    created_at_ms, asset, slug, condition_id, outcome, token_id,
                    price_to_beat, observed_price, distance_bps, estimated_prob,
                    ask_price, edge, reason, accepted
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.created_at_ms,
                    signal.asset,
                    signal.slug,
                    signal.condition_id,
                    signal.outcome,
                    signal.token_id,
                    signal.price_to_beat,
                    signal.observed_price,
                    signal.distance_bps,
                    signal.estimated_prob,
                    signal.ask_price,
                    signal.edge,
                    signal.reason,
                    int(accepted),
                ),
            )
            self._conn.commit()

    def open_trade(self, position: Position) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO trades(
                    trade_id, opened_at_ms, mode, asset, slug, condition_id,
                    outcome, token_id, start_ts, end_ts, shares, entry_price, cost_usd, order_id,
                    status, price_to_beat, entry_oracle_price
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    position.trade_id,
                    position.opened_at_ms,
                    position.mode,
                    position.asset,
                    position.slug,
                    position.condition_id,
                    position.outcome,
                    position.token_id,
                    position.start_ts,
                    position.end_ts,
                    position.shares,
                    position.entry_price,
                    position.cost_usd,
                    position.order_id,
                    position.status,
                    position.price_to_beat,
                    position.entry_oracle_price,
                ),
            )
            self._conn.commit()

    def load_open_positions(self) -> list[Position]:
        with self._lock:
            rows = list(
                self._conn.execute(
                    """
                    SELECT *
                    FROM trades
                    WHERE status = 'open'
                    ORDER BY opened_at_ms ASC
                    """
                ).fetchall()
            )
        positions: list[Position] = []
        for row in rows:
            start_ts = row["start_ts"] if "start_ts" in row.keys() else None
            end_ts = row["end_ts"] if "end_ts" in row.keys() else None
            if start_ts is None or end_ts is None:
                start_ts, end_ts = _infer_window_from_slug(str(row["slug"]))
            if start_ts is None or end_ts is None:
                continue
            positions.append(
                Position(
                    trade_id=str(row["trade_id"]),
                    mode=row["mode"],
                    asset=str(row["asset"]),
                    slug=str(row["slug"]),
                    condition_id=str(row["condition_id"]),
                    outcome=row["outcome"],
                    token_id=str(row["token_id"]),
                    start_ts=int(start_ts),
                    end_ts=int(end_ts),
                    shares=float(row["shares"]),
                    entry_price=float(row["entry_price"]),
                    cost_usd=float(row["cost_usd"]),
                    order_id=row["order_id"],
                    price_to_beat=float(row["price_to_beat"]),
                    entry_oracle_price=float(row["entry_oracle_price"]),
                    opened_at_ms=int(row["opened_at_ms"]),
                    status=str(row["status"]),
                )
            )
        return positions

    def close_trade(
        self,
        trade_id: str,
        closed_at_ms: int,
        exit_oracle_price: float,
        winning_outcome: str,
        realized_pnl: float,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE trades
                SET closed_at_ms = ?,
                    status = 'closed',
                    exit_oracle_price = ?,
                    winning_outcome = ?,
                    realized_pnl = ?,
                    metadata_json = ?
                WHERE trade_id = ?
                """,
                (
                    closed_at_ms,
                    exit_oracle_price,
                    winning_outcome,
                    realized_pnl,
                    json.dumps(metadata or {}, separators=(",", ":"), sort_keys=True),
                    trade_id,
                ),
            )
            self._conn.commit()

    def log_event(self, level: str, event_type: str, message: str, metadata: dict[str, Any] | None = None) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO events(created_at_ms, level, event_type, message, metadata_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    int(time.time() * 1000),
                    level,
                    event_type,
                    message,
                    json.dumps(metadata or {}, separators=(",", ":"), sort_keys=True),
                ),
            )
            self._conn.commit()

    def realized_pnl_since(self, start_ms: int) -> float:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COALESCE(SUM(realized_pnl), 0.0) AS pnl
                FROM trades
                WHERE status = 'closed' AND closed_at_ms >= ?
                """,
                (start_ms,),
            ).fetchone()
        return float(row["pnl"] if row else 0.0)

    def trades_opened_since(self, start_ms: int) -> int:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM trades
                WHERE opened_at_ms >= ?
                """,
                (start_ms,),
            ).fetchone()
        return int(row["count"] if row else 0)

    def recent_trades(self, limit: int = 10) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    """
                    SELECT *
                    FROM trades
                    ORDER BY opened_at_ms DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            )


def _infer_window_from_slug(slug: str) -> tuple[int | None, int | None]:
    try:
        start_ts = int(slug.rsplit("-", 1)[-1])
    except (ValueError, IndexError):
        return None, None
    if "-5m-" in slug:
        seconds = 5 * 60
    elif "-15m-" in slug:
        seconds = 15 * 60
    elif "-1h-" in slug:
        seconds = 60 * 60
    else:
        seconds = 15 * 60
    return start_ts, start_ts + seconds
