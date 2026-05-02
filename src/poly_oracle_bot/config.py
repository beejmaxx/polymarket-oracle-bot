from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class AssetConfig:
    symbol: str
    slug_prefix: str
    chainlink_symbol: str
    enabled: bool = True


@dataclass(slots=True)
class TradingConfig:
    mode: str = "paper"
    timeframe: str = "15m"
    market_poll_interval_seconds: float = 4.0
    signal_interval_seconds: float = 0.25
    settlement_poll_interval_seconds: float = 5.0
    dashboard_interval_seconds: float = 1.0
    lookback_windows: int = 1
    lookahead_windows: int = 2
    opening_capture_grace_seconds: int = 20
    settlement_grace_seconds: int = 2
    live_enable_orders: bool = False
    live_confirm_real_money: bool = False
    live_confirm_polymarket_terms: bool = False

    def validate(self) -> None:
        if self.mode not in {"paper", "live"}:
            raise ValueError("trading.mode must be 'paper' or 'live'")
        if self.mode == "live":
            missing = []
            if not self.live_enable_orders:
                missing.append("trading.live_enable_orders")
            if not self.live_confirm_real_money:
                missing.append("trading.live_confirm_real_money")
            if not self.live_confirm_polymarket_terms:
                missing.append("trading.live_confirm_polymarket_terms")
            if missing:
                raise ValueError(
                    "live mode refused; set all explicit live gates: "
                    + ", ".join(missing)
                )


@dataclass(slots=True)
class RiskConfig:
    bankroll_usd: float = 1000.0
    kelly_fraction: float = 0.25
    max_position_usd: float = 25.0
    max_position_fraction: float = 0.03
    min_order_usd: float = 5.0
    max_daily_drawdown_usd: float = 75.0
    min_signal_distance_bps: float = 4.0
    probability_scale_bps: float = 12.0
    probability_cap: float = 0.88
    min_probability_edge: float = 0.04
    min_entry_price: float = 0.05
    max_entry_price: float = 0.92
    min_elapsed_seconds: float = 1.0
    max_seconds_to_expiry: float = 8.0
    max_open_exposure_usd: float = 100.0
    max_trades_per_hour: int = 12
    max_tick_age_ms: int = 2500
    max_tick_feed_lag_ms: int = 10_000
    risk_day_timezone: str = "America/New_York"


@dataclass(slots=True)
class PolymarketConfig:
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    clob_base_url: str = "https://clob.polymarket.com"
    market_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    rtds_ws_url: str = "wss://ws-live-data.polymarket.com"
    chain_id: int = 137
    market_ws_ping_payload: str = "PING"


@dataclass(slots=True)
class TelegramConfig:
    enabled: bool = False
    bot_token_env: str = "TELEGRAM_BOT_TOKEN"
    chat_id_env: str = "TELEGRAM_CHAT_ID"
    heartbeat_interval_seconds: int = 3600

    @property
    def bot_token(self) -> str | None:
        return os.getenv(self.bot_token_env)

    @property
    def chat_id(self) -> str | None:
        return os.getenv(self.chat_id_env)


@dataclass(slots=True)
class TelemetryConfig:
    enabled: bool = True
    path: str = "data/events.jsonl"
    queue_max: int = 20000
    storage_queue_max: int = 20000
    batch_size: int = 100
    flush_interval_seconds: float = 0.25
    record_ticks: bool = True
    record_orderbook: bool = True
    record_rejected_signals: bool = True


@dataclass(slots=True)
class AppConfig:
    trading: TradingConfig = field(default_factory=TradingConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    polymarket: PolymarketConfig = field(default_factory=PolymarketConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)
    assets: list[AssetConfig] = field(
        default_factory=lambda: [
            AssetConfig("BTC", "btc", "btc/usd"),
            AssetConfig("ETH", "eth", "eth/usd"),
            AssetConfig("SOL", "sol", "sol/usd"),
            AssetConfig("XRP", "xrp", "xrp/usd"),
        ]
    )

    @property
    def enabled_assets(self) -> list[AssetConfig]:
        return [asset for asset in self.assets if asset.enabled]

    def validate(self) -> None:
        self.trading.validate()
        if not self.enabled_assets:
            raise ValueError("at least one asset must be enabled")


def _section(data: dict[str, Any], name: str) -> dict[str, Any]:
    value = data.get(name, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a table")
    return value


def _build_dataclass(cls: type[Any], values: dict[str, Any]) -> Any:
    valid = cls.__dataclass_fields__.keys()  # type: ignore[attr-defined]
    return cls(**{key: value for key, value in values.items() if key in valid})


def load_config(path: Path | None = None) -> AppConfig:
    raw: dict[str, Any] = {}
    if path is not None:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)

    cfg = AppConfig(
        trading=_build_dataclass(TradingConfig, _section(raw, "trading")),
        risk=_build_dataclass(RiskConfig, _section(raw, "risk")),
        polymarket=_build_dataclass(PolymarketConfig, _section(raw, "polymarket")),
        telegram=_build_dataclass(TelegramConfig, _section(raw, "telegram")),
        telemetry=_build_dataclass(TelemetryConfig, _section(raw, "telemetry")),
        assets=[
            _build_dataclass(AssetConfig, asset)
            for asset in raw.get("assets", [])
            if isinstance(asset, dict)
        ]
        or AppConfig().assets,
    )
    cfg.validate()
    return cfg
