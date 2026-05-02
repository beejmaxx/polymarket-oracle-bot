from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from .config import RiskConfig
from .models import MarketWindow, Signal, SizeDecision


class RiskManager:
    def __init__(self, cfg: RiskConfig, realized_pnl_today: float = 0.0) -> None:
        self.cfg = cfg
        self.realized_pnl_today = realized_pnl_today

    @property
    def drawdown_blocked(self) -> bool:
        return self.realized_pnl_today <= -abs(self.cfg.max_daily_drawdown_usd)

    def apply_realized_pnl(self, pnl: float) -> None:
        self.realized_pnl_today += pnl

    def size_for_signal(
        self,
        signal: Signal,
        market: MarketWindow,
        open_exposure_usd: float = 0.0,
        trades_opened_last_hour: int = 0,
    ) -> SizeDecision:
        if self.drawdown_blocked:
            return SizeDecision(0.0, 0.0, 0.0, "daily drawdown kill switch active")
        if self.cfg.max_trades_per_hour > 0 and trades_opened_last_hour >= self.cfg.max_trades_per_hour:
            return SizeDecision(0.0, 0.0, 0.0, "max_trades_per_hour limit active")
        exposure_remaining = self.cfg.max_open_exposure_usd - max(0.0, open_exposure_usd)
        if exposure_remaining <= 0.0:
            return SizeDecision(0.0, 0.0, 0.0, "max_open_exposure_usd limit active")

        price = signal.ask_price
        if price <= 0.0 or price >= 1.0:
            return SizeDecision(0.0, 0.0, 0.0, "invalid binary price")

        kelly = max(0.0, (signal.estimated_prob - price) / (1.0 - price))
        scaled_fraction = kelly * self.cfg.kelly_fraction
        cost = self.cfg.bankroll_usd * scaled_fraction
        cost = min(cost, self.cfg.max_position_usd)
        cost = min(cost, self.cfg.bankroll_usd * self.cfg.max_position_fraction)
        cost = min(cost, exposure_remaining)

        remaining_dd = abs(self.cfg.max_daily_drawdown_usd) + self.realized_pnl_today
        if remaining_dd <= 0.0:
            return SizeDecision(0.0, 0.0, kelly, "daily drawdown budget exhausted")
        cost = min(cost, remaining_dd)

        if cost < self.cfg.min_order_usd:
            return SizeDecision(0.0, cost, kelly, "sized cost below min_order_usd")

        shares = cost / price
        if shares < market.min_order_size:
            return SizeDecision(0.0, cost, kelly, "sized shares below market min_order_size")
        return SizeDecision(shares, cost, kelly, "accepted")


def risk_day_start_ms(now: datetime, timezone_name: str) -> int:
    tz = ZoneInfo(timezone_name)
    local = now.astimezone(tz)
    start = local.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(start.timestamp() * 1000)
