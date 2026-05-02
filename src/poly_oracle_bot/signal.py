from __future__ import annotations

import math
import time

from .config import RiskConfig
from .models import MarketWindow, Outcome, PriceTick, Quote, Signal


class SignalEngine:
    def __init__(self, risk: RiskConfig) -> None:
        self.risk = risk

    def estimate_probability(self, distance_bps: float) -> float:
        excess = max(0.0, abs(distance_bps) - self.risk.min_signal_distance_bps)
        if excess <= 0.0:
            return 0.5
        scale = max(self.risk.probability_scale_bps, 1e-9)
        span = max(self.risk.probability_cap - 0.5, 0.0)
        return min(self.risk.probability_cap, 0.5 + span * (1.0 - math.exp(-excess / scale)))

    def evaluate(
        self,
        market: MarketWindow,
        tick: PriceTick,
        quote: Quote,
        now_ms: int | None = None,
    ) -> Signal | None:
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        now_ts = now_ms / 1000.0
        if market.price_to_beat is None:
            return None
        if market.closed or not market.active or not market.accepting_orders:
            return None
        if now_ts < market.start_ts + self.risk.min_elapsed_seconds:
            return None
        if now_ts > market.end_ts - self.risk.max_seconds_to_expiry:
            return None
        if quote.best_ask is None:
            return None

        outcome: Outcome = "Up" if tick.price >= market.price_to_beat else "Down"
        token_id = market.token_for(outcome)
        if quote.token_id != token_id:
            return None

        distance_bps = ((tick.price / market.price_to_beat) - 1.0) * 10_000.0
        signed_distance = distance_bps if outcome == "Up" else -distance_bps
        ask = quote.best_ask
        if now_ms - tick.received_ts_ms > self.risk.max_tick_age_ms:
            return self._rejected_signal(
                market,
                tick,
                outcome,
                token_id,
                signed_distance,
                ask,
                -1.0,
                "oracle tick stale",
                now_ms,
            )
        if tick.received_ts_ms - tick.feed_ts_ms > self.risk.max_tick_feed_lag_ms:
            return self._rejected_signal(
                market,
                tick,
                outcome,
                token_id,
                signed_distance,
                ask,
                -1.0,
                "oracle feed lag above limit",
                now_ms,
            )
        if signed_distance < self.risk.min_signal_distance_bps:
            return self._rejected_signal(
                market,
                tick,
                outcome,
                token_id,
                signed_distance,
                ask,
                -1.0,
                "distance below threshold",
                now_ms,
            )
        if ask < self.risk.min_entry_price or ask > self.risk.max_entry_price:
            return self._rejected_signal(
                market,
                tick,
                outcome,
                token_id,
                signed_distance,
                ask,
                -1.0,
                "quote outside configured price band",
                now_ms,
            )

        probability = self.estimate_probability(signed_distance)
        edge = probability - ask
        if edge < self.risk.min_probability_edge:
            return self._rejected_signal(
                market,
                tick,
                outcome,
                token_id,
                signed_distance,
                ask,
                edge,
                "edge below threshold",
                now_ms,
            )

        return Signal(
            asset=market.asset,
            slug=market.slug,
            condition_id=market.condition_id,
            start_ts=market.start_ts,
            end_ts=market.end_ts,
            outcome=outcome,
            token_id=token_id,
            price_to_beat=market.price_to_beat,
            observed_price=tick.price,
            distance_bps=signed_distance,
            estimated_prob=probability,
            ask_price=ask,
            edge=edge,
            reason="accepted",
            created_at_ms=now_ms,
        )

    def _rejected_signal(
        self,
        market: MarketWindow,
        tick: PriceTick,
        outcome: Outcome,
        token_id: str,
        signed_distance: float,
        ask: float,
        edge: float,
        reason: str,
        now_ms: int,
    ) -> Signal:
        return Signal(
            asset=market.asset,
            slug=market.slug,
            condition_id=market.condition_id,
            start_ts=market.start_ts,
            end_ts=market.end_ts,
            outcome=outcome,
            token_id=token_id,
            price_to_beat=market.price_to_beat or 0.0,
            observed_price=tick.price,
            distance_bps=signed_distance,
            estimated_prob=self.estimate_probability(signed_distance),
            ask_price=ask,
            edge=edge,
            reason=reason,
            created_at_ms=now_ms,
        )
