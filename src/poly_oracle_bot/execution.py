from __future__ import annotations

import asyncio
import importlib
import os
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
from typing import Any

from .config import AppConfig
from .models import MarketWindow, OrderResult, Signal, SizeDecision

LIVE_ENV_VARS = (
    "POLYMARKET_PRIVATE_KEY",
    "POLYMARKET_API_KEY",
    "POLYMARKET_API_SECRET",
    "POLYMARKET_API_PASSPHRASE",
)


@dataclass(slots=True, frozen=True)
class ExecutorDryRunResult:
    ok: bool
    variant: str | None
    message: str
    timings_ms: dict[str, float]


@dataclass(slots=True, frozen=True)
class _OrderApi:
    variant: str
    market_order_args: Any
    partial_options: Any
    order_type: Any
    buy_side: Any
    balance_allowance_params: Any
    asset_type: Any


class PaperExecutor:
    async def submit(self, signal: Signal, size: SizeDecision, market: MarketWindow) -> OrderResult:
        return OrderResult(
            filled=True,
            order_id=f"paper-{signal.asset}-{signal.start_ts}-{signal.outcome.lower()}",
            fill_price=signal.ask_price,
            shares=size.shares,
            message="paper fill",
            raw=None,
        )


class LiveExecutor:
    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        self._client: Any | None = None
        self._variant: str | None = None

    async def submit(self, signal: Signal, size: SizeDecision, market: MarketWindow) -> OrderResult:
        client, variant = await asyncio.to_thread(self._client_or_create)
        price = _round_buy_limit(signal.ask_price, market.tick_size)
        raw = await asyncio.to_thread(
            self._post_buy_order,
            client,
            variant,
            signal.token_id,
            price,
            size.shares,
            size.cost_usd,
            market,
        )
        filled = _is_immediate_fill(raw)
        return OrderResult(
            filled=filled,
            order_id=_extract_order_id(raw),
            fill_price=price,
            shares=_extract_filled_shares(raw, size.shares),
            message="live FOK matched" if filled else f"live order not matched: {_extract_status(raw)}",
            raw=raw,
        )

    def _client_or_create(self) -> tuple[Any, str]:
        if self._client is not None and self._variant is not None:
            return self._client, self._variant
        try:
            self._client = self._create_v2_client()
            self._variant = "v2"
            return self._client, self._variant
        except ImportError:
            self._client = self._create_v1_client()
            self._variant = "v1"
            return self._client, self._variant

    def _create_v2_client(self) -> Any:
        from py_clob_client_v2 import ClobClient

        creds = {
            "apiKey": _required_env("POLYMARKET_API_KEY"),
            "secret": _required_env("POLYMARKET_API_SECRET"),
            "passphrase": _required_env("POLYMARKET_API_PASSPHRASE"),
        }
        return ClobClient(
            host=self.cfg.polymarket.clob_base_url,
            chain_id=self.cfg.polymarket.chain_id,
            key=_required_env("POLYMARKET_PRIVATE_KEY"),
            creds=creds,
            signature_type=int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "1")),
            funder=os.getenv("POLYMARKET_FUNDER_ADDRESS"),
        )

    def _create_v1_client(self) -> Any:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        creds = ApiCreds(
            api_key=_required_env("POLYMARKET_API_KEY"),
            api_secret=_required_env("POLYMARKET_API_SECRET"),
            api_passphrase=_required_env("POLYMARKET_API_PASSPHRASE"),
        )
        return ClobClient(
            host=self.cfg.polymarket.clob_base_url,
            key=_required_env("POLYMARKET_PRIVATE_KEY"),
            chain_id=self.cfg.polymarket.chain_id,
            creds=creds,
            signature_type=int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "1")),
            funder=os.getenv("POLYMARKET_FUNDER_ADDRESS"),
        )

    def _post_buy_order(
        self,
        client: Any,
        variant: str,
        token_id: str,
        price: float,
        shares: float,
        cost_usd: float,
        market: MarketWindow,
    ) -> Any:
        if variant == "v2":
            from py_clob_client_v2 import MarketOrderArgs, OrderType, PartialCreateOrderOptions
            from py_clob_client_v2.order_builder.constants import BUY

            return client.create_and_post_market_order(
                MarketOrderArgs(token_id=token_id, amount=cost_usd, price=price, side=BUY),
                options=PartialCreateOrderOptions(
                    tick_size=str(market.tick_size),
                    neg_risk=market.neg_risk,
                ),
                order_type=OrderType.FOK,
            )

        from py_clob_client.clob_types import MarketOrderArgs, OrderType, PartialCreateOrderOptions
        from py_clob_client.order_builder.constants import BUY

        signed = client.create_market_order(
            MarketOrderArgs(token_id=token_id, amount=cost_usd, price=price, side=BUY),
            options=PartialCreateOrderOptions(tick_size=str(market.tick_size), neg_risk=market.neg_risk),
        )
        return client.post_order(signed, OrderType.FOK)


def executor_for_config(cfg: AppConfig) -> PaperExecutor | LiveExecutor:
    if cfg.trading.mode == "live":
        return LiveExecutor(cfg)
    return PaperExecutor()


def live_executor_dry_run(cfg: AppConfig, market: MarketWindow) -> ExecutorDryRunResult:
    """Build and sign a live CLOB order without submitting it."""
    timings: dict[str, float] = {}
    total_started = time.perf_counter_ns()

    import_started = time.perf_counter_ns()
    try:
        api = _import_order_api()
    except ImportError as exc:
        timings["import"] = _elapsed_ms(import_started)
        timings["total"] = _elapsed_ms(total_started)
        return ExecutorDryRunResult(False, None, f"py-clob-client unavailable: {exc}", timings)
    timings["import"] = _elapsed_ms(import_started)

    missing = missing_live_env_vars()
    if missing:
        timings["total"] = _elapsed_ms(total_started)
        return ExecutorDryRunResult(
            False,
            api.variant,
            f"variant={api.variant}; missing {', '.join(missing)}; no order submitted",
            timings,
        )

    token_id = market.tokens.get("Up") or next(iter(market.tokens.values()), None)
    if not token_id:
        timings["total"] = _elapsed_ms(total_started)
        return ExecutorDryRunResult(False, api.variant, "market has no token id; no order submitted", timings)

    client_started = time.perf_counter_ns()
    try:
        live = LiveExecutor(cfg)
        client = live._create_v2_client() if api.variant == "v2" else live._create_v1_client()
    except Exception as exc:
        timings["client"] = _elapsed_ms(client_started)
        timings["total"] = _elapsed_ms(total_started)
        return ExecutorDryRunResult(
            False,
            api.variant,
            f"variant={api.variant}; client construction failed: {exc}; no order submitted",
            timings,
        )
    timings["client"] = _elapsed_ms(client_started)

    balance_started = time.perf_counter_ns()
    balance_summary = "balance_check=skipped"
    try:
        balance_summary = _balance_allowance_summary(client, api)
    except Exception as exc:
        timings["balance"] = _elapsed_ms(balance_started)
        timings["total"] = _elapsed_ms(total_started)
        return ExecutorDryRunResult(
            False,
            api.variant,
            f"variant={api.variant}; balance/allowance check failed: {exc}; no order submitted",
            timings,
        )
    timings["balance"] = _elapsed_ms(balance_started)

    args_started = time.perf_counter_ns()
    amount_usd = max(5.0, cfg.risk.min_order_usd, market.min_order_size)
    price = _round_buy_limit(min(0.99, cfg.risk.max_entry_price), market.tick_size)
    order_args = api.market_order_args(
        token_id=token_id,
        amount=amount_usd,
        side=api.buy_side,
        price=price,
        order_type=api.order_type.FOK,
    )
    options = api.partial_options(tick_size=str(market.tick_size), neg_risk=market.neg_risk)
    timings["args"] = _elapsed_ms(args_started)

    sign_started = time.perf_counter_ns()
    try:
        signed = client.create_market_order(order_args, options=options)
    except Exception as exc:
        timings["sign"] = _elapsed_ms(sign_started)
        timings["total"] = _elapsed_ms(total_started)
        return ExecutorDryRunResult(
            False,
            api.variant,
            f"variant={api.variant}; signing/payload dry-run failed: {exc}; no order submitted",
            timings,
        )
    timings["sign"] = _elapsed_ms(sign_started)
    timings["total"] = _elapsed_ms(total_started)

    signed_type = type(signed).__name__
    return ExecutorDryRunResult(
        True,
        api.variant,
        (
            f"variant={api.variant}; token={token_id[:10]}...; amount_usd={amount_usd:.2f}; "
            f"price={price:.4f}; tick_size={market.tick_size}; min_order_size={market.min_order_size}; "
            f"{balance_summary}; signed_type={signed_type}; no order submitted; "
            + _format_timings(timings)
        ),
        timings,
    )


def missing_live_env_vars() -> list[str]:
    return [name for name in LIVE_ENV_VARS if not os.getenv(name)]


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is required for live execution")
    return value


def _import_order_api() -> _OrderApi:
    try:
        module = importlib.import_module("py_clob_client_v2")
        constants = importlib.import_module("py_clob_client_v2.order_builder.constants")
        return _OrderApi(
            variant="v2",
            market_order_args=module.MarketOrderArgs,
            partial_options=module.PartialCreateOrderOptions,
            order_type=module.OrderType,
            buy_side=constants.BUY,
            balance_allowance_params=module.BalanceAllowanceParams,
            asset_type=module.AssetType,
        )
    except ImportError as v2_exc:
        try:
            clob_types = importlib.import_module("py_clob_client.clob_types")
            constants = importlib.import_module("py_clob_client.order_builder.constants")
            return _OrderApi(
                variant="v1",
                market_order_args=clob_types.MarketOrderArgs,
                partial_options=clob_types.PartialCreateOrderOptions,
                order_type=clob_types.OrderType,
                buy_side=constants.BUY,
                balance_allowance_params=clob_types.BalanceAllowanceParams,
                asset_type=clob_types.AssetType,
            )
        except ImportError as v1_exc:
            raise ImportError(f"v2 import failed ({v2_exc}); v1 import failed ({v1_exc})") from v1_exc


def _balance_allowance_summary(client: Any, api: _OrderApi) -> str:
    if not hasattr(client, "get_balance_allowance"):
        return "balance_check=unavailable"
    params = api.balance_allowance_params(asset_type=api.asset_type.COLLATERAL)
    raw = client.get_balance_allowance(params)
    if isinstance(raw, dict):
        keys = ",".join(sorted(str(key) for key in raw.keys()))
        balance = _first_numeric(raw, ("balance", "usdc_balance"))
        allowance = _first_numeric(raw, ("allowance", "usdc_allowance"))
        parts = [f"balance_keys={keys or 'none'}"]
        if balance is not None:
            parts.append(f"balance={balance:g}")
        if allowance is not None:
            parts.append(f"allowance={allowance:g}")
        return "balance_check=ok " + " ".join(parts)
    return f"balance_check=ok response_type={type(raw).__name__}"


def _first_numeric(raw: dict[str, Any], names: tuple[str, ...]) -> float | None:
    lowered = {str(key).lower(): value for key, value in raw.items()}
    for name in names:
        value = lowered.get(name)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _elapsed_ms(started_ns: int) -> float:
    return (time.perf_counter_ns() - started_ns) / 1_000_000.0


def _format_timings(timings: dict[str, float]) -> str:
    return ", ".join(f"{name}_ms={value:.3f}" for name, value in timings.items())


def _round_buy_limit(price: float, tick_size: float) -> float:
    tick = Decimal(str(tick_size))
    value = Decimal(str(price))
    rounded = (value / tick).to_integral_value(rounding=ROUND_CEILING) * tick
    return float(min(Decimal("0.99"), rounded))


def _extract_order_id(raw: Any) -> str | None:
    if isinstance(raw, dict):
        for key in ("orderID", "order_id", "id"):
            if raw.get(key):
                return str(raw[key])
    return None


def _extract_status(raw: Any) -> str:
    if isinstance(raw, dict):
        return str(raw.get("status") or raw.get("errorMsg") or raw.get("error") or "unknown")
    return "unknown"


def _is_immediate_fill(raw: Any) -> bool:
    if not isinstance(raw, dict):
        return False
    if raw.get("success") is False:
        return False
    return str(raw.get("status") or "").lower() == "matched"


def _extract_filled_shares(raw: Any, fallback: float) -> float:
    if isinstance(raw, dict):
        for key in ("takingAmount", "taking_amount", "filledSize", "sizeMatched"):
            value = raw.get(key)
            if value in (None, ""):
                continue
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                continue
            if parsed > 0.0:
                return parsed
    return fallback
