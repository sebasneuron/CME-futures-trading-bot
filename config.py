from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timezone

from dotenv import load_dotenv


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _get_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw


@dataclass(frozen=True)
class Settings:
    # IB connection
    ib_host: str
    ib_port: int
    ib_client_id: int

    # Mode
    trading_enabled: bool
    kill_switch: bool

    # Instruments
    fut_symbol: str
    fut_exchange: str
    fut_currency: str
    fut_use_front_month: bool
    fut_front_month_offset: int

    spy_symbol: str
    spy_exchange: str
    spy_currency: str

    spy_to_index_factor: float

    # Carry inputs
    risk_free_rate_annual: float
    dividend_yield_annual: float
    fred_api_key: str | None
    fred_series_id: str

    # Trading rules
    entry_threshold_bps: float
    exit_threshold_bps: float
    estimated_cost_bps: float
    cooldown_seconds: int
    max_hold_days: int

    # Risk limits
    max_fut_contracts: int
    daily_loss_limit_usd: float
    stop_loss_bps: float
    hedge_timeout_seconds: int

    # Infra
    log_level: str
    db_path: str

    # Time handling
    tz = timezone.utc


def load_settings() -> Settings:
    load_dotenv(override=False)

    fred_key = os.getenv("FRED_API_KEY")
    if fred_key is not None and fred_key.strip() == "":
        fred_key = None

    return Settings(
        ib_host=_get_str("IB_HOST", "127.0.0.1"),
        ib_port=_get_int("IB_PORT", 7497),
        ib_client_id=_get_int("IB_CLIENT_ID", 7),
        trading_enabled=_get_bool("TRADING_ENABLED", False),
        kill_switch=_get_bool("KILL_SWITCH", False),
        fut_symbol=_get_str("FUT_SYMBOL", "ES"),
        fut_exchange=_get_str("FUT_EXCHANGE", "CME"),
        fut_currency=_get_str("FUT_CURRENCY", "USD"),
        fut_use_front_month=_get_bool("FUT_USE_FRONT_MONTH", True),
        fut_front_month_offset=_get_int("FUT_FRONT_MONTH_OFFSET", 0),
        spy_symbol=_get_str("SPY_SYMBOL", "SPY"),
        spy_exchange=_get_str("SPY_EXCHANGE", "ARCA"),
        spy_currency=_get_str("SPY_CURRENCY", "USD"),
        spy_to_index_factor=_get_float("SPY_TO_INDEX_FACTOR", 10.0),
        risk_free_rate_annual=_get_float("RISK_FREE_RATE_ANNUAL", 0.045),
        dividend_yield_annual=_get_float("DIVIDEND_YIELD_ANNUAL", 0.014),
        fred_api_key=fred_key,
        fred_series_id=_get_str("FRED_SERIES_ID", "DTB3"),
        entry_threshold_bps=_get_float("ENTRY_THRESHOLD_BPS", 20.0),
        exit_threshold_bps=_get_float("EXIT_THRESHOLD_BPS", 5.0),
        estimated_cost_bps=_get_float("ESTIMATED_COST_BPS", 4.0),
        cooldown_seconds=_get_int("COOLDOWN_SECONDS", 30),
        max_hold_days=_get_int("MAX_HOLD_DAYS", 5),
        max_fut_contracts=_get_int("MAX_FUT_CONTRACTS", 1),
        daily_loss_limit_usd=_get_float("DAILY_LOSS_LIMIT_USD", 250.0),
        stop_loss_bps=_get_float("STOP_LOSS_BPS", 40.0),
        hedge_timeout_seconds=_get_int("HEDGE_TIMEOUT_SECONDS", 5),
        log_level=_get_str("LOG_LEVEL", "INFO"),
        db_path=_get_str("DB_PATH", "arb_bot.sqlite3"),
    )
