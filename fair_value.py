from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from math import exp
from pathlib import Path

import requests


def _parse_yyyymmdd_or_yyyymm(s: str | None) -> date | None:
    if not s:
        return None
    s = s.strip()
    if len(s) >= 8 and s[0:8].isdigit():
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    if len(s) == 6 and s.isdigit():
        # Approximate: use last calendar day of the month
        y, m = int(s[0:4]), int(s[4:6])
        if m == 12:
            return date(y, 12, 31)
        return date(y, m + 1, 1) - timedelta(days=1)
    return None


def year_fraction(now_utc: datetime, expiry_date: date) -> float:
    expiry_dt = datetime(expiry_date.year, expiry_date.month, expiry_date.day, tzinfo=timezone.utc)
    dt = expiry_dt - now_utc.astimezone(timezone.utc)
    days = max(dt.total_seconds(), 0.0) / 86400.0
    return days / 365.25


def fair_futures_price(spot_index: float, r: float, d: float, t: float) -> float:
    return float(spot_index) * exp((float(r) - float(d)) * float(t))


@dataclass
class CarryInputs:
    r_annual: float
    d_annual: float
    t_years: float
    expiry_date: date | None


class RiskFreeRateProvider:
    def __init__(
        self,
        *,
        logger: logging.Logger,
        fred_api_key: str | None,
        fred_series_id: str,
        fallback_rate_annual: float,
        cache_path: str = "rate_cache.json",
    ):
        self.log = logger
        self.fred_api_key = fred_api_key
        self.fred_series_id = fred_series_id
        self.fallback_rate_annual = float(fallback_rate_annual)
        self.cache_path = Path(cache_path)
        self._rate_annual: float = self.fallback_rate_annual
        self._asof_utc: datetime | None = None
        self._load_cache()

    @property
    def rate_annual(self) -> float:
        return float(self._rate_annual)

    def _load_cache(self) -> None:
        try:
            if not self.cache_path.exists():
                return
            data = json.loads(self.cache_path.read_text(encoding="utf-8"))
            self._rate_annual = float(data["rate_annual"])
            self._asof_utc = datetime.fromisoformat(data["asof_utc"])
        except Exception as e:
            self.log.warning("Failed loading rate cache: %s", e)

    def _save_cache(self) -> None:
        try:
            payload = {
                "rate_annual": float(self._rate_annual),
                "asof_utc": (self._asof_utc or datetime.now(timezone.utc)).isoformat(),
                "source": "FRED" if self.fred_api_key else "FALLBACK",
                "series_id": self.fred_series_id,
            }
            self.cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception as e:
            self.log.warning("Failed saving rate cache: %s", e)

    def _fetch_fred_rate_sync(self) -> float:
        if not self.fred_api_key:
            raise RuntimeError("FRED_API_KEY not set")
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "api_key": self.fred_api_key,
            "file_type": "json",
            "series_id": self.fred_series_id,
            "sort_order": "desc",
            "limit": 10,
        }
        for attempt in range(3):
            try:
                resp = requests.get(url, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                obs = data.get("observations", [])
                for o in obs:
                    v = o.get("value")
                    if v is None:
                        continue
                    if v == ".":
                        continue
                    rate_pct = float(v)
                    return rate_pct / 100.0
                raise RuntimeError("No numeric observations")
            except Exception as e:
                if attempt == 2:
                    raise
                continue
        raise RuntimeError("Unreachable")

    async def refresh_if_stale(self, *, max_age_hours: float = 24.0) -> None:
        if not self.fred_api_key:
            return
        if self._asof_utc is not None:
            age = datetime.now(timezone.utc) - self._asof_utc
            if age.total_seconds() < max_age_hours * 3600:
                return
        try:
            new_rate = await asyncio.to_thread(self._fetch_fred_rate_sync)
            self._rate_annual = float(new_rate)
            self._asof_utc = datetime.now(timezone.utc)
            self._save_cache()
            self.log.info("Updated risk-free rate from FRED: %.4f", self._rate_annual)
        except Exception as e:
            self.log.warning("Failed refreshing FRED rate; using cached/fallback. err=%s", e)


class DividendYieldProvider:
    def __init__(self, *, default_dividend_yield_annual: float, by_expiry_yyyymm: dict[str, float] | None = None):
        self.default = float(default_dividend_yield_annual)
        self.by_expiry = dict(by_expiry_yyyymm or {})

    def get(self, expiry_yyyymm: str | None) -> float:
        if expiry_yyyymm and expiry_yyyymm in self.by_expiry:
            return float(self.by_expiry[expiry_yyyymm])
        return float(self.default)


def spot_index_from_spy(spy_price: float, spy_to_index_factor: float) -> float:
    return float(spy_price) * float(spy_to_index_factor)


def derive_expiry_date_from_contract_month(exp: str | None) -> date | None:
    return _parse_yyyymmdd_or_yyyymm(exp)

