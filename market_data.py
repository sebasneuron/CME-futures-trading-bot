from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from typing import TYPE_CHECKING, Any

from asyncio_compat import ensure_event_loop

if TYPE_CHECKING:
    from ib_insync import IB, Contract, ContractDetails


@dataclass(frozen=True)
class Quote:
    bid: float | None
    ask: float | None
    last: float | None
    mid: float | None
    ts_utc: datetime


@dataclass(frozen=True)
class InstrumentInfo:
    contract: Any
    conid: int
    symbol: str
    expiry_yyyymm: str | None
    multiplier: float | None


def _safe_mid(bid: float | None, ask: float | None, last: float | None) -> float | None:
    if bid is not None and ask is not None and isfinite(bid) and isfinite(ask) and bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    if last is not None and isfinite(last) and last > 0:
        return float(last)
    return None


def _parse_expiry_to_sort_key(exp: str) -> tuple[int, int, int]:
    # Common formats: YYYYMM or YYYYMMDD
    exp = exp.strip()
    if len(exp) >= 8:
        return int(exp[0:4]), int(exp[4:6]), int(exp[6:8])
    if len(exp) == 6:
        return int(exp[0:4]), int(exp[4:6]), 1
    return 9999, 12, 31


def _expiry_yyyymm(exp: str | None) -> str | None:
    if not exp:
        return None
    exp = exp.strip()
    if len(exp) >= 6 and exp[0:6].isdigit():
        return exp[0:6]
    return None


class MarketData:
    def __init__(self, ib: "IB", logger: logging.Logger):
        self.ib = ib
        self.log = logger
        self._ticker_by_conid: dict[int, Any] = {}
        self._quote_by_conid: dict[int, Quote] = {}
        self._lock = asyncio.Lock()

        # ib_insync provides a batch of tickers on pendingTickersEvent
        self.ib.pendingTickersEvent += self._on_pending_tickers

    def _on_pending_tickers(self, tickers: set[Any]) -> None:
        now = datetime.now(timezone.utc)
        for t in tickers:
            conid = getattr(t.contract, "conId", 0) or 0
            if conid <= 0:
                continue
            bid = float(t.bid) if t.bid is not None and isfinite(t.bid) and t.bid > 0 else None
            ask = float(t.ask) if t.ask is not None and isfinite(t.ask) and t.ask > 0 else None
            last = float(t.last) if t.last is not None and isfinite(t.last) and t.last > 0 else None
            mid = _safe_mid(bid, ask, last)
            self._quote_by_conid[conid] = Quote(bid=bid, ask=ask, last=last, mid=mid, ts_utc=now)

    async def qualify(self, contract: Any) -> Any:
        qualified = await self.ib.qualifyContractsAsync(contract)
        if not qualified:
            raise RuntimeError(f"Could not qualify contract: {contract}")
        return qualified[0]

    async def _get_future_contract_details(self, symbol: str, exchange: str, currency: str) -> list["ContractDetails"]:
        # Request all available futures for the symbol.
        ensure_event_loop()
        from ib_insync import Future

        details = await self.ib.reqContractDetailsAsync(Future(symbol=symbol, exchange=exchange, currency=currency))
        if not details:
            raise RuntimeError(f"No contract details returned for Future({symbol}, {exchange}, {currency})")
        return details

    async def resolve_front_future(
        self,
        *,
        symbol: str,
        exchange: str,
        currency: str,
        front_month_offset: int = 0,
    ) -> InstrumentInfo:
        details = await self._get_future_contract_details(symbol, exchange, currency)

        now = datetime.now(timezone.utc)
        today_key = (now.year, now.month, now.day)

        candidates: list[tuple[tuple[int, int, int], ContractDetails]] = []
        for d in details:
            exp = d.contract.lastTradeDateOrContractMonth or d.contract.lastTradeDate
            if not exp:
                continue
            k = _parse_expiry_to_sort_key(exp)
            if k >= today_key:
                candidates.append((k, d))

        if not candidates:
            candidates = [(_parse_expiry_to_sort_key(d.contract.lastTradeDateOrContractMonth or ""), d) for d in details]

        candidates.sort(key=lambda x: x[0])
        idx = min(max(front_month_offset, 0), len(candidates) - 1)
        chosen = candidates[idx][1]
        c = await self.qualify(chosen.contract)

        mult = None
        if getattr(c, "multiplier", None):
            try:
                mult = float(c.multiplier)
            except Exception:
                mult = None

        exp_ym = _expiry_yyyymm(c.lastTradeDateOrContractMonth or getattr(c, "lastTradeDate", None))

        self.log.info(
            "Resolved future %s -> conId=%s expiry=%s multiplier=%s",
            symbol,
            c.conId,
            exp_ym,
            mult,
        )

        return InstrumentInfo(contract=c, conid=c.conId, symbol=symbol, expiry_yyyymm=exp_ym, multiplier=mult)

    async def resolve_spy(self, *, symbol: str = "SPY", exchange: str = "ARCA", currency: str = "USD") -> InstrumentInfo:
        ensure_event_loop()
        from ib_insync import Stock

        c = await self.qualify(Stock(symbol=symbol, exchange=exchange, currency=currency))
        return InstrumentInfo(contract=c, conid=c.conId, symbol=symbol, expiry_yyyymm=None, multiplier=None)

    async def subscribe(self, info: InstrumentInfo) -> None:
        async with self._lock:
            if info.conid in self._ticker_by_conid:
                return
            t = self.ib.reqMktData(info.contract, "", False, False)
            self._ticker_by_conid[info.conid] = t
            self.log.info("Subscribed market data conId=%s (%s)", info.conid, info.symbol)

    def get_quote(self, conid: int) -> Quote | None:
        return self._quote_by_conid.get(conid)

