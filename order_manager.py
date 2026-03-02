from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from asyncio_compat import ensure_event_loop

from database import Database

if TYPE_CHECKING:
    from ib_insync import IB, Order, Trade


Direction = Literal["SELL_FUT_BUY_SPY", "BUY_FUT_SELL_SPY"]


@dataclass(frozen=True)
class LegSpec:
    contract: Any
    conid: int
    symbol: str
    action: Literal["BUY", "SELL"]
    quantity: int


@dataclass(frozen=True)
class PairTradeResult:
    ok: bool
    fut_avg_fill: float | None
    spy_avg_fill: float | None
    error: str | None = None


def _mk_limit(action: str, qty: int, price: float, tif: str = "IOC") -> "Order":
    ensure_event_loop()
    from ib_insync import Order

    o = Order()
    o.action = action
    o.orderType = "LMT"
    o.totalQuantity = float(qty)
    o.lmtPrice = float(price)
    o.tif = tif
    o.transmit = True
    return o


def _mk_market(action: str, qty: int) -> "Order":
    ensure_event_loop()
    from ib_insync import MarketOrder

    return MarketOrder(action, qty)


def _avg_fill_price(trade: "Trade") -> float | None:
    fills = getattr(trade, "fills", None) or []
    if not fills:
        return None
    total_qty = 0.0
    total_px_qty = 0.0
    for f in fills:
        ex = getattr(f, "execution", None)
        if not ex:
            continue
        qty = float(getattr(ex, "shares", 0.0) or 0.0)
        px = float(getattr(ex, "price", 0.0) or 0.0)
        if qty <= 0 or px <= 0:
            continue
        total_qty += qty
        total_px_qty += qty * px
    if total_qty <= 0:
        return None
    return total_px_qty / total_qty


class OrderManager:
    def __init__(self, ib: "IB", db: Database, logger: logging.Logger):
        self.ib = ib
        self.db = db
        self.log = logger

    async def _wait_done(self, trade: "Trade", timeout_s: int) -> None:
        end = asyncio.get_event_loop().time() + float(timeout_s)
        while asyncio.get_event_loop().time() < end:
            if trade.isDone():
                return
            await asyncio.sleep(0.05)
        return

    def _record_fills(self, trade_id: str | None, conid: int, symbol: str, trade: "Trade") -> None:
        for f in getattr(trade, "fills", None) or []:
            ex = getattr(f, "execution", None)
            c = getattr(f, "commissionReport", None)
            if not ex:
                continue
            self.db.insert_fill(
                trade_id=trade_id,
                conid=conid,
                symbol=symbol,
                side=str(getattr(ex, "side", "") or ""),
                qty=float(getattr(ex, "shares", 0.0) or 0.0),
                price=float(getattr(ex, "price", 0.0) or 0.0),
                commission=float(getattr(c, "commission", 0.0)) if c and getattr(c, "commission", None) is not None else None,
                order_id=int(getattr(ex, "orderId", 0) or 0) if getattr(ex, "orderId", None) else None,
                exec_id=str(getattr(ex, "execId", "") or "") if getattr(ex, "execId", None) else None,
            )

    async def place_paired_ioc_limits(
        self,
        *,
        trade_id: str,
        fut_leg: LegSpec,
        spy_leg: LegSpec,
        fut_limit_price: float,
        spy_limit_price: float,
        hedge_timeout_seconds: int,
    ) -> PairTradeResult:
        if not self.ib.isConnected():
            return PairTradeResult(ok=False, fut_avg_fill=None, spy_avg_fill=None, error="IB not connected")

        fut_order = _mk_limit(fut_leg.action, fut_leg.quantity, fut_limit_price, tif="IOC")
        spy_order = _mk_limit(spy_leg.action, spy_leg.quantity, spy_limit_price, tif="IOC")

        self.log.info(
            "Placing paired IOC limits trade_id=%s FUT %s %s @%.2f | SPY %s %s @%.2f",
            trade_id,
            fut_leg.action,
            fut_leg.quantity,
            fut_limit_price,
            spy_leg.action,
            spy_leg.quantity,
            spy_limit_price,
        )

        fut_trade = self.ib.placeOrder(fut_leg.contract, fut_order)
        spy_trade = self.ib.placeOrder(spy_leg.contract, spy_order)

        await asyncio.gather(
            self._wait_done(fut_trade, hedge_timeout_seconds),
            self._wait_done(spy_trade, hedge_timeout_seconds),
        )

        self._record_fills(trade_id, fut_leg.conid, fut_leg.symbol, fut_trade)
        self._record_fills(trade_id, spy_leg.conid, spy_leg.symbol, spy_trade)

        fut_filled = float(getattr(fut_trade.orderStatus, "filled", 0.0) or 0.0)
        spy_filled = float(getattr(spy_trade.orderStatus, "filled", 0.0) or 0.0)

        fut_avg = _avg_fill_price(fut_trade)
        spy_avg = _avg_fill_price(spy_trade)

        # If only one leg filled, immediately hedge remaining with market order.
        if fut_filled > 0 and spy_filled < float(spy_leg.quantity):
            remaining = int(round(float(spy_leg.quantity) - spy_filled))
            if remaining > 0:
                self.log.warning("Partial hedge: SPY remaining=%s -> sending MARKET", remaining)
                hedge_trade = self.ib.placeOrder(spy_leg.contract, _mk_market(spy_leg.action, remaining))
                await self._wait_done(hedge_trade, hedge_timeout_seconds)
                self._record_fills(trade_id, spy_leg.conid, spy_leg.symbol, hedge_trade)
                spy_avg = spy_avg or _avg_fill_price(hedge_trade)
                spy_filled = float(getattr(hedge_trade.orderStatus, "filled", 0.0) or 0.0) + spy_filled

        if spy_filled > 0 and fut_filled < float(fut_leg.quantity):
            remaining = int(round(float(fut_leg.quantity) - fut_filled))
            if remaining > 0:
                self.log.warning("Partial hedge: FUT remaining=%s -> sending MARKET", remaining)
                hedge_trade = self.ib.placeOrder(fut_leg.contract, _mk_market(fut_leg.action, remaining))
                await self._wait_done(hedge_trade, hedge_timeout_seconds)
                self._record_fills(trade_id, fut_leg.conid, fut_leg.symbol, hedge_trade)
                fut_avg = fut_avg or _avg_fill_price(hedge_trade)
                fut_filled = float(getattr(hedge_trade.orderStatus, "filled", 0.0) or 0.0) + fut_filled

        ok = (fut_filled >= float(fut_leg.quantity) - 1e-6) and (spy_filled >= float(spy_leg.quantity) - 1e-6)
        if not ok:
            return PairTradeResult(ok=False, fut_avg_fill=fut_avg, spy_avg_fill=spy_avg, error="Paired order not fully filled")

        return PairTradeResult(ok=True, fut_avg_fill=fut_avg, spy_avg_fill=spy_avg, error=None)

    async def flatten_position_market(
        self,
        *,
        trade_id: str,
        fut_contract: Any,
        fut_conid: int,
        fut_symbol: str,
        fut_action: Literal["BUY", "SELL"],
        fut_qty: int,
        spy_contract: Any,
        spy_conid: int,
        spy_symbol: str,
        spy_action: Literal["BUY", "SELL"],
        spy_qty: int,
        timeout_seconds: int,
    ) -> tuple[float | None, float | None]:
        self.log.warning("Flattening position trade_id=%s via MARKET", trade_id)
        fut_trade = self.ib.placeOrder(fut_contract, _mk_market(fut_action, fut_qty))
        spy_trade = self.ib.placeOrder(spy_contract, _mk_market(spy_action, spy_qty))

        await asyncio.gather(self._wait_done(fut_trade, timeout_seconds), self._wait_done(spy_trade, timeout_seconds))
        self._record_fills(trade_id, fut_conid, fut_symbol, fut_trade)
        self._record_fills(trade_id, spy_conid, spy_symbol, spy_trade)

        return _avg_fill_price(fut_trade), _avg_fill_price(spy_trade)

    @staticmethod
    def estimate_realized_pnl_usd(
        *,
        direction: Direction,
        fut_multiplier: float,
        fut_qty: int,
        spy_qty: int,
        entry_fut: float,
        entry_spy: float,
        exit_fut: float,
        exit_spy: float,
    ) -> float:
        # Positive PnL means profit.
        # FUT PnL = (exit - entry) * multiplier * signed_qty
        # SPY PnL = (exit - entry) * signed_shares
        if direction == "SELL_FUT_BUY_SPY":
            fut_signed = -abs(int(fut_qty))
            spy_signed = abs(int(spy_qty))
        else:
            fut_signed = abs(int(fut_qty))
            spy_signed = -abs(int(spy_qty))

        fut_pnl = (float(exit_fut) - float(entry_fut)) * float(fut_multiplier) * float(fut_signed)
        spy_pnl = (float(exit_spy) - float(entry_spy)) * float(spy_signed)
        return float(fut_pnl + spy_pnl)

