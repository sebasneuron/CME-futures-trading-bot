from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from pathlib import Path

from database import Database, TradeRecord
from fair_value import (
    DividendYieldProvider,
    RiskFreeRateProvider,
    derive_expiry_date_from_contract_month,
    fair_futures_price,
    spot_index_from_spy,
    year_fraction,
)
from market_data import InstrumentInfo, MarketData, Quote
from order_manager import LegSpec, OrderManager
from risk_manager import OpenPosition, RiskManager


def _bps(x: float) -> float:
    return float(x) * 10_000.0


def _mispricing_bps(fut_px: float, fair_px: float) -> float:
    return _bps((float(fut_px) - float(fair_px)) / float(fair_px))


def _as_price(x: float | None) -> float | None:
    if x is None:
        return None
    if not isfinite(x) or x <= 0:
        return None
    return float(x)


@dataclass(frozen=True)
class BotState:
    fut: InstrumentInfo
    spy: InstrumentInfo
    fut_multiplier: float
    expiry_date: datetime | None


class ArbitrageBot:
    def __init__(
        self,
        *,
        md: MarketData,
        om: OrderManager,
        db: Database,
        risk: RiskManager,
        rf: RiskFreeRateProvider,
        div: DividendYieldProvider,
        logger: logging.Logger,
        trading_enabled: bool,
        kill_switch: bool,
        spy_to_index_factor: float,
        entry_threshold_bps: float,
        exit_threshold_bps: float,
        estimated_cost_bps: float,
        cooldown_seconds: int,
        hedge_timeout_seconds: int,
    ):
        self.md = md
        self.om = om
        self.db = db
        self.risk = risk
        self.rf = rf
        self.div = div
        self.log = logger
        self.trading_enabled = bool(trading_enabled)
        self.kill_switch = bool(kill_switch)
        self.spy_to_index_factor = float(spy_to_index_factor)
        self.entry_threshold_bps = float(entry_threshold_bps)
        self.exit_threshold_bps = float(exit_threshold_bps)
        self.estimated_cost_bps = float(estimated_cost_bps)
        self.cooldown_seconds = int(cooldown_seconds)
        self.hedge_timeout_seconds = int(hedge_timeout_seconds)

        self.state: BotState | None = None
        self._cooldown_until: float = 0.0

    async def initialize(self, *, fut: InstrumentInfo, spy: InstrumentInfo) -> None:
        await self.md.subscribe(fut)
        await self.md.subscribe(spy)

        mult = fut.multiplier
        if mult is None:
            # Common defaults if IB doesn't populate multiplier.
            if fut.symbol.upper() == "ES":
                mult = 50.0
            elif fut.symbol.upper() == "MES":
                mult = 5.0
            else:
                mult = 1.0

        exp_raw = fut.contract.lastTradeDateOrContractMonth or getattr(fut.contract, "lastTradeDate", None)
        exp_date = derive_expiry_date_from_contract_month(exp_raw)
        exp_dt = datetime(exp_date.year, exp_date.month, exp_date.day, tzinfo=timezone.utc) if exp_date else None

        self.state = BotState(fut=fut, spy=spy, fut_multiplier=float(mult), expiry_date=exp_dt)
        self.log.info("Bot initialized. fut_multiplier=%s expiry=%s", mult, exp_dt)

    def _in_cooldown(self) -> bool:
        return asyncio.get_event_loop().time() < self._cooldown_until

    def _set_cooldown(self) -> None:
        self._cooldown_until = asyncio.get_event_loop().time() + float(max(self.cooldown_seconds, 0))

    @staticmethod
    def _spy_shares_for_one_fut(*, fut_price: float, fut_multiplier: float, spy_price: float) -> int:
        notional = float(fut_price) * float(fut_multiplier)
        shares = int(round(notional / float(spy_price)))
        return max(shares, 1)

    def _quotes(self) -> tuple[Quote, Quote] | None:
        if not self.state:
            return None
        fq = self.md.get_quote(self.state.fut.conid)
        sq = self.md.get_quote(self.state.spy.conid)
        if fq is None or sq is None:
            return None
        return fq, sq

    def _carry_and_fair(self, *, spy_price: float, expiry_dt: datetime | None) -> tuple[float, float, float]:
        now = datetime.now(timezone.utc)
        r = self.rf.rate_annual
        d = self.div.get(self.state.fut.expiry_yyyymm if self.state else None)
        if expiry_dt is None:
            t = 1.0 / 365.25
        else:
            t = year_fraction(now, expiry_dt.date())
        spot_index = spot_index_from_spy(spy_price, self.spy_to_index_factor)
        fair = fair_futures_price(spot_index, r, d, t)
        return r, d, fair

    async def _try_open(self, *, direction: str, fut_px: float, spy_px: float, fair: float, mis_bps: float) -> None:
        assert self.state is not None
        if self.kill_switch:
            self.log.warning("Kill switch enabled; ignoring entry signal")
            return
        if self._in_cooldown():
            return
        if not self.risk.can_open_new(1):
            return

        spy_shares = self._spy_shares_for_one_fut(fut_price=fut_px, fut_multiplier=self.state.fut_multiplier, spy_price=spy_px)
        fut_qty = 1

        if direction == "SELL_FUT_BUY_SPY":
            fut_action = "SELL"
            spy_action = "BUY"
        else:
            fut_action = "BUY"
            spy_action = "SELL"

        trade_id = str(uuid.uuid4())
        self.db.insert_trade(
            TradeRecord(
                trade_id=trade_id,
                ts_utc=datetime.now(timezone.utc),
                direction=direction,
                fut_conid=self.state.fut.conid,
                spy_conid=self.state.spy.conid,
                fut_qty=fut_qty,
                spy_qty=spy_shares,
                fair_fut=float(fair),
                entry_mispricing_bps=float(mis_bps),
                status="OPEN",
            )
        )

        if not self.trading_enabled:
            self.log.info(
                "SIGNAL (paper): %s mis=%.1fbps fut=%.2f spy=%.2f fair=%.2f shares=%s",
                direction,
                mis_bps,
                fut_px,
                spy_px,
                fair,
                spy_shares,
            )
            self.db.close_trade(trade_id, None, None, None, status="CLOSED")
            self._set_cooldown()
            return

        fut_leg = LegSpec(
            contract=self.state.fut.contract,
            conid=self.state.fut.conid,
            symbol=self.state.fut.symbol,
            action=fut_action,
            quantity=fut_qty,
        )
        spy_leg = LegSpec(
            contract=self.state.spy.contract,
            conid=self.state.spy.conid,
            symbol=self.state.spy.symbol,
            action=spy_action,
            quantity=spy_shares,
        )

        res = await self.om.place_paired_ioc_limits(
            trade_id=trade_id,
            fut_leg=fut_leg,
            spy_leg=spy_leg,
            fut_limit_price=float(fut_px),
            spy_limit_price=float(spy_px),
            hedge_timeout_seconds=self.hedge_timeout_seconds,
        )

        if not res.ok:
            self.log.error("Entry failed trade_id=%s err=%s", trade_id, res.error)
            self.db.close_trade(trade_id, res.fut_avg_fill, res.spy_avg_fill, None, status="ERROR")
            self._set_cooldown()
            return

        self.db.update_trade_entry_prices(trade_id, res.fut_avg_fill, res.spy_avg_fill)
        self.risk.set_open_position(
            OpenPosition(
                trade_id=trade_id,
                direction=direction,
                opened_ts_utc=datetime.now(timezone.utc),
                entry_mispricing_bps=float(mis_bps),
                fut_qty=fut_qty,
                spy_qty=spy_shares,
            )
        )
        self.log.info("Opened trade_id=%s %s fut=%.2f spy=%.2f", trade_id, direction, res.fut_avg_fill or fut_px, res.spy_avg_fill or spy_px)
        self._set_cooldown()

    async def _try_close(self, *, reason: str, fut_mid: float, spy_mid: float) -> None:
        assert self.state is not None
        pos = self.risk.position
        if pos is None:
            return
        if self.kill_switch:
            self.log.warning("Kill switch enabled; closing anyway. reason=%s", reason)

        if not self.trading_enabled:
            self.log.info("Would close (paper) trade_id=%s reason=%s", pos.trade_id, reason)
            self.db.close_trade(pos.trade_id, None, None, 0.0, status="CLOSED")
            self.risk.set_open_position(None)
            self._set_cooldown()
            return

        if pos.direction == "SELL_FUT_BUY_SPY":
            close_fut_action = "BUY"
            close_spy_action = "SELL"
        else:
            close_fut_action = "SELL"
            close_spy_action = "BUY"

        exit_fut, exit_spy = await self.om.flatten_position_market(
            trade_id=pos.trade_id,
            fut_contract=self.state.fut.contract,
            fut_conid=self.state.fut.conid,
            fut_symbol=self.state.fut.symbol,
            fut_action=close_fut_action,
            fut_qty=pos.fut_qty,
            spy_contract=self.state.spy.contract,
            spy_conid=self.state.spy.conid,
            spy_symbol=self.state.spy.symbol,
            spy_action=close_spy_action,
            spy_qty=pos.spy_qty,
            timeout_seconds=self.hedge_timeout_seconds,
        )

        row = self.db.fetchone("SELECT entry_fut_price, entry_spy_price, direction, fut_qty, spy_qty FROM trades WHERE trade_id=?", (pos.trade_id,))
        realized = None
        if row and row["entry_fut_price"] and row["entry_spy_price"] and exit_fut and exit_spy:
            realized = self.om.estimate_realized_pnl_usd(
                direction=row["direction"],
                fut_multiplier=self.state.fut_multiplier,
                fut_qty=int(row["fut_qty"]),
                spy_qty=int(row["spy_qty"]),
                entry_fut=float(row["entry_fut_price"]),
                entry_spy=float(row["entry_spy_price"]),
                exit_fut=float(exit_fut),
                exit_spy=float(exit_spy),
            )

        self.db.close_trade(pos.trade_id, exit_fut, exit_spy, realized, status="CLOSED")
        self.log.info("Closed trade_id=%s reason=%s realized_pnl=%s", pos.trade_id, reason, realized)
        self.risk.set_open_position(None)
        self._set_cooldown()

    async def run(self) -> None:
        if self.state is None:
            raise RuntimeError("Bot not initialized")

        kill_file = Path("KILL_SWITCH")

        while True:
            if kill_file.exists():
                self.kill_switch = True

            await self.rf.refresh_if_stale()
            q = self._quotes()
            if q is None:
                await asyncio.sleep(0.1)
                continue

            fut_q, spy_q = q
            es_bid = _as_price(fut_q.bid)
            es_ask = _as_price(fut_q.ask)
            es_mid = _as_price(fut_q.mid)
            spy_bid = _as_price(spy_q.bid)
            spy_ask = _as_price(spy_q.ask)
            spy_mid = _as_price(spy_q.mid)

            if es_mid is None or spy_mid is None:
                await asyncio.sleep(0.1)
                continue

            # Compute mid fair value for monitoring / exits.
            _, _, fair_mid = self._carry_and_fair(spy_price=spy_mid, expiry_dt=self.state.expiry_date)
            mis_mid_bps = _mispricing_bps(es_mid, fair_mid)

            if self.risk.position is not None:
                if self.kill_switch:
                    await self._try_close(reason="kill_switch", fut_mid=es_mid, spy_mid=spy_mid)
                    await asyncio.sleep(0.1)
                    continue

                if self.risk.should_force_exit(current_mispricing_bps=mis_mid_bps):
                    await self._try_close(reason="risk", fut_mid=es_mid, spy_mid=spy_mid)
                    await asyncio.sleep(0.1)
                    continue

                if abs(mis_mid_bps) <= (abs(self.exit_threshold_bps) + abs(self.estimated_cost_bps)):
                    await self._try_close(reason="converged", fut_mid=es_mid, spy_mid=spy_mid)
                    await asyncio.sleep(0.1)
                    continue

            # Entry logic (actionable, uses bid/ask).
            if self.risk.position is None and not self._in_cooldown():
                threshold = abs(self.entry_threshold_bps) + abs(self.estimated_cost_bps)

                if es_bid is not None and spy_ask is not None:
                    _, _, fair_sell = self._carry_and_fair(spy_price=spy_ask, expiry_dt=self.state.expiry_date)
                    mis_sell_bps = _mispricing_bps(es_bid, fair_sell)
                    if mis_sell_bps >= threshold:
                        await self._try_open(
                            direction="SELL_FUT_BUY_SPY",
                            fut_px=es_bid,
                            spy_px=spy_ask,
                            fair=fair_sell,
                            mis_bps=mis_sell_bps,
                        )
                        await asyncio.sleep(0.1)
                        continue

                if es_ask is not None and spy_bid is not None:
                    _, _, fair_buy = self._carry_and_fair(spy_price=spy_bid, expiry_dt=self.state.expiry_date)
                    mis_buy_bps = _mispricing_bps(es_ask, fair_buy)
                    if mis_buy_bps <= -threshold:
                        await self._try_open(
                            direction="BUY_FUT_SELL_SPY",
                            fut_px=es_ask,
                            spy_px=spy_bid,
                            fair=fair_buy,
                            mis_bps=mis_buy_bps,
                        )
                        await asyncio.sleep(0.1)
                        continue

            await asyncio.sleep(0.1)

