from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite

import numpy as np
import pandas as pd

from fair_value import fair_futures_price, spot_index_from_spy, year_fraction


@dataclass
class TradeSim:
    direction: str
    entry_ts: datetime
    exit_ts: datetime | None = None
    entry_fut: float = 0.0
    entry_spy: float = 0.0
    exit_fut: float | None = None
    exit_spy: float | None = None
    fut_qty: int = 1
    spy_qty: int = 0
    pnl: float | None = None


def _mid(bid: float, ask: float, last: float | None = None) -> float | None:
    if isfinite(bid) and isfinite(ask) and bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    if last is not None and isfinite(last) and last > 0:
        return float(last)
    return None


def _mispricing_bps(fut_px: float, fair_px: float) -> float:
    return (fut_px - fair_px) / fair_px * 10_000.0


def _shares_for_one_fut(fut_price: float, fut_multiplier: float, spy_price: float) -> int:
    notional = fut_price * fut_multiplier
    return max(int(round(notional / spy_price)), 1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="CSV with columns: ts, es_bid, es_ask, spy_bid, spy_ask")
    ap.add_argument("--expiry", required=True, help="Futures expiry date YYYY-MM-DD")
    ap.add_argument("--spy_to_index", type=float, default=10.0)
    ap.add_argument("--fut_multiplier", type=float, default=50.0)
    ap.add_argument("--r", type=float, default=0.045)
    ap.add_argument("--d", type=float, default=0.014)
    ap.add_argument("--entry_bps", type=float, default=20.0)
    ap.add_argument("--exit_bps", type=float, default=5.0)
    ap.add_argument("--cost_bps", type=float, default=4.0)
    args = ap.parse_args()

    df = pd.read_csv(args.csv)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    expiry_dt = datetime.fromisoformat(args.expiry).replace(tzinfo=timezone.utc)

    open_trade: TradeSim | None = None
    trades: list[TradeSim] = []
    equity = 0.0
    equity_curve = []

    threshold = abs(args.entry_bps) + abs(args.cost_bps)
    exit_thr = abs(args.exit_bps) + abs(args.cost_bps)

    for _, row in df.iterrows():
        ts: datetime = row["ts"].to_pydatetime()
        es_bid = float(row["es_bid"])
        es_ask = float(row["es_ask"])
        spy_bid = float(row["spy_bid"])
        spy_ask = float(row["spy_ask"])

        es_mid = _mid(es_bid, es_ask)
        spy_mid = _mid(spy_bid, spy_ask)
        if es_mid is None or spy_mid is None:
            equity_curve.append(equity)
            continue

        t = year_fraction(ts, expiry_dt.date())
        fair_mid = fair_futures_price(spot_index_from_spy(spy_mid, args.spy_to_index), args.r, args.d, t)
        mis_mid = _mispricing_bps(es_mid, fair_mid)

        if open_trade is not None:
            if abs(mis_mid) <= exit_thr:
                open_trade.exit_ts = ts
                open_trade.exit_fut = es_mid
                open_trade.exit_spy = spy_mid
                if open_trade.direction == "SELL_FUT_BUY_SPY":
                    fut_signed = -1
                    spy_signed = 1
                else:
                    fut_signed = 1
                    spy_signed = -1
                fut_pnl = (open_trade.exit_fut - open_trade.entry_fut) * args.fut_multiplier * fut_signed
                spy_pnl = (open_trade.exit_spy - open_trade.entry_spy) * open_trade.spy_qty * spy_signed
                open_trade.pnl = float(fut_pnl + spy_pnl)
                equity += open_trade.pnl
                trades.append(open_trade)
                open_trade = None
            equity_curve.append(equity)
            continue

        # Entry (actionable)
        fair_sell = fair_futures_price(spot_index_from_spy(spy_ask, args.spy_to_index), args.r, args.d, t)
        mis_sell = _mispricing_bps(es_bid, fair_sell)
        if mis_sell >= threshold:
            shares = _shares_for_one_fut(es_bid, args.fut_multiplier, spy_ask)
            open_trade = TradeSim(direction="SELL_FUT_BUY_SPY", entry_ts=ts, entry_fut=es_bid, entry_spy=spy_ask, spy_qty=shares)
            equity_curve.append(equity)
            continue

        fair_buy = fair_futures_price(spot_index_from_spy(spy_bid, args.spy_to_index), args.r, args.d, t)
        mis_buy = _mispricing_bps(es_ask, fair_buy)
        if mis_buy <= -threshold:
            shares = _shares_for_one_fut(es_ask, args.fut_multiplier, spy_bid)
            open_trade = TradeSim(direction="BUY_FUT_SELL_SPY", entry_ts=ts, entry_fut=es_ask, entry_spy=spy_bid, spy_qty=shares)
            equity_curve.append(equity)
            continue

        equity_curve.append(equity)

    pnl = sum(t.pnl for t in trades if t.pnl is not None)
    eq = np.array(equity_curve, dtype=float)
    dd = eq - np.maximum.accumulate(eq) if len(eq) else np.array([0.0])
    max_dd = float(dd.min()) if len(dd) else 0.0

    print(f"Trades: {len(trades)}")
    print(f"Total PnL: {pnl:.2f}")
    print(f"Max drawdown (equity): {max_dd:.2f}")
    if trades:
        wins = sum(1 for t in trades if (t.pnl or 0) > 0)
        print(f"Win rate: {wins/len(trades)*100:.1f}%")


if __name__ == "__main__":
    main()

