from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from database import Database


@dataclass
class OpenPosition:
    trade_id: str
    direction: str
    opened_ts_utc: datetime
    entry_mispricing_bps: float
    fut_qty: int
    spy_qty: int


class RiskManager:
    def __init__(
        self,
        *,
        db: Database,
        max_fut_contracts: int,
        daily_loss_limit_usd: float,
        stop_loss_bps: float,
        max_hold_days: int,
    ):
        self.db = db
        self.max_fut_contracts = int(max_fut_contracts)
        self.daily_loss_limit_usd = float(daily_loss_limit_usd)
        self.stop_loss_bps = float(stop_loss_bps)
        self.max_hold_days = int(max_hold_days)
        self.position: OpenPosition | None = None

    def set_open_position(self, pos: OpenPosition | None) -> None:
        self.position = pos

    def within_daily_loss_limit(self) -> bool:
        pnl = self.db.realized_pnl_today()
        return pnl >= -abs(self.daily_loss_limit_usd)

    def can_open_new(self, requested_fut_contracts: int) -> bool:
        if self.position is not None:
            return False
        if abs(int(requested_fut_contracts)) > self.max_fut_contracts:
            return False
        return self.within_daily_loss_limit()

    def should_force_exit(self, *, current_mispricing_bps: float) -> bool:
        if self.position is None:
            return False

        # Stop-loss on widening against entry.
        entry = float(self.position.entry_mispricing_bps)
        cur = float(current_mispricing_bps)

        # If we entered "fut rich" (sell fut / buy spy), adverse move is more positive mispricing.
        if self.position.direction == "SELL_FUT_BUY_SPY":
            if cur - entry >= abs(self.stop_loss_bps):
                return True
        else:
            # Entered "fut cheap" (buy fut / sell spy), adverse move is more negative mispricing.
            if entry - cur >= abs(self.stop_loss_bps):
                return True

        # Max holding period to avoid roll / expiry risk.
        max_age = timedelta(days=max(self.max_hold_days, 1))
        if datetime.now(timezone.utc) - self.position.opened_ts_utc >= max_age:
            return True

        # Daily loss limit breach after entry (stop trading + exit).
        if not self.within_daily_loss_limit():
            return True

        return False

