from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class TradeRecord:
    trade_id: str
    ts_utc: datetime
    direction: str  # "SELL_FUT_BUY_SPY" or "BUY_FUT_SELL_SPY"
    fut_conid: int
    spy_conid: int
    fut_qty: int
    spy_qty: int
    fair_fut: float
    entry_mispricing_bps: float
    status: str  # "OPEN" / "CLOSED" / "ERROR"


class Database:
    def __init__(self, path: str):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self._conn.close()

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS trades (
              trade_id TEXT PRIMARY KEY,
              ts_utc TEXT NOT NULL,
              direction TEXT NOT NULL,
              fut_conid INTEGER NOT NULL,
              spy_conid INTEGER NOT NULL,
              fut_qty INTEGER NOT NULL,
              spy_qty INTEGER NOT NULL,
              fair_fut REAL NOT NULL,
              entry_mispricing_bps REAL NOT NULL,
              status TEXT NOT NULL,
              entry_fut_price REAL,
              entry_spy_price REAL,
              exit_ts_utc TEXT,
              exit_fut_price REAL,
              exit_spy_price REAL,
              realized_pnl_usd REAL
            );

            CREATE TABLE IF NOT EXISTS fills (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_utc TEXT NOT NULL,
              trade_id TEXT,
              conid INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              side TEXT NOT NULL,
              qty REAL NOT NULL,
              price REAL NOT NULL,
              commission REAL,
              order_id INTEGER,
              exec_id TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_fills_trade_id ON fills(trade_id);
            CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts_utc);
            """
        )
        self._conn.commit()

    def execute(self, sql: str, params: Iterable[Any] = ()) -> None:
        self._conn.execute(sql, tuple(params))
        self._conn.commit()

    def executemany(self, sql: str, rows: list[Iterable[Any]]) -> None:
        self._conn.executemany(sql, [tuple(r) for r in rows])
        self._conn.commit()

    def fetchone(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
        cur = self._conn.execute(sql, tuple(params))
        return cur.fetchone()

    def fetchall(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        cur = self._conn.execute(sql, tuple(params))
        return list(cur.fetchall())

    @staticmethod
    def _ts(ts: datetime | None = None) -> str:
        if ts is None:
            ts = datetime.now(timezone.utc)
        return ts.astimezone(timezone.utc).isoformat()

    def insert_trade(self, tr: TradeRecord) -> None:
        self.execute(
            """
            INSERT INTO trades (
              trade_id, ts_utc, direction, fut_conid, spy_conid, fut_qty, spy_qty,
              fair_fut, entry_mispricing_bps, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tr.trade_id,
                self._ts(tr.ts_utc),
                tr.direction,
                tr.fut_conid,
                tr.spy_conid,
                tr.fut_qty,
                tr.spy_qty,
                tr.fair_fut,
                tr.entry_mispricing_bps,
                tr.status,
            ),
        )

    def update_trade_entry_prices(self, trade_id: str, fut_price: float | None, spy_price: float | None) -> None:
        self.execute(
            "UPDATE trades SET entry_fut_price=?, entry_spy_price=? WHERE trade_id=?",
            (fut_price, spy_price, trade_id),
        )

    def close_trade(
        self,
        trade_id: str,
        exit_fut_price: float | None,
        exit_spy_price: float | None,
        realized_pnl_usd: float | None,
        status: str = "CLOSED",
    ) -> None:
        self.execute(
            """
            UPDATE trades
            SET status=?,
                exit_ts_utc=?,
                exit_fut_price=?,
                exit_spy_price=?,
                realized_pnl_usd=?
            WHERE trade_id=?
            """,
            (status, self._ts(), exit_fut_price, exit_spy_price, realized_pnl_usd, trade_id),
        )

    def insert_fill(
        self,
        *,
        trade_id: str | None,
        conid: int,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        commission: float | None,
        order_id: int | None,
        exec_id: str | None,
    ) -> None:
        self.execute(
            """
            INSERT INTO fills (ts_utc, trade_id, conid, symbol, side, qty, price, commission, order_id, exec_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (self._ts(), trade_id, conid, symbol, side, qty, price, commission, order_id, exec_id),
        )

    def realized_pnl_today(self) -> float:
        # Uses UTC day; adjust if you prefer US/Eastern.
        day_prefix = datetime.now(timezone.utc).date().isoformat()
        row = self.fetchone(
            """
            SELECT COALESCE(SUM(realized_pnl_usd), 0.0) AS pnl
            FROM trades
            WHERE status='CLOSED' AND exit_ts_utc LIKE ?
            """,
            (f"{day_prefix}%",),
        )
        return float(row["pnl"]) if row is not None else 0.0

