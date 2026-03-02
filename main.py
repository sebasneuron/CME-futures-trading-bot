from __future__ import annotations

import asyncio

from config import load_settings
from database import Database
from fair_value import DividendYieldProvider, RiskFreeRateProvider
from ib_connection import IBConnection, IBConnectionParams
from logger import setup_logging


async def amain() -> None:
    # Delay importing ib_insync-dependent modules until *after* an event loop exists.
    # This avoids Python 3.14+ import-time failures where some libs call get_event_loop().
    from arbitrage_bot import ArbitrageBot
    from market_data import MarketData
    from order_manager import OrderManager
    from risk_manager import RiskManager

    cfg = load_settings()
    log = setup_logging(cfg.log_level)

    db = Database(cfg.db_path)
    rf = RiskFreeRateProvider(
        logger=log,
        fred_api_key=cfg.fred_api_key,
        fred_series_id=cfg.fred_series_id,
        fallback_rate_annual=cfg.risk_free_rate_annual,
    )
    div = DividendYieldProvider(default_dividend_yield_annual=cfg.dividend_yield_annual)
    risk = RiskManager(
        db=db,
        max_fut_contracts=cfg.max_fut_contracts,
        daily_loss_limit_usd=cfg.daily_loss_limit_usd,
        stop_loss_bps=cfg.stop_loss_bps,
        max_hold_days=cfg.max_hold_days,
    )

    ibc = IBConnection(IBConnectionParams(cfg.ib_host, cfg.ib_port, cfg.ib_client_id), log)
    await ibc.ensure_connected()

    md = MarketData(ibc.ib, log)
    om = OrderManager(ibc.ib, db, log)

    if cfg.fut_use_front_month:
        fut = await md.resolve_front_future(
            symbol=cfg.fut_symbol,
            exchange=cfg.fut_exchange,
            currency=cfg.fut_currency,
            front_month_offset=cfg.fut_front_month_offset,
        )
    else:
        # If you want a specific expiry, set FUT_USE_FRONT_MONTH=0 and hardcode below.
        fut = await md.resolve_front_future(
            symbol=cfg.fut_symbol,
            exchange=cfg.fut_exchange,
            currency=cfg.fut_currency,
            front_month_offset=cfg.fut_front_month_offset,
        )

    spy = await md.resolve_spy(symbol=cfg.spy_symbol, exchange=cfg.spy_exchange, currency=cfg.spy_currency)

    bot = ArbitrageBot(
        md=md,
        om=om,
        db=db,
        risk=risk,
        rf=rf,
        div=div,
        logger=log,
        trading_enabled=cfg.trading_enabled,
        kill_switch=cfg.kill_switch,
        spy_to_index_factor=cfg.spy_to_index_factor,
        entry_threshold_bps=cfg.entry_threshold_bps,
        exit_threshold_bps=cfg.exit_threshold_bps,
        estimated_cost_bps=cfg.estimated_cost_bps,
        cooldown_seconds=cfg.cooldown_seconds,
        hedge_timeout_seconds=cfg.hedge_timeout_seconds,
    )
    await bot.initialize(fut=fut, spy=spy)

    watchdog = asyncio.create_task(ibc.run_watchdog())
    try:
        await bot.run()
    finally:
        watchdog.cancel()
        try:
            await watchdog
        except Exception:
            pass
        await ibc.disconnect()
        db.close()


if __name__ == "__main__":
    # On Windows + newer Python versions, some dependencies expect a current event loop
    # to exist at import time. We create and run our own loop for compatibility.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(amain())
    except KeyboardInterrupt:
        pass
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        asyncio.set_event_loop(None)
        loop.close()

