# ES–SPY Cash-and-Carry Arbitrage Bot (Cursor Project)

## What this is
This is a **Python 3.10+** trading bot that monitors **CME E-mini S&P 500 futures (ES)** vs **SPDR S&P 500 ETF (SPY)** and trades a simplified **cash-and-carry / reverse cash-and-carry** signal when the futures price deviates from a theoretical fair value:

\[
F = S \times e^{(r-d)t}
\]

- \(S\): SPY price used as a proxy for the S&P 500 spot (converted to “index points” via `SPY_TO_INDEX_FACTOR`)
- \(r\): risk-free rate (optional daily refresh via FRED)
- \(d\): dividend yield (constant, optionally per-expiry)
- \(t\): time to futures expiration

**Important:** Using SPY as a spot proxy is approximate; `SPY_TO_INDEX_FACTOR` is not constant over time. This bot is intended for **research + paper trading first**.

## Project files
- `main.py`: live bot entrypoint (IBKR + ib_insync)
- `config.py`: environment-based settings
- `ib_connection.py`: IB connection + reconnect watchdog
- `market_data.py`: real-time top-of-book subscriptions + quote cache
- `fair_value.py`: carry model, FRED rate provider, dividend model helpers
- `arbitrage_bot.py`: strategy loop (signals, entry/exit, cooldown)
- `order_manager.py`: paired order execution + partial-fill hedging + fill logging
- `risk_manager.py`: position limits, stop-loss-on-spread, max hold days, daily loss limit
- `database.py`: SQLite persistence for trades/fills
- `backtest.py`: CSV-driven backtest (simple state machine)
- `requirements.txt`: dependencies
- `.env.example`: configuration template

## Setup
### 1) Install deps

```bash
python -m pip install -r requirements.txt
```

### 2) Configure environment
- Copy `.env.example` to `.env`
- Set:
  - **IB connection**: `IB_HOST`, `IB_PORT`, `IB_CLIENT_ID`
  - **Paper mode (default)**: keep `TRADING_ENABLED=0`
  - **FRED (optional)**: set `FRED_API_KEY` to pull `DTB3` daily

### 3) Interactive Brokers
- In TWS/IB Gateway enable: **API → “Enable ActiveX and Socket Clients”**
- Typical ports:
  - **Paper TWS**: `7497`
  - **Live TWS**: `7496`
- Ensure you’re entitled for **CME futures** and have market data subscriptions (or accept delayed / no quotes).

## Running (live feed; paper execution by default)

```bash
python main.py
```

- With `TRADING_ENABLED=0`, the bot logs signals and writes them to SQLite but **does not place orders**.
- To enable trading: set `TRADING_ENABLED=1` in `.env` (after paper testing).

## Kill switch (manual)
Two ways to immediately stop new entries (and force-close any open position):
- Set `KILL_SWITCH=1` in `.env` and restart
- Or create a file named `KILL_SWITCH` in the project folder while the bot is running

## Backtesting
Provide a CSV with columns:
- `ts` (timestamp parseable by pandas; UTC recommended)
- `es_bid`, `es_ask`
- `spy_bid`, `spy_ask`

Example:

```bash
python backtest.py --csv data.csv --expiry 2026-03-20 --r 0.045 --d 0.014 --entry_bps 20 --exit_bps 5 --cost_bps 4
```

## Practical notes / limitations
- **Hedge sizing**: shares are sized to match ES notional using `multiplier` and current SPY price; then rounded to whole shares.
- **Execution**: entry uses paired **IOC limit** orders; if one leg fills and the other doesn’t, the bot immediately hedges the remainder with a market order.
- **Shorting SPY**: requires borrow availability and permissions; IB may reject shorts.
- **Dividends**: modeled as a simple annual yield; production-grade implementations typically use a dividend schedule or implied dividends.
- **Fair value**: the model is intentionally simplified and does not include financing spreads, funding, borrow costs, tax, or microstructure effects.

## Data / storage
- SQLite file defaults to `arb_bot.sqlite3` (`DB_PATH`).
- Logs go to `logs/arb_bot.log`.

## Disclaimer
This is **not** financial advice. Use at your own risk. Test in paper trading and validate all assumptions, fees, and regulatory requirements for your setup.

