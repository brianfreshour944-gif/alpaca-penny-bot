# Alpaca Bot

Paper-first Alpaca trading bot. **PAPER by default** until you explicitly flip the
mode flag; no secrets are committed (`.env` is gitignored, `.env.example` is the
template to copy).

## Run

```powershell
pip install -r requirements.txt
```

1. Copy the template and paste a **test account** key + secret (and a streaming
   token only if you use the live data feed):

   ```powershell
   copy .env.example .env
   notepad .env   # paste ALPACA_API_KEY / ALPACA_SECRET_KEY, then save
   ```

2. Start paper (default) or explicit LIVE trading:

   ```powershell
   python -m server.server
   python -m server.server --order-mode live   # only when you mean it
   ```

## What the loop does

Each iteration (every 5s):

1. **Sync positions** from Alpaca (`list_positions`) — Alpaca is the source of
   truth, not a local cache; positions closed elsewhere disappear here too.
2. **Poll bars** — REST daily bars (last 40 days) for every watchlist symbol;
   the iex websocket stream (if a token is set) enriches intraday prices.
3. **Exits** — a held position sells on stop-loss (`STOP_LOSS_PCT` below entry)
   or take-profit (`TAKE_PROFIT_PCT` above entry). Bad/zero prices stand aside.
4. **Entries** — a watchlist symbol not held buys when the latest close is above
   its SMA(20), sized as `floor(MAX_ORDER_SIZE_USD / close)` shares (min 1,
   capped by `MAX_POSITION_QTY`).

All order placement is gated by the Alpaca market clock (`get_clock().is_open`);
position sync and bar polling run regardless so state stays fresh.

## Config

- `DEFAULT_ORDER_MODE` — `paper` (default) or `live`. Always starts paper unless
  you override by mode flag.
- `MAX_ORDER_SIZE_USD` — per-symbol notional budget (shares = budget / close).
- `STOP_LOSS_PCT` / `TAKE_PROFIT_PCT` — exit thresholds vs entry price.
- `MAX_POSITION_QTY` — hard cap on shares per symbol.
- `WATCHLIST` — comma-separated symbols to trade (default: AAPL,MSFT,GOOGL).

## Notes

- Data feed: REST daily bars use the live data host (`data.alpaca.markets`) with
  `feed='iex'` — Basic plans have no SIP access. The websocket stream uses the
  same iex feed and runs as a background task (never blocks the trading loop).
- Paper/live is enforced by the REST client's `base_url`; orders are only routed
  live when you set `DEFAULT_ORDER_MODE=live`.
- `.env` is ignored by git — never commit it.
- This is a research scaffold, not a final trading system — sanity-check every
  line before running against any account.
