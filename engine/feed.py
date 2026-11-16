"""Alpaca data feed: daily bars (REST) + live trades/quotes (websocket).

Design:
  - REST polling: always enabled; fetches the last N daily bars per symbol
    (enough for SMA(20)). Uses the LIVE data host with feed='iex' — market
    data auth is separate from trading auth, and Basic plans have no SIP.
  - Websocket: runs as a fire-and-forget asyncio task (connect() returns
    immediately; a previous version awaited the stream inline inside connect()
    and blocked the whole bot). Parses Alpaca's real format
    {"stream": "...", "data": {...}} — the old parser expected keys
    ("u", "t", "a", ...) that do not exist in Alpaca's stream.
"""
import asyncio
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import websockets

from engine.config import BotConfig

DATA_HOST = "https://data.alpaca.markets"
STREAM_HOST = "wss://stream.data.alpaca.markets/v2/iex"


class DataFeed:
    def __init__(self, config: BotConfig):
        self.config = config
        self._bars = defaultdict(dict)    # symbol -> {day: (close, ts)}
        self._quotation = defaultdict(lambda: (0, 0))  # symbol -> (last, ts)
        self._live = False

    # ---- public API consumed by the strategy ----
    def get_daily_bar(self, symbol: str, day: str):
        return self._bars[symbol].get(day)

    def last_bar(self, symbol: str):
        return list(self._bars[symbol].values())[-1] if self._bars[symbol] else None

    def get_bars(self, symbol: str):
        """Full day->(close, ts) history for the symbol (copy)."""
        return dict(self._bars.get(symbol, {}))

    def get_quotation(self, symbol: str):
        return dict(zip(("last", "ts"), self._quotation[symbol]))

    def set_live(self, value: bool):
        self._live = value

    def is_live(self):
        return self._live

    # ---- REST fallback (no streaming token needed) ----
    def poll_rest_bars(self, symbol: str, days: int = 40):
        """Fetch recent daily bars via alpaca-trade-api REST and store them.

        Uses the LIVE data host for BOTH paper and live keys (market data auth
        is separate from trading auth; sandbox data has no IEX feed for Basic
        plans). feed='iex' is required for Basic plans.
        """
        try:
            import os as _os

            from alpaca_trade_api import REST

            _prev = _os.environ.get("APCA_API_DATA_URL")
            _os.environ["APCA_API_DATA_URL"] = DATA_HOST
            try:
                client = REST(
                    key_id=self.config.api_key,
                    secret_key=self.config.secret_key,
                    base_url=DATA_HOST,
                )
                bars = client.get_bars(
                    symbol,
                    timeframe="1Day",
                    start=(datetime.now(timezone.utc) - timedelta(days=days)).isoformat(),
                    limit=days,
                    feed="iex",
                )
            finally:
                if _prev is None:
                    _os.environ.pop("APCA_API_DATA_URL", None)
                else:
                    _os.environ["APCA_API_DATA_URL"] = _prev
            if not bars:
                return False
            for bar in bars:
                close = float(bar.c if hasattr(bar, "c") else bar["c"])
                ts = float(bar.t.timestamp() if hasattr(bar.t, "timestamp") else bar.t)
                day = datetime.fromtimestamp(ts).strftime("%d-%m-%Y")
                self._bars[symbol][day] = (round(close, 4), ts)
            self._quotation[symbol] = self._bars[symbol][day]
            return True
        except Exception as e:
            print(f"[feed] REST poll failed for {symbol}: {type(e).__name__}: {e}")
            return False

    # ---- websocket (fire-and-forget task) ----
    async def connect(self) -> None:
        """Start the stream consumer as a background task; returns immediately."""
        if not self.config.stream_token:
            print("[feed] no streaming token set — using REST polling only.")
            return
        self._stream_task = asyncio.ensure_future(self._stream_loop())
        print("[feed] websocket task started (iex stream).")

    async def _stream_loop(self):
        auth = {"action": "auth", "key": self.config.api_key, "secret": self.config.stream_token}
        sub = {"action": "subscribe", "trades": [], "quotes": [], "bars": ["*"]}
        while True:
            try:
                async with websockets.connect(STREAM_HOST, open_timeout=30, close_timeout=10) as ws:
                    await ws.send(json.dumps(auth))
                    await ws.send(json.dumps(sub))
                    self._live = True
                    async for raw in ws:
                        self._on_message(raw)
            except Exception as e:
                print(f"[feed] stream error: {type(e).__name__}: {e} — reconnecting in 10s")
            self._live = False
            await asyncio.sleep(10)

    def _on_message(self, raw):
        try:
            msg = json.loads(raw)
        except Exception:
            return
        stream = msg.get("stream", "")
        data = msg.get("data", {})
        if stream == "error":
            print(f"[feed] stream error message: {data}")
            return
        if stream in ("trade_updates", "status_updates"):
            return
        if stream.startswith(("b.", "t.", "q.")):
            symbol = data.get("S")
            if not symbol:
                return
            if stream.startswith("b."):  # bar: {S, o, h, l, c, v, t}
                ts = float(data.get("t", 0)) / 1000.0
                day = datetime.fromtimestamp(ts).strftime("%d-%m-%Y")
                self._bars[symbol][day] = (round(float(data.get("c", 0)), 4), ts)
                self._quotation[symbol] = (round(float(data.get("c", 0)), 4), ts)
            elif stream.startswith("t."):  # trade: {S, p, t}
                px, ts = float(data.get("p", 0)), float(data.get("t", 0)) / 1000.0
                self._quotation[symbol] = (px, ts)
            elif stream.startswith("q."):  # quote: {S, bp, ap}
                self._quotation[symbol] = (
                    float(data.get("ap", 0) or self._quotation[symbol][0]),
                    float(data.get("t", 0)) / 1000.0,
                )

    async def stop(self):
        task = getattr(self, "_stream_task", None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
