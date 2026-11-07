"""Async Alpaca data feed: daily bars + live quote updates.

Two independent consumers subscribe to bars/quotes on demand:

    bars  : get_daily_bar / last_bar(symbol)       -> latest close/timestamp per day
    ticks : get_quotation(symbol, "last") / "size" -> realtime last size/timestamp (no auth)

No pub/sub event callbacks; state is read via the getters above.
"""
from collections import defaultdict

import websockets

from engine.config import BotConfig


class DataFeed:
    def __init__(self, config: BotConfig):
        self.config = config
        self._bars = defaultdict(dict)    # symbol -> {day: (close, ts)}
        self._quotation = defaultdict(lambda: (0, 0))  # symbol -> (last, ts)
        self._live = False
        self._rest_fallback_enabled = False

    # ---- public API consumed by subscribers ----
    def get_daily_bar(self, symbol: str, day: str):
        return self._bars[symbol].get(day)

    def last_bar(self, symbol: str):
        return list(self._bars[symbol].values())[-1] if self._bars[symbol] else None

    def get_quotation(self, symbol: str):
        return dict(self._quotation[symbol])

    def set_live(self, value: bool):
        self._live = value

    def is_live(self):
        return self._live

    # ---- REST fallback (no streaming token needed) ----
    def poll_rest_bars(self, symbol: str):
        """Fetch the latest daily bar via alpaca-trade-api REST and store it.

        Uses the same alpaca_trade_api client credentials as the broker
        (paper or live via base_url). Returns True if a bar was stored.
        """
        if not self._rest_fallback_enabled:
            return False
        try:
            from alpaca_trade_api import REST
            from datetime import datetime, timedelta

            # ROOT CAUSE (confirmed 2026-11-07 via source inspection):
            # alpaca_trade_api.REST.data_get() calls get_data_url() which
            # reads APCA_API_DATA_URL env var, defaulting to
            # data.alpaca.markets (live). Paper keys get 401 there.
            # base_url= does NOT affect data calls.
            #
            # Fix: set APCA_API_DATA_URL env var for paper mode so
            # get_data_url() returns the sandbox host.
            if self.config.order_mode == "paper":
                data_base = "https://data.sandbox.alpaca.markets"
            else:
                data_base = "https://data.alpaca.markets"
            print(f"[feed] REST using {data_base} for {symbol} (mode={self.config.order_mode})")

            import os as _os
            _prev_data_url = _os.environ.get("APCA_API_DATA_URL")
            _os.environ["APCA_API_DATA_URL"] = data_base
            try:
                client = REST(
                    key_id=self.config.api_key,
                    secret_key=self.config.secret_key,
                    base_url=data_base,
                )
            finally:
                if _prev_data_url is None:
                    _os.environ.pop("APCA_API_DATA_URL", None)
                else:
                    _os.environ["APCA_API_DATA_URL"] = _prev_data_url
            # Env var must be set during get_bars too (get_data_url
            # reads it at call time, not construction time).
            import os as _os2
            _prev2 = _os2.environ.get("APCA_API_DATA_URL")
            _os2.environ["APCA_API_DATA_URL"] = data_base
            try:
                # Last 5 days of daily bars to get the most recent close
                bars = client.get_bars(
                    symbol,
                    timeframe="1Day",
                    start=(datetime.utcnow() - timedelta(days=5)).isoformat(),
                    limit=1,
                )
            finally:
                if _prev2 is None:
                    _os2.environ.pop("APCA_API_DATA_URL", None)
                else:
                    _os2.environ["APCA_API_DATA_URL"] = _prev2
            if not bars:
                print(f"[feed] REST: no bars returned for {symbol}")
                return False
            bar = bars[-1] if isinstance(bars, list) else bars
            close = float(bar.c if hasattr(bar, "c") else bar["c"])
            ts = float(bar.t.timestamp() if hasattr(bar.t, "timestamp") else bar["t"])
            day = datetime.fromtimestamp(ts).strftime("%d-%m-%Y")
            self._bars[symbol][day] = (round(close, 4), ts)
            self._quotation[symbol] = (round(close, 4), ts)
            return True
        except Exception as e:
            print(f"[feed] REST poll failed for {symbol}: {type(e).__name__}: {e}")
            return False

    # ---- async entry points ----
    async def set_last_bar(self, symbol: str, day: str, close: float, ts: float):
        self._bars[symbol][day] = (round(close, 4), ts)

    async def set_bid(self, symbol: str, bid: float) -> None:
        q = self._quotation[symbol]
        self._quotation[symbol] = (bid, q[1])

    async def set_ask(self, symbol: str, ask: float) -> None:
        q = self._quotation[symbol]
        self._quotation[symbol] = (q[0], ask)

    async def on_quote(self, symbol: str) -> None:
        self._quotation[symbol] = dict(self._quotation[symbol])

    async def connect(self) -> None:
        if not self.config.stream_token:
            # No websocket stream: fall back to REST polling so the bot
            # still gets real bars without a streaming token.
            self._rest_fallback_enabled = True
            print("[feed] no streaming token set — using REST polling instead.")
            return
        try:
            async with websockets.connect(
                "wss://data.stream.alpaca.markets",
                open_timeout=30,
                close_timeout=10,
            ) as ws:
                await self._handle_data(ws)
                self._live = True
        except Exception as e:
            print(f"[feed] stream connect failed: {e}")

    async def _handle_data(self, ws):
        import json

        async for raw in ws:
            try:
                d = json.loads(raw)
            except Exception:
                continue
            if not d.get("u") or "t" not in d:
                continue
            tag = d["t"][0]
            tid = d["t"][1]
            ts = d["ts"] / 1000
            s = d["a"]["symbols"][0]["s"]
            if tag == "T":  # trade
                bid = float(d["b"][0]["bidPx"]) if d["b"] else self._quotation[s][0]
                ask = float(d["a"][0]["askPx"]) if d["a"] else self._quotation[s][1]
                # set_bid/skip
                await self.set_bid(s, bid)
                await self.set_ask(s, ask)
            elif tag == "c":  # quote (skip — set_bid handles last)
                pass
            elif tag == "b":  # correction (ignore)
                pass
