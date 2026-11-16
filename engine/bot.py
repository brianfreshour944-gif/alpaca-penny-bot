"""Async Alpaca bot orchestrator.

    Bot.load(config); bot = bot; await bot.start(); then await bot.stop()

Main loop per iteration:
  1. sync_positions()  — Alpaca is the source of truth for what is held
  2. poll bars         — REST daily bars (websocket enriches intraday)
  3. exits             — stop-loss / take-profit on held positions
  4. entries           — watchlist symbols not held, gated by SMA signal

All trading is gated by broker.market_hours(); positions sync and bar
polling run regardless so state stays fresh.
"""
import asyncio

DEFAULT_WATCHLIST = ["AAPL", "MSFT", "GOOGL"]  # fallback if WATCHLIST unset


class Bot:
    def __init__(self, config):
        self.config = config
        self.strategy = None
        self.broker = self._make_broker(config)
        self.feed = None
        self._data_task = None
        self._data_ws = None
        self._stop_requested = False

    def _make_broker(self, config):
        from engine import broker as broker_engine
        return broker_engine.Broker(config)

    def _make_feed(self, config):
        from engine import feed as feed_engine
        return feed_engine.DataFeed(config)

    def _make_strategy(self, config, cache):
        from engine import strategy as strategy_engine
        return strategy_engine.Strategy(config, cache)

    @classmethod
    def load(cls, config):
        return cls(config)

    @property
    def symbols(self):
        return self.config.watchlist or DEFAULT_WATCHLIST

    async def start(self):
        """Initialize and start the bot components."""
        from engine.db import TradeCache

        cache = TradeCache()
        self.feed = self._make_feed(self.config)
        self.strategy = self._make_strategy(self.config, cache)
        self.broker.init_cache(cache)
        # Fire-and-forget stream consumer (returns immediately).
        await self.feed.connect()
        self._data_task = getattr(self.feed, "_stream_task", None)
        # Seed bars for the watchlist before the first loop pass.
        for s in self.symbols:
            self.feed.poll_rest_bars(s)
        self.strategy.update_last_bar(self.symbols, feed=self.feed)
        print(f"[bot] started: symbols={self.symbols} mode={self.config.order_mode}")

    async def stop(self):
        """Lifecycle stop (called by server on loop end). No-op if not running."""
        self._stop_requested = True
        if self.feed is not None:
            await self.feed.stop()
        if self._data_task:
            self._data_task.cancel()
            try:
                await self._data_task
            except asyncio.CancelledError:
                pass

    # ---- one trading iteration ----
    def iteration(self):
        """Run one sync -> exits -> entries pass. Returns a summary dict."""
        summary = {"synced": 0, "exits": [], "entries": []}
        summary["synced"] = len(self.broker.sync_positions())
        for s in self.symbols:
            self.feed.poll_rest_bars(s)
        self.strategy.update_last_bar(self.symbols, feed=self.feed)

        if not self.broker.market_hours():
            return summary  # outside the session: observe only

        # Exits first (risk before new exposure).
        for pos in self.strategy.get_strategy_positions():
            symbol = pos["symbol"]
            reason = self.strategy.check_exit(symbol)
            if reason:
                qty = self.strategy.get_position_size(symbol)
                resp = self.broker.trade([symbol], qty, side="sell")
                if resp:
                    print(f"[loop] SELL {symbol} qty={qty} ({reason})")
                    self.strategy.cache.del_position(symbol)
                    summary["exits"].append((symbol, reason))

        # Entries: not already held and signal passes.
        for symbol in self.symbols:
            if self.strategy.get_position_size(symbol) > 0:
                continue
            qty = self.strategy.enter_one_symbol(symbol)
            if qty > 0:
                print(f"[loop] placing market buy: symbol={symbol} qty={qty}")
                resp = self.broker.trade([symbol], qty, side="buy")
                if resp:
                    order_id = resp["orders"].get(symbol, {}).get("id", "")
                    self.strategy.record_entry(symbol, qty, order_id=order_id)
                    print(f"[loop] order placed: {resp}")
                    summary["entries"].append(symbol)
        return summary


async def run_until_server_stop(bot):
    """Wire the data stream and main loop; run until the user stops the server."""
    try:
        await bot.start()
        consecutive_errors = 0
        while not bot._stop_requested:
            try:
                bot.iteration()
                consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
                delay = min(60, 5 * consecutive_errors)
                print(f"[loop] error #{consecutive_errors} ({type(e).__name__}: {e}) — "
                      f"backing off {delay}s")
            await asyncio.sleep(5)
    finally:
        await bot.stop()
