"""Async Alpaca bot orchestrator.

    Bot.load(config); bot = bot; await bot.start(); then await bot.stop()

paper/live is enforced by the Alpaca client itself (paper=...). Data stream and
trade loop share one async context; the ws closes cleanly on stop.
"""
import asyncio  # noqa: E402


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
        from engine import broker as broker_engine  # noqa: F401
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

    async def start(self):
        """Initialize and start the bot components."""
        from engine.db import TradeCache
        
        # Initialize cache
        cache = TradeCache()
        
        # Initialize feed
        self.feed = self._make_feed(self.config)
        
        # Initialize strategy with cache
        self.strategy = self._make_strategy(self.config, cache)
        
        # Initialize broker with cache
        self.broker.init_cache(cache)
        
        # Connect to data stream
        await self.feed.connect()
        
        # Start data handling task
        self._data_task = asyncio.ensure_future(self._handle_datastream())

    async def _handle_datastream(self):
        token = self.config.stream_token
        if not token:
            print("[feed] no streaming token — using REST polling instead.")
            return
        try:
            import websockets

            async with websockets.connect(
                "wss://data.stream.alpaca.markets", close_timeout=10, open_timeout=120
            ) as ws:
                self._data_ws = ws
                async for raw in ws:
                    print("[feed] ", raw[:200])
        except Exception as e:
            print("[feed] stream error:", e)
        finally:
            if self._data_task:
                self._data_task = None


    async def stop(self):
        """Lifecycle stop (called by server on loop end). No-op if not running."""
        self._stop_requested = True
        if self._data_ws is not None:
            try:
                await self._data_ws.close()
            except Exception:
                pass
            self._data_ws = None
        if self._data_task:
            self._data_task.cancel()
            try:
                await self._data_task
            except asyncio.CancelledError:
                pass


async def run_until_server_stop(bot):
    """Wire the data stream and main loop; run until the user stops the server."""
    try:
        await bot.start()
        
        # Update strategy with latest bars from feed
        if bot.strategy and bot.feed:
            symbols = ["AAPL", "MSFT", "GOOGL"]  # Default symbols to track
            bot.strategy.update_last_bar(symbols, feed=bot.feed)
            # No bars available (no stream token): stay idle rather than
            # trading on synthetic zero-price bars. Re-checked each loop
            # iteration so bars arriving later resume trading.
            if not any(bot.feed.last_bar(s) for s in symbols):
                print("[loop] no bars available (check stream token) — idling, no trading.")
            
        if bot.strategy is None:
            # No strategy wired yet: keep the server alive (idle) until an
            # explicit stop instead of exiting immediately. No trading occurs.
            print("[loop] no strategy wired — idling (no trading). "
                  "Press stop/Ctrl+C to shut down.")
            while not bot._stop_requested:
                await asyncio.sleep(5)
        
        # Main trading loop — exceptions are logged with backoff, never
        # tight-looped (a bad API response must not crash-loop the container;
        # Coolify marks it unhealthy after the 30s start period).
        consecutive_errors = 0
        while not bot._stop_requested:
            try:
                # Get positions from strategy
                if bot.strategy:
                    pos = bot.strategy.get_strategy_positions() if hasattr(bot.strategy, 'get_strategy_positions') else []
                else:
                    pos = []

                if not pos:
                    # Check for entry signals
                    if bot.strategy and bot.feed:
                        symbols = ["AAPL", "MSFT", "GOOGL"]
                        # Poll REST for fresh bars (fallback when no stream
                        # token — the websocket path populates _bars directly).
                        for s in symbols:
                            bot.feed.poll_rest_bars(s)
                        # Skip trading entirely when no bars are available —
                        # update_last_bar would otherwise synthesize close=0.0
                        # bars and trade on meaningless prices.
                        if not any(bot.feed.last_bar(s) for s in symbols):
                            await asyncio.sleep(5)
                            continue
                        bot.strategy.update_last_bar(symbols, feed=bot.feed)

                        # Try to enter positions
                        for symbol in symbols:
                            if bot.strategy.get_position_size(symbol) == 0:
                                qty = bot.strategy.enter_one_symbol(symbol)
                                if qty > 0:
                                    print(f"[loop] placing market buy: symbol={symbol} qty={qty}")
                                    resp = bot.broker.trade([symbol], qty)
                                    if resp:
                                        print(f"[loop] order placed: {resp}")
                consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
                delay = min(60, 5 * consecutive_errors)
                print(f"[loop] error #{consecutive_errors} ({type(e).__name__}: {e}) — "
                      f"backing off {delay}s")

            await asyncio.sleep(5)
    finally:
        await bot.stop()

