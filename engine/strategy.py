"""Entry/exit strategy with real sizing, stop-loss and take-profit.

Signals are simple and deterministic on daily closes:
  entry : last close above a short SMA (momentum confirmation) and price known
  exit  : price fell below stop_loss_pct below entry, or rose above
          take_profit_pct above entry; close==0/bad data -> stand aside

Sizing: floor(max_order_size_usd / close) shares per symbol, minimum 1,
capped by max_position_qty. Symbols are cached as pending positions only
AFTER a successfully accepted order, never before (the previous bug entered
the cache before ordering, which permanently blocked re-entry).
"""
import math

from engine.db import TradeCache


class Strategy:
    def __init__(self, config, cache: TradeCache):
        self.config = config
        self.cache = cache
        self.stop_loss_pct = getattr(config, "stop_loss_pct", 0.08)
        self.take_profit_pct = getattr(config, "take_profit_pct", 0.15)

    # ---- data plumbing ----
    def set_last_bar(self, symbol: str, day: str, ts: float, close: float):
        self.cache.set_last_bar(symbol, day=day, ts=ts, close=close)

    def last_close(self, symbol: str):
        return self.cache.get_last_bar(symbol)

    def update_last_bar(self, symbols: list, feed=None):
        """Mirror the feed's full bar history into the cache.

        SMA(20) needs the last 20 daily closes; copying only the latest bar
        (the previous behaviour) left the SMA permanently None and the entry
        gate never passed. No bar available: do NOT synthesize a 0.0 close —
        leave the previous bars in place so entries/exits use real prices.
        """
        for symbol in symbols:
            if feed is None:
                continue
            for day, (close, ts) in feed.get_bars(symbol).items():
                self.set_last_bar(symbol, day=day, ts=ts, close=close)

    def bar_date(self):
        import datetime

        return datetime.datetime.utcnow().strftime("%d-%m-%Y")

    def server_time(self):
        import time

        return time.time()

    # ---- positions ----
    def get_position_size(self, symbol: str):
        """Return integer shares already held for this symbol (0 if none)."""
        pos = next((p for p in self.cache.all_positions() if p["symbol"] == symbol), None)
        if pos is None or pos.get("qty") is None:
            return 0
        return pos["qty"]

    def get_strategy_positions(self):
        """Return current positions from cache."""
        return self.cache.all_positions()

    # ---- sizing ----
    def quantity_for(self, symbol: str):
        """Shares per symbol: floor(max_order_size_usd / close), capped by
        max_position_qty. Returns 0 when no price is known or one share would
        exceed the budget, so the symbol is skipped instead of overspending."""
        close = self.last_close(symbol)
        cap = int(getattr(self.config, "max_position_qty", 1000))
        if not close or close <= 0:
            return 0
        budget = float(getattr(self.config, "max_order_size_usd", 25.0))
        qty = math.floor(budget / close)
        if qty <= 0:
            return 0
        return min(qty, cap)

    def next_order_qty(self, symbol: str, qty: int):
        return qty

    # ---- entries ----
    def should_enter(self, symbol: str):
        """Entry gate: not already held, real price known, close > SMA20."""
        if self.get_position_size(symbol) > 0:
            return False
        bar = self.cache.get_last_bar_full(symbol)
        if not bar or bar["close"] <= 0:
            return False
        sma = self.cache.sma(symbol, 20)
        if sma is None:
            return False
        return bar["close"] > sma

    def enter_one_symbol(self, symbol: str):
        """Return an order qty if the entry gate passes, else 0.

        NOTE: does NOT touch the position cache — the caller records the
        position only after the order is accepted by the broker.
        """
        if not self.should_enter(symbol):
            return 0
        qty = self.quantity_for(symbol)
        if qty <= 0:
            close = self.last_close(symbol) or 0.0
            budget = float(getattr(self.config, "max_order_size_usd", 25.0))
            print(f"[strategy] skip {symbol}: price {close} exceeds "
                  f"max order size {budget}")
        return qty

    # ---- exits ----
    def check_exit(self, symbol: str):
        """Return a reason string to exit this symbol, else None."""
        pos = next((p for p in self.cache.all_positions() if p["symbol"] == symbol), None)
        if not pos or pos.get("qty", 0) <= 0:
            return None
        close = self.last_close(symbol)
        if not close or close <= 0:
            return None  # stand aside on bad data, never panic-sell on zeros
        entry = pos.get("avg_entry_px") or 0
        if entry <= 0:
            return None
        if close <= entry * (1 - self.stop_loss_pct):
            return "stop_loss"
        if close >= entry * (1 + self.take_profit_pct):
            return "take_profit"
        return None

    def record_entry(self, symbol: str, qty: int, order_id: str = ""):
        """Cache a pending position after a successfully accepted order."""
        close = self.last_close(symbol) or 0.0
        self.cache.set_position(symbol, {
            "symbol": symbol,
            "qty": qty,
            "avg_entry_px": close,
            "current_px": close,
            "order_id": order_id,
            "source": "bot",
        })
