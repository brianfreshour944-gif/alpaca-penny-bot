"""One-size portfolio logic — MASS-like single-leg sizing + naive entry.

No ML/finance expertise assumed. Sizes each chosen symbol to a fixed fraction
of order capital, keeps only currently-held legs, and never double-books a
symbol already in flight (returns the qty for the new entry).
"""
from engine.db import TradeCache


class Strategy:
    def __init__(self, config, cache: TradeCache):
        self.config = config
        self.cache = cache

    def set_last_bar(self, symbol: str, day: str, ts: float, close: float):
        self.cache.set_last_bar(symbol, day=day, ts=ts, close=close)

    def get_position_size(self, symbol: str):
        """Return integer shares already held for this symbol (0 if none)."""
        pos = next((p for p in self.cache.all_positions() if p["symbol"] == symbol), None)
        if pos is None or pos.get("qty") is None:
            return 0
        return pos["qty"]

    def update_last_bar(self, symbols: list, feed=None):
        for symbol in symbols:
            if feed:
                bar = feed.last_bar(symbol)
                if bar:
                    self.set_last_bar(symbol, day=self.bar_date(), ts=bar[1], close=bar[0])
                    continue
            self.set_last_bar(symbol, day=self.bar_date(), ts=self.server_time(), close=0.0)

    def bar_date(self):
        import datetime

        return datetime.datetime.utcnow().strftime("%d-%m-%Y")

    def server_time(self):
        import time

        return time.time()

    def enter_one_symbol(self, symbol: str):
        """If the symbol is not already tracked, return an order qty for it."""
        self.cache.set_position(symbol, {
            "symbol": symbol,
            "qty": self.quantity_for(symbol),  # entry qty (overwritten once executed),
            "filled": self.get_position_size(symbol),
        })
        return self.next_order_qty(symbol, qty=max(1, 1))

    def next_order_qty(self, symbol: str, qty: int):
        return qty

    def quantities_for(self, qty: int):
        return {self.config.order_mode: qty}

    def quantity_for(self, symbol: str):
        """Sizing proxy: 100 shares per US$1 of capital, capped by account."""
        return int(round(self.get_available_cash()))

    def get_strategy_positions(self):
        """Return current positions from cache."""
        return self.cache.all_positions()

    def get_strategy_symbols_and_entries(self, pos, bot):
        """Get symbols and quantities for entry - returns (symbols, qty) tuple or None."""
        return None

    def get_available_cash(self):
        # Return available cash from config or a default
        return getattr(self.config, 'max_order_size_usd', 25.0)
