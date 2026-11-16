"""Simple trade cache (in-memory + JSONL replay).

No external DB required on paper. Positions + open orders are held in memory
and appended to data/trades.jsonl for replay/inspection. Daily bars are also
held in memory per symbol/day and power the SMA/entry/exit signals.
"""
import json
import os
import threading
from datetime import UTC, datetime


def _cache_path() -> str:
    root = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(root, os.pardir, "data", "trades.jsonl")


class TradeCache:
    """Minimal trade ledger.

    Attributes
    ----------
    positions: list[dict]  — one open leg per symbol
    open_orders: dict[str, dict]
    trades: list[dict]  — historical executions, JSONL appended
    bars: dict[symbol, dict[day, (close, ts)]]  — daily closes for signals
    """

    def __init__(self):
        self.positions = []
        self.open_orders = {}
        self.trades = []
        self._bars = {}  # symbol -> {day: (close, ts)}
        self._lock = threading.Lock()

    # ---- positions ----
    def upsert_position(self, leg: dict) -> None:
        with self._lock:
            self.positions = [p for p in self.positions if p["symbol"] != leg["symbol"]]
            self.positions.append(leg)

    def set_position(self, symbol: str, leg: dict) -> None:
        with self._lock:
            self.positions = [
                p for p in self.positions if p["symbol"] != symbol
            ] + [leg]

    def del_position(self, symbol: str) -> None:
        with self._lock:
            self.positions = [p for p in self.positions if p["symbol"] != symbol]

    def all_positions(self):
        with self._lock:
            return list(self.positions)

    # ---- open orders ----
    def set_open_orders(self, mapping: dict):
        with self._lock:
            self.open_orders = dict(mapping)

    def get_open_order(self, symbol: str):
        return self.open_orders.get(symbol)

    def delete_open_order(self, symbol: str) -> None:
        with self._lock:
            self.open_orders.pop(symbol, None)

    def all_open_orders(self):
        with self._lock:
            return dict(self.open_orders)

    # ---- trade ledger ----
    def record_trade(self, record: dict) -> None:
        record["ts"] = record.get("ts", datetime.now(UTC).isoformat())
        with self._lock:
            self.trades.append(record)
            path = _cache_path()
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record) + "\n")

    def all_trades(self):
        with self._lock:
            return list(self.trades)

    def records(self):
        with self._lock:
            return {
                "positions": list(self.positions),
                "open_orders": dict(self.open_orders),
                "trades": list(self.trades),
            }

    # ---- bars / signals ----
    def set_last_bar(self, symbol: str, day: str, ts: float, close: float):
        """Store the latest bar (close, ts) for a symbol/day."""
        with self._lock:
            self._bars.setdefault(symbol, {})[day] = (float(close), float(ts))

    def get_last_bar(self, symbol: str):
        """Latest close price for the symbol, or None."""
        days = self._bars.get(symbol)
        if not days:
            return None
        return list(days.values())[-1][0]

    def get_last_bar_full(self, symbol: str):
        """Latest bar as {'close': float, 'ts': float, 'day': str}, or None."""
        days = self._bars.get(symbol)
        if not days:
            return None
        day = list(days.keys())[-1]
        close, ts = days[day]
        return {"day": day, "ts": ts, "close": close}

    def sma(self, symbol: str, window: int = 20):
        """Simple moving average of the most recent `window` closes, or None."""
        days = self._bars.get(symbol)
        if not days:
            return None
        closes = [c for _, (c, _) in sorted(days.items())][-window:]
        if len(closes) < window:
            return None
        return sum(closes) / len(closes)
