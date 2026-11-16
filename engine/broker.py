"""Alpaca order broker wrapper: buy/sell, position sync, paper/live guardrails.

Paper/live selection is enforced by alpaca-trade-api via REST(base_url=...):
the paper client talks to https://paper-api.alpaca.markets, the live client
to https://api.alpaca.markets. NEVER pass paper=True to submit_order() —
alpaca-trade-api 3.x does not accept that kwarg (TypeError crash seen in
Coolify, 2026-11-07).
"""
import threading
from datetime import datetime

from alpaca_trade_api import REST
from dotenv import load_dotenv

from engine.db import TradeCache

load_dotenv()


class Broker:
    def __init__(self, config):
        self.config = config
        self.config.cache = None
        self.client = REST(
            key_id=config.api_key,
            secret_key=config.secret_key,
            base_url=config.base_url,
        )
        self._cache = None
        self._lock = threading.Lock()

    def init_cache(self, cache):
        with self._lock:
            self._cache = cache
            self.config.cache = cache
        return cache

    # ---- market clock ----
    def market_hours(self):
        """True during US regular session (source of truth: Alpaca clock)."""
        try:
            clock = self.client.get_clock()
            return bool(getattr(clock, "is_open", False))
        except Exception:
            # Fallback: ET weekday/9:30-16:00 heuristic when the clock API
            # is unreachable (offline tests, degraded network).
            from datetime import timedelta, timezone
            now = datetime.now(timezone(timedelta(hours=-5)))
            return now.weekday() < 5 and (9, 30) <= (now.hour, now.minute) < (16, 0)

    # ---- positions ----
    def sync_positions(self):
        """Pull live Alpaca positions into the cache (source of truth)."""
        cache = self._cache or TradeCache()
        try:
            positions = self.client.list_positions()
        except Exception as e:
            print(f"[broker] position sync failed: {type(e).__name__}: {e}")
            return []
        synced = []
        for p in positions:
            leg = {
                "symbol": p.symbol,
                "qty": int(float(p.qty)),
                "avg_entry_px": float(p.avg_entry_price),
                "current_px": float(p.current_price or 0),
                "market_value": float(p.market_value or 0),
                "unrealized_pl": float(p.unrealized_pl or 0),
                "source": "alpaca",
            }
            cache.set_position(p.symbol, leg)
            synced.append(leg)
        # Drop cached legs that no longer exist at Alpaca (closed elsewhere).
        live_symbols = {p.symbol for p in positions}
        for cached in cache.all_positions():
            if cached["symbol"] not in live_symbols:
                cache.del_position(cached["symbol"])
        return synced

    # ---- orders ----
    def trade(self, symbols, qty, side="buy"):
        """Place market order(s). side is 'buy' or 'sell'; returns the
        normalized response dict on acceptance, else None."""
        if qty <= 0:
            return None
        if symbols is None or len(symbols) == 0:
            return None
        if side not in ("buy", "sell"):
            raise ValueError(f"invalid side: {side!r}")
        if self.config.cache is None:
            self.init_cache(TradeCache())

        orders = {
            s: {"order_type": "market", "side": side, "qty": qty}
            for s in symbols
        }
        resp = self.mplace(self.config.order_mode, orders)

        # Alpaca REST returns Order objects, not dicts. Normalize to the
        # dict shape callers expect, preserving the order id when present.
        if resp is None or not hasattr(resp, "status"):
            resp = {"status": "ACCEPTED" if resp else "REJECTED",
                    "side": side,
                    "orders": {s: {"id": str(resp) if resp else ""} for s in symbols},
                    "cost": None}
        else:
            status = getattr(resp, "status", "new") or "new"
            order_id = getattr(resp, "id", str(resp))
            # 'pending_new' is a valid accepted state (order queued at the
            # exchange); treating it as failure caused duplicate orders.
            accepted = str(status).lower() in (
                "new", "accepted", "pending_new", "pending_replace")
            resp = {"status": "ACCEPTED" if accepted else status,
                    "side": side,
                    "orders": {s: {"id": str(order_id), "status": status} for s in symbols},
                    "cost": None}

        if resp.get("status") == "ACCEPTED":
            for s in symbols:
                self._cache.record_trade({
                    "type": self.config.order_mode,
                    "event": "TRADE",
                    "ts": datetime.utcnow().isoformat(timespec="seconds"),
                    "acct": self.config.order_mode,
                    "order_id": resp["orders"][s]["id"],
                    "side": side, "qty": qty, "symbol": s,
                    "cost": resp["orders"][s].get("cost"),
                    "avg_px": resp["orders"][s].get("avg_filled_px"),
                    "filled_qty": resp["orders"][s].get("filled_qty"),
                    "filled_px": resp["orders"][s].get("filled_px"),
                    "open_qty": resp["orders"][s].get("qty"),
                    "cum_qty": resp["orders"][s].get("filled_qty"),
                    "total": resp["cost"],
                    "status": resp["orders"][s].get("status"),
                })
            return resp
        print("FAILED order:", resp)
        return None

    def mplace(self, mode, orders):
        # Validate orders structure
        if not orders or not isinstance(orders, dict):
            raise ValueError("Invalid orders structure")

        last_resp = None
        for symbol, order in orders.items():
            last_resp = self.client.submit_order(
                symbol=symbol,
                side=order.get("side", "buy"),
                type=order.get("order_type", "market"),
                qty=str(order.get("qty", "1")),
                time_in_force="day",
            )
        return last_resp
