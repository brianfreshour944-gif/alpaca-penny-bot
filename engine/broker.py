"""Alpaca order broker wrapper: paper/live guardrails + activity guard.

Paper/live selection is enforced by alpaca-trade-api via REST(base_url=...):
the paper client talks to https://paper-api.alpaca.markets, the live client
to https://api.alpaca.markets. NEVER pass paper=True to submit_order() —
alpaca-trade-api 3.x does not accept that kwarg (TypeError crash seen in
Coolify, 2026-11-07).
"""
import time
import threading
from datetime import datetime, timezone, timedelta

from alpaca_trade_api import REST
from dotenv import load_dotenv

from engine.db import TradeCache

load_dotenv()
class Broker:
    def __init__(self, config):
        self.config = config
        self.config.cache = None
        self.alpha_client = REST(
            key_id=config.api_key,
            secret_key=config.secret_key,
            base_url=config.base_url,
        )
        self._cache = None
        self._last_activity = time.time()
        self._start_time = time.time()
        self._lock = threading.Lock()

    def init_cache(self, cache):
        with self._lock:
            self._cache = cache
            self.config.cache = cache
        return cache

    def market_hours(self):
        server_tz = timezone(timedelta(hours=int(self.alpha_client.timezone.get_offset())))
        now = datetime.now(server_tz)
        # US regular session: Mon-Fri, 9:30-16:00 ET. (Previously `00 <= hour <= 23`
        # which was always true — every hour of every weekday passed.)
        return now.weekday() < 5 and (9, 30) <= (now.hour, now.minute) < (16, 0)

    def trade(self, symbols, qty):
        self._activity_guard()
        if qty <= 0:
            return None
        # max_order_size_usd is a shares cap (despite the name); qty above it
        # was previously silently rejected via `qty > max` — now clamp to the cap
        # so a large signal doesn't zero the order.
        qty = min(qty, float(self.config.max_order_size_usd))
        if symbols is None or len(symbols) == 0:
            return None
        if self.config.cache is None:
            self.init_cache(TradeCache())
        
        orders = {}
        for s in symbols:
            orders[s] = {
                "order_type": "market",
                "side": "buy", "qty": qty}
        
        resp = self.mplace(self.config.order_mode, orders)

        # Alpaca REST returns Order objects, not dicts. Normalize to the
        # dict shape trade() expects, preserving the order id when present.
        if resp is None or not hasattr(resp, "status"):
            resp = {"status": "ACCEPTED" if resp else "REJECTED",
                    "orders": {s: {"id": str(resp) if resp else ""} for s in symbols},
                    "cost": None}
        else:
            # Order object (or dict-like) from alpaca-trade-api
            status = getattr(resp, "status", "new") or "new"
            order_id = getattr(resp, "id", str(resp))
            resp = {"status": "ACCEPTED" if status in ("new", "accepted", "ACCEPTED") else status,
                    "orders": {s: {"id": str(order_id), "status": status} for s in symbols},
                    "cost": None}

        if resp.get("status") == "ACCEPTED":
            for s in symbols:
                self._cache.record_trade({
                    "type": self.config.order_mode,
                    "event": "TRADE", "ts": datetime.utcnow().isoformat(timespec="seconds"),
                    "acct": self.config.order_mode,
                    "order_id": resp["orders"][s]["id"],
                    "qty": qty, "symbol": s,
                    "cost": resp["orders"][s].get("cost"),
                    "avg_px": resp["orders"][s].get("avg_filled_px"),
                    "filled_qty": resp["orders"][s].get("filled_qty"),
                    "filled_px": resp["orders"][s].get("filled_px"),
                    "open_qty": resp["orders"][s].get("qty"),
                    "cum_qty": resp["orders"][s].get("filled_qty"),
                    "total": resp["cost"],
                    "status": resp["orders"][s].get("status"),
                    "last_filled_qty": resp["orders"][s].get("filled_qty"),
                    "last_filled_px": resp["orders"][s].get("filled_px"),
                    "cost": resp["orders"][s].get("cost"),
                })
            return resp
        print("FAILED order:", resp)
        return None

    def mplace(self, mode, orders):
        # Validate orders structure
        if not orders or not isinstance(orders, dict):
            raise ValueError("Invalid orders structure")

        # Paper/live is determined by the REST client's base_url (set from
        # config at construction). Do NOT pass paper=True — alpaca-trade-api
        # 3.x submit_order() has no such parameter.
        # Paper/live is determined by the REST client's base_url (set from
        # config at construction). Do NOT pass paper=True — alpaca-trade-api
        # 3.x submit_order() has no such parameter.
        last_resp = None
        for symbol, order in orders.items():
            last_resp = self.alpha_client.submit_order(
                symbol=symbol,
                side=order.get("side", "buy"),
                type=order.get("order_type", "market"),
                qty=str(order.get("qty", "1")),
                time_in_force="day",
            )
        return last_resp

    def _activity_guard(self):
        if time.time() - self._last_activity > 120:
            raise TimeoutError("broker activity timeout")
        self._last_activity = time.time()