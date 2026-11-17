"""End-to-end test of the trading loop with a stubbed Alpaca REST client.

Stubs alpaca_trade_api.REST in sys.modules before engine modules import it,
so the full broker/strategy/feed/bot path runs without network access.
"""
import asyncio
import sys
import types
from datetime import UTC, datetime

import pytest


class _FakeClock:
    is_open = True


class _FakePosition:
    def __init__(self, symbol, qty, avg, cur):
        self.symbol, self.qty = symbol, str(qty)
        self.avg_entry_price, self.current_price = str(avg), str(cur)
        self.market_value, self.unrealized_pl = "0", "0"


class _FakeOrder:
    def __init__(self, **kw):
        self.id, self.status = kw.get("id", "fake-order-1"), kw.get("status", "new")
        self.symbol = kw.get("symbol", "PENY")
        self.side = kw.get("side", "buy")


def _install_rest_stub():
    """Install a stateful fake REST into sys.modules['alpaca_trade_api']."""
    mod = types.ModuleType("alpaca_trade_api")

    class FakeREST:
        instances: list = []  # noqa: RUF012

        def __init__(self, key_id=None, secret_key=None, base_url=None):
            self.key_id, self.secret_key, self.base_url = key_id, secret_key, base_url
            FakeREST.instances.append(self)
            self.submitted = []       # list of submit_order kwargs
            self.fake_positions = []  # current fake account positions
            self._pristine = sys.modules.get("alpaca_trade_api")

        def get_clock(self):
            return _FakeClock()

        def list_positions(self):
            return [p for p in self.fake_positions if p.qty != "0"]

        def submit_order(self, **kw):
            self.submitted.append(kw)
            # Simulate an immediate fill at the symbol's latest close
            # (real Alpaca reports avg_entry_price on the position).
            sym, side, q = kw["symbol"], kw["side"], int(float(kw["qty"]))
            px = _BARS[sym][-1].c
            held = {p.symbol: p for p in self.fake_positions}
            if side == "buy":
                if sym in held:
                    p = held[sym]
                    p.qty = str(int(p.qty) + q)
                else:
                    self.fake_positions.append(_FakePosition(sym, q, px, px))
            else:  # sell
                if sym in held:
                    p = held[sym]
                    p.qty = str(max(0, int(p.qty) - q))
            return _FakeOrder()

        def get_bars(self, symbol, **kw):
            return _BARS[kw.get("symbol", symbol)]

    mod.REST = FakeREST
    sys.modules["alpaca_trade_api"] = mod
    return FakeREST


# 40 daily bars, rising trend so SMA(20) < last close (entry signal passes).
_CLOSES = [round(2.0 + i * 0.01, 4) for i in range(40)]
# Bars carry a `c` (close) and `t` (timestamp) attribute each.
class _B:
    def __init__(self, c, t):
        self.c, self.t = c, t
_DUD_CLOSES = [round(4.0 - i * 0.02, 4) for i in range(40)]  # declining
_BARS = {
    "PENY": [_B(_CLOSES[i], datetime.fromtimestamp(1700000000 + i * 86400, tz=UTC)) for i in range(40)],
    "DUD":  [_B(_DUD_CLOSES[i], datetime.fromtimestamp(1700000000 + i * 86400, tz=UTC)) for i in range(40)],
}


def test_buy_then_sell_flow():
    from engine.bot import Bot
    _install_rest_stub()
    _install_rest_stub()
    _install_rest_stub()
    _install_rest_stub()
    from engine.config import BotConfig

    cfg = BotConfig(api_key="fake_key", secret_key="fake_secret",
                    order_mode="paper", max_order_size_usd=25.0,
                    stop_loss_pct=0.08, take_profit_pct=0.15)
    # Default watchlist (when WATCHLIST is unset) must be the legacy trio.
    from engine.bot import DEFAULT_WATCHLIST
    assert DEFAULT_WATCHLIST == ["AAPL", "MSFT", "GOOGL"]
    cfg.watchlist = ["PENY"]
    bot = Bot.load(cfg)
    asyncio.run(bot.start())
    bot.feed.poll_rest_bars = lambda s: False  # freeze prices for assertions

    # --- iteration 1: entry ---
    s1 = bot.iteration()
    rest = bot.broker.client
    assert len(rest.submitted) == 1, rest.submitted
    buy = rest.submitted[0]
    assert buy["symbol"] == "PENY" and buy["side"] == "buy"
    # Sizing: floor(25.0 / 2.39) = 10 shares.
    assert buy["qty"] == "10", buy["qty"]
    assert s1["entries"] == ["PENY"]

    # Position recorded in cache AFTER acceptance (not before).
    pos = bot.strategy.cache.get_last_bar_full("PENY")
    assert pos and pos["close"] > 0
    held = bot.strategy.get_position_size("PENY")
    assert held == 10

    # --- iteration 2: no change (held + no exit trigger) ---
    s2 = bot.iteration()
    assert len(rest.submitted) == 1, "should not re-enter or double-buy"
    assert s2["entries"] == []

    # --- iteration 3: price falls below stop-loss -> sell ---
    # drop last bar's close by 10% vs entry (entry was 2.39)
    last_day = list(bot.feed._bars["PENY"].keys())[-1]
    prev = bot.feed._bars["PENY"][last_day]
    bot.feed._bars["PENY"][last_day] = (round(prev[0] * 0.90, 4), prev[1])
    bot.strategy.update_last_bar(["PENY"], feed=bot.feed)
    s3 = bot.iteration()
    assert len(rest.submitted) == 2, rest.submitted
    sell = rest.submitted[1]
    assert sell["symbol"] == "PENY" and sell["side"] == "sell"
    assert sell["qty"] == "10"
    assert s3["exits"] == [("PENY", "stop_loss")]

    # --- iteration 4: nothing held, price recovered -> may re-enter ---
    # Position cache was cleared on the sell; restore the price.
    bot.feed._bars["PENY"][last_day] = prev
    bot.strategy.update_last_bar(["PENY"], feed=bot.feed)
    s4 = bot.iteration()
    assert len(rest.submitted) == 3
    assert rest.submitted[2]["side"] == "buy"
    assert s4["entries"] == ["PENY"]

    # --- trade ledger recorded every fill ---
    trades = bot.broker._cache.all_trades()
    assert len(trades) == 3
    assert trades[0]["side"] == "buy" and trades[1]["side"] == "sell"


def test_market_hours_gate_blocks_trading():
    from engine.bot import Bot
    from engine.config import BotConfig

    cfg = BotConfig(api_key="fake_key", secret_key="fake_secret", watchlist=["PENY"])
    bot = Bot.load(cfg)
    bot.config = cfg
    asyncio.run(bot.start())
    bot.broker.client.get_clock = lambda: _FakeClock() if False else type("C", (), {"is_open": False})()
    s = bot.iteration()
    assert s["entries"] == [] and s["exits"] == []
    assert bot.broker.client.submitted == [], "no orders outside market hours"


def test_position_sync_pulls_alpaca_state():
    from engine.bot import Bot
    from engine.config import BotConfig

    cfg = BotConfig(api_key="fake_key", secret_key="fake_secret", watchlist=["PENY"])
    bot = Bot.load(cfg)
    bot.config = cfg
    asyncio.run(bot.start())
    # Pre-seed a stale cache position that Alpaca no longer has.
    bot.strategy.cache.set_position("GONE", {"symbol": "GONE", "qty": 5})
    # And fake an Alpaca-held position in a symbol the bot is not watching.
    rest = bot.broker.client
    rest.fake_positions.append(_FakePosition("ALPA", 7, 1.50, 1.60))
    synced = bot.broker.sync_positions()
    symbols = {p["symbol"] for p in synced}
    assert symbols == {"ALPA"}
    assert bot.strategy.get_position_size("ALPA") == 7
    assert bot.strategy.get_position_size("GONE") == 0  # stale leg dropped


def test_entry_gate_needs_sma():
    from engine.bot import Bot
    from engine.config import BotConfig

    cfg = BotConfig(api_key="fake_key", secret_key="fake_secret", watchlist=["DUD"])
    bot = Bot.load(cfg)
    bot.config = cfg
    asyncio.run(bot.start())
    # Push the last close far BELOW the SMA(20) so the entry gate fails.
    last_day = list(bot.feed._bars["DUD"].keys())[-1]
    prev = bot.feed._bars["DUD"][last_day]
    bot.feed._bars["DUD"][last_day] = (0.5, prev[1])
    bot.strategy.update_last_bar(["DUD"], feed=bot.feed)
    s = bot.iteration()
    assert s["entries"] == []
    assert bot.broker.client.submitted == []


if __name__ == "__main__":
    sys.exit(0)


def test_sma_uses_newest_20_bars_across_months():
    """(a) Bars spanning two months: SMA must use the 20 most recent bars
    by timestamp, not by the %d-%m-%Y day string (which sorts by day first)."""
    from engine.db import TradeCache

    cache = TradeCache()
    # 25 bars: 5 in Jan (ts early) + 20 in Dec (ts later, inserted first so
    # insertion order is wrong too). Day strings like 05-01 sort BEFORE 20-12
    # numerically even though Jan is later in real time.
    for i in range(20):  # December, ts 1700000000..1700166400
        ts = 1700000000 + i * 86400
        day = datetime.fromtimestamp(ts).strftime("%d-%m-%Y")
        cache.set_last_bar("X", day=day, ts=ts, close=10.0)
    for i in range(5):   # January (later), ts 1704000000..1704129600
        ts = 1704000000 + i * 86400
        day = datetime.fromtimestamp(ts).strftime("%d-%m-%Y")
        cache.set_last_bar("X", day=day, ts=ts, close=50.0)
    sma = cache.sma("X", 20)
    # Newest 20 bars by ts = 15 Dec bars (10.0) + 5 Jan bars (50.0)
    expected = (15 * 10.0 + 5 * 50.0) / 20
    assert sma == expected, f"{sma} != {expected}"


def test_get_last_bar_picks_newest_by_ts_out_of_order():
    """(b) Insert bars out of order; get_last_bar/get_last_bar_full must
    return the bar with the largest ts, not dict insertion order."""
    from engine.db import TradeCache

    cache = TradeCache()
    late_ts = 1704100000.0
    early_ts = 1700000000.0
    cache.set_last_bar("Y", day="31-12-2023", ts=late_ts, close=111.0)
    cache.set_last_bar("Y", day="14-11-2023", ts=early_ts, close=22.0)
    # Inserted newest first; dict order would return 111.0 anyway. Now
    # reverse: insert oldest last and confirm the newest still wins.
    cache2 = TradeCache()
    cache2.set_last_bar("Y", day="14-11-2023", ts=early_ts, close=22.0)
    cache2.set_last_bar("Y", day="31-12-2023", ts=late_ts, close=111.0)
    assert cache2.get_last_bar("Y") == 111.0
    full = cache2.get_last_bar_full("Y")
    assert full["close"] == 111.0 and full["ts"] == late_ts
    assert cache.get_last_bar("Y") == 111.0


def test_stock_above_budget_returns_zero_and_no_order():
    """(c) Price above max_order_size_usd -> qty 0, no order placed."""
    _install_rest_stub()
    from engine.bot import Bot
    from engine.config import BotConfig

    cfg = BotConfig(api_key="fake_key", secret_key="fake_secret",
                    watchlist=["PENY"], max_order_size_usd=25.0)
    bot = Bot.load(cfg)
    asyncio.run(bot.start())
    bot.feed.poll_rest_bars = lambda s: False
    # Overwrite the last bar with a price above the $25 budget.
    days = bot.feed._bars["PENY"]
    last_day = max(days, key=lambda d: days[d][1])
    close, ts = days[last_day]
    days[last_day] = (round(close * 100, 4), ts)  # ~$239 > $25 budget
    bot.strategy.update_last_bar(["PENY"], feed=bot.feed)
    s = bot.iteration()
    assert s["entries"] == []
    assert bot.broker.client.submitted == [], "must not order above budget"


def test_stock_below_budget_gets_floor_shares():
    """(d) Price below budget -> floor(budget / price) shares."""
    _install_rest_stub()
    from engine.bot import Bot
    from engine.config import BotConfig

    cfg = BotConfig(api_key="fake_key", secret_key="fake_secret",
                    watchlist=["PENY"], max_order_size_usd=25.0)
    bot = Bot.load(cfg)
    asyncio.run(bot.start())
    bot.feed.poll_rest_bars = lambda s: False
    # Last close ~2.39 -> floor(25 / 2.39) = 10 shares.
    qty = bot.strategy.quantity_for("PENY")
    close = bot.strategy.last_close("PENY")
    assert qty == int(25.0 / close), f"{qty} != floor(25/{close})"
    assert qty == 10


def test_live_base_url_refused_without_allow_live(monkeypatch):
    """(e) Live base_url raises RuntimeError unless ALLOW_LIVE=true."""
    _install_rest_stub()
    from engine.config import BotConfig

    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")
    monkeypatch.delenv("ALLOW_LIVE", raising=False)
    monkeypatch.delenv("PACA_BASE_URL", raising=False)

    # Live endpoint, no opt-in -> refused.
    monkeypatch.setenv("ALPACA_BASE_URL", "https://api.alpaca.markets")
    with pytest.raises(RuntimeError):
        BotConfig.load("/dev/null")

    # Explicit opt-in + live order mode -> allowed.
    monkeypatch.setenv("ALLOW_LIVE", "true")
    monkeypatch.setenv("DEFAULT_ORDER_MODE", "live")
    cfg = BotConfig.load("/dev/null")
    assert "api.alpaca.markets" in cfg.base_url

    # paper label + live endpoint -> refused even with the opt-in.
    monkeypatch.setenv("DEFAULT_ORDER_MODE", "paper")
    with pytest.raises(RuntimeError):
        BotConfig.load("/dev/null")


def test_pending_new_order_not_double_bought():
    """Regression: a buy that is still pending_new at Alpaca must not be
    treated as closed by sync_positions(), which previously caused the bot
    to place a duplicate market buy every loop until buying power ran out."""
    _install_rest_stub()
    from engine.bot import Bot
    from engine.config import BotConfig

    cfg = BotConfig(api_key="fake_key", secret_key="fake_secret",
                    watchlist=["GRAB"], max_order_size_usd=25.0)
    bot = Bot.load(cfg)
    asyncio.run(bot.start())
    # Seed GRAB bars so the entry gate passes, then neutralize REST polling.
    bot.feed.poll_rest_bars = lambda s: None
    for i in range(20):
        bot.strategy.set_last_bar("GRAB", day=f"{(i % 28) + 1:02d}-01-2026",
                                  ts=1e9 + i * 86400, close=3.0)
    bot.strategy.cache.set_position("GRAB", {"symbol": "GRAB", "qty": 7,
                                             "avg_entry_px": 3.0, "current_px": 3.0,
                                             "order_id": "slow-1", "source": "bot"})
    bot.broker.market_hours = lambda: True
    # Simulate: order accepted (pending_new) but NOT yet a position.
    bot.broker.client.list_positions = list
    bot.broker.client.list_orders = lambda status="open": [
        _FakeOrder(id="slow-1", status="pending_new", symbol="GRAB")]
    bot.broker.client.submit_order = lambda **kw: bot.broker.client.submitted.append(kw)

    bot.iteration()  # previously: sync deleted GRAB, then re-bought
    assert bot.strategy.get_position_size("GRAB") == 7  # still held
    assert bot.broker.client.submitted == []  # no duplicate order
    asyncio.run(bot.stop())
