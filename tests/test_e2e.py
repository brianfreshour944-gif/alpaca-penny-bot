"""End-to-end test of the trading loop with a stubbed Alpaca REST client.

Stubs alpaca_trade_api.REST in sys.modules before engine modules import it,
so the full broker/strategy/feed/bot path runs without network access.
"""
import asyncio
import sys
import types
from datetime import UTC, datetime


class _FakeClock:
    is_open = True


class _FakePosition:
    def __init__(self, symbol, qty, avg, cur):
        self.symbol, self.qty = symbol, str(qty)
        self.avg_entry_price, self.current_price = str(avg), str(cur)
        self.market_value, self.unrealized_pl = "0", "0"


class _FakeOrder:
    def __init__(self):
        self.id, self.status = "fake-order-1", "new"


def _install_rest_stub():
    """Install a stateful fake REST into sys.modules['alpaca_trade_api']."""
    mod = types.ModuleType("alpaca_trade_api")

    class FakeREST:
        instances = []

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
    FakeREST = _install_rest_stub()
    from engine.bot import Bot
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
    FakeREST = _install_rest_stub()
    from engine.bot import Bot
    from engine.config import BotConfig

    cfg = BotConfig(api_key="fake_key", secret_key="fake_secret", watchlist=["PENY"])
    bot = Bot.load(cfg)
    bot.config = cfg
    asyncio.run(bot.start())
    bot.broker.client.get_clock = lambda: _FakeClock() if False else type("C", (), {"is_open": False})()
    s = bot.iteration()
    assert s["entries"] == [] and s["exits"] == []
    assert FakeREST.instances[-1].submitted == [], "no orders outside market hours"


def test_position_sync_pulls_alpaca_state():
    FakeREST = _install_rest_stub()
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
    FakeREST = _install_rest_stub()
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
