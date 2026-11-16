"""Paper-safe Alpaca bot server — runnable entry point (no extra deps).

    python -m server.server                 # direct module run
    python -m server.server --order-mode live  # only when you mean it

Loads config, runs the paper (or explicitly LIVE) trades, and streams a few
price/quote ticks. PAPER is the default. Set the order mode in `.env`.
"""
import argparse
import asyncio

from engine.bot import Bot, run_until_server_stop
from engine.config import BotConfig


async def main(argv=None):
    parser = argparse.ArgumentParser(description="Alpaca paper bot server")
    parser.add_argument("--config", help="path to .env (default: .env)")
    parser.add_argument("--order-mode", choices=["paper", "live"], default="paper",
                        help="set explicitly in .env in production")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args(argv)

    # NOTE: was `Bot.load(args.config)` — built a Bot from the raw --config
    # string/None instead of loading BotConfig from the .env path. Wiring fix
    # only; startup order (config -> bot -> run loop) is unchanged.
    config = BotConfig.load(args.config)
    print(f"[server] order mode: {config.order_mode}")

    bot = Bot.load(config)
    await run_until_server_stop(bot)
    print("server stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nstopped by user")
