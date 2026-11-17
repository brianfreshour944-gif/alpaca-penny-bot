"""Environment + config loading.

Paper by default. LIVE requires an explicit flag and is never assumed.
Secrets MUST be pasted by the user into .env (never committed; .env is gitignored).
"""
import os
from dataclasses import dataclass, field

try:
    from dotenv import load_dotenv

    DOTENV_LOADED = True
except Exception:  # pragma: no cover - dotenv required
    DOTENV_LOADED = False


@dataclass
class BotConfig:
    """Validated runtime configuration."""

    api_key: str
    secret_key: str
    base_url: str = "https://paper-api.alpaca.markets"
    stream_token: str = ""
    order_mode: str = "paper"
    max_order_size_usd: float = 25.0
    # Strategy/risk parameters (defaults keep the bot conservative).
    stop_loss_pct: float = 0.08
    take_profit_pct: float = 0.15
    max_position_qty: int = 1000
    watchlist: list = field(default_factory=list)

    @classmethod
    def load(cls, dotenv_path: str = ".env", **kwargs) -> "BotConfig":
        # Load .env file first (does NOT override existing env vars).
        load_dotenv(dotenv_path, override=False)

        # Accept both naming conventions: ALPACA_* (standard) and PACA_* (legacy).
        api_key = os.getenv("ALPACA_API_KEY") or os.getenv("PACA_API_KEY", "")
        secret_key = os.getenv("ALPACA_SECRET_KEY") or os.getenv("PACA_SECRET_KEY", "")
        base_url = (
            os.getenv("ALPACA_BASE_URL")
            or os.getenv("PACA_BASE_URL")
            or "https://paper-api.alpaca.markets"
        )
        stream_token = (
            os.getenv("ALPACA_STREAMING_TOKEN")
            or os.getenv("PACA_STREAMING_TOKEN", "")
        )
        watchlist = [
            s.strip().upper()
            for s in os.getenv("WATCHLIST", "").split(",")
            if s.strip()
        ]

        config = cls(
            api_key=api_key,
            secret_key=secret_key,
            base_url=base_url,
            stream_token=stream_token,
            order_mode=os.getenv("DEFAULT_ORDER_MODE", "paper"),
            max_order_size_usd=float(os.getenv("MAX_ORDER_SIZE_USD", "25.00")),
            stop_loss_pct=float(os.getenv("STOP_LOSS_PCT", "0.08")),
            take_profit_pct=float(os.getenv("TAKE_PROFIT_PCT", "0.15")),
            max_position_qty=int(os.getenv("MAX_POSITION_QTY", "1000")),
            watchlist=watchlist,
        )
        if not config.api_key or not config.secret_key:
            raise RuntimeError(
                "ALPACA_API_KEY and ALPACA_SECRET_KEY (or PACA_* legacy names) "
                "are not set. Check Coolify Environment Variables."
            )
        # Paper-only guard: refuse to start against a live endpoint unless
        # the operator explicitly opts in with ALLOW_LIVE=true. Also never
        # let the paper label disagree with the configured endpoint.
        if config.base_url and not config.base_url.endswith("/"):
            config.base_url += "/"
        is_paper_url = "paper-api.alpaca.markets" in config.base_url
        allow_live = os.getenv("ALLOW_LIVE", "").strip().lower() == "true"
        if not is_paper_url and not allow_live:
            raise RuntimeError(
                f"Refusing to start: base_url={config.base_url!r} is not the "
                "paper endpoint. Set ALLOW_LIVE=true explicitly to trade live."
            )
        if config.order_mode == "paper" and not is_paper_url:
            raise RuntimeError(
                "order_mode=paper but base_url is not the paper endpoint — "
                "labels and endpoint must agree."
            )
        if config.order_mode not in ("paper", "live"):
            config.order_mode = "paper"
        # Mirror to PACA_* for any legacy code expecting those names.
        os.environ.setdefault("PACA_API_KEY", config.api_key)
        os.environ.setdefault("PACA_SECRET_KEY", config.secret_key)
        # Strip whitespace (Coolify UI sometimes preserves trailing newline).
        config.api_key = config.api_key.strip()
        config.secret_key = config.secret_key.strip()
        # Diagnostic: log key fingerprints (first/last 4 chars, length) so
        # mismatches are visible in Coolify logs without exposing secrets.
        print(f"[config] key_id={config.api_key[:4]}...{config.api_key[-4:]} "
              f"(len={len(config.api_key)}) "
              f"secret=...{config.secret_key[-4:]} (len={len(config.secret_key)}) "
              f"mode={config.order_mode} base_url={config.base_url}")
        return config


if __name__ == "__main__":
    try:
        cfg = BotConfig.load()
        print(f"mode={cfg.order_mode.upper()} max_order_usd={cfg.max_order_size_usd}")
    except RuntimeError as e:
        print("ERROR:", e)
