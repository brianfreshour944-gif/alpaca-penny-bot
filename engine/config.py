"""Environment + config loading.

Paper by default. LIVE requires an explicit flag and is never assumed.
Secrets MUST be pasted by the user into .env (never committed; .env is gitignored).
"""
import os
from dataclasses import dataclass

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

    @classmethod
    def load(cls, dotenv_path: str = ".env", **kwargs) -> "BotConfig":
        # Load .env file first (does NOT override existing env vars).
        load_dotenv(dotenv_path, override=False)

        # Accept both naming conventions: ALPACA_* (standard) and PACA_* (legacy).
        # This fixes the Coolify mismatch where vars were set as ALPACA_* but
        # code only looked for PACA_* (empty string → RuntimeError).
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

        config = cls(
            api_key=api_key,
            secret_key=secret_key,
            base_url=base_url,
            stream_token=stream_token,
            order_mode=os.getenv("DEFAULT_ORDER_MODE", "paper"),
            max_order_size_usd=float(os.getenv("MAX_ORDER_SIZE_USD", "25.00")),
        )
        if not config.api_key or not config.secret_key:
            raise RuntimeError(
                "ALPACA_API_KEY and ALPACA_SECRET_KEY (or PACA_* legacy names) "
                "are not set. Check Coolify Environment Variables."
            )
        if config.base_url and not config.base_url.endswith("/"):
            config.base_url += "/"
        if config.order_mode not in ("paper", "live"):
            config.order_mode = "paper"
        # Mirror to PACA_* for any legacy code expecting those names.
        os.environ.setdefault("PACA_API_KEY", config.api_key)
        os.environ.setdefault("PACA_SECRET_KEY", config.secret_key)
        return config


if __name__ == "__main__":
    try:
        cfg = BotConfig.load()
        print(f"mode={cfg.order_mode.upper()} max_order_usd={cfg.max_order_size_usd}")
    except RuntimeError as e:
        print("ERROR:", e)
