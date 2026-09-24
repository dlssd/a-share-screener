from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _bool(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() in {"1","true","yes","on"}


@dataclass(frozen=True)
class Settings:
    db_path: str = os.getenv("DB_PATH", "./data/screener.db")
    app_username: str = os.getenv("APP_USERNAME", "").strip()
    app_password: str = os.getenv("APP_PASSWORD", "").strip()
    market_cap_min_yi: float = float(os.getenv("MARKET_CAP_MIN_YI", "80"))
    recent_trading_days: int = int(os.getenv("RECENT_TRADING_DAYS", "20"))
    min_limit_ups: int = int(os.getenv("MIN_LIMIT_UPS", "2"))
    low_window_days: int = int(os.getenv("LOW_WINDOW_DAYS", "250"))
    max_low_position: float = float(os.getenv("MAX_LOW_POSITION", "0.30"))
    min_pullback_pct: float = float(os.getenv("MIN_PULLBACK_PCT", "0.03"))
    max_pullback_pct: float = float(os.getenv("MAX_PULLBACK_PCT", "0.25"))
    max_recent_return_pct: float = float(os.getenv("MAX_RECENT_RETURN_PCT", "0.40"))
    request_timeout: float = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
    request_retries: int = int(os.getenv("REQUEST_RETRIES", "3"))
    price_tolerance_pct: float = float(os.getenv("PRICE_TOLERANCE_PCT", "0.005"))
    pct_chg_tolerance_points: float = float(os.getenv("PCT_CHG_TOLERANCE_POINTS", "0.5"))
    publish_after_hour: int = int(os.getenv("PUBLISH_AFTER_HOUR", "18"))
    initial_pool_days: int = int(os.getenv("INITIAL_POOL_DAYS", "25"))
    exclude_st: bool = _bool("EXCLUDE_ST", "true")
    include_star_market: bool = _bool("INCLUDE_STAR_MARKET", "true")
    include_chinext: bool = _bool("INCLUDE_CHINEXT", "true")
    include_bse: bool = _bool("INCLUDE_BSE", "true")
    timezone: str = os.getenv("TZ", "Asia/Shanghai")

    @property
    def market_cap_min_cny(self) -> float:
        # 东方财富“总市值”的单位是元；1 亿人民币 = 100,000,000 元。
        return self.market_cap_min_yi * 100_000_000

    def ensure_dirs(self) -> None:
        Path(self.db_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


settings = Settings()
settings.ensure_dirs()
