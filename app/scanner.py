from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .config import settings


@dataclass(frozen=True)
class ScanParams:
    market_cap_min_cny: float
    recent_trading_days: int
    min_limit_ups: int
    low_window_days: int
    max_low_position: float
    min_pullback_pct: float
    max_pullback_pct: float
    max_recent_return_pct: float

    @classmethod
    def from_settings(cls):
        return cls(settings.market_cap_min_cny, settings.recent_trading_days, settings.min_limit_ups,
                   settings.low_window_days, settings.max_low_position, settings.min_pullback_pct,
                   settings.max_pullback_pct, settings.max_recent_return_pct)


def has_consecutive_limit_ups(limit_dates: list[str], trading_days: list[str]) -> bool:
    positions = {d: i for i, d in enumerate(trading_days)}
    indexes = sorted(positions[d] for d in limit_dates if d in positions)
    return any(b == a + 1 for a, b in zip(indexes, indexes[1:]))


def evaluate_stages(history: pd.DataFrame, limit_dates: list[str], params: ScanParams) -> dict | None:
    """All price metrics use adjusted_close; raw_close is never accepted here."""
    if "adjusted_close" not in history.columns:
        raise ValueError("adjusted history must contain adjusted_close")
    h = history.sort_values("trade_date").drop_duplicates("trade_date").reset_index(drop=True).copy()
    h["adjusted_close"] = pd.to_numeric(h["adjusted_close"], errors="coerce")
    h = h.dropna(subset=["adjusted_close"])
    if len(h) < params.low_window_days:
        return None
    window = h.tail(params.low_window_days)
    current = float(window["adjusted_close"].iloc[-1])
    low_250, high_250 = float(window["adjusted_close"].min()), float(window["adjusted_close"].max())
    if not all(np.isfinite(v) and v > 0 for v in (current, low_250, high_250)):
        return None
    low_position = 0.0 if high_250 == low_250 else (current-low_250)/(high_250-low_250)
    distance = current/low_250-1
    recent = h.tail(params.recent_trading_days)
    first = float(recent["adjusted_close"].iloc[0])
    recent_return = current/first-1 if first > 0 else None
    passes_low = low_position <= params.max_low_position
    passes_recent_return = recent_return is not None and recent_return <= params.max_recent_return_pct
    today = str(h["trade_date"].iloc[-1])
    previous = [d for d in sorted(limit_dates) if d < today]
    pullback = None
    if previous:
        indexes = h.index[h["trade_date"].astype(str) == previous[-1]].tolist()
        if indexes:
            trough_rows = h.iloc[indexes[-1]+1:-1]
            if not trough_rows.empty:
                previous_close = float(h.iloc[indexes[-1]]["adjusted_close"])
                pullback = 1-float(trough_rows["adjusted_close"].min())/previous_close
    passes_pullback = pullback is not None and params.min_pullback_pct <= pullback <= params.max_pullback_pct
    return {"low_position_pct":low_position,"distance_from_low_pct":distance,
            "pullback_pct":pullback,"recent_return_pct":recent_return,
            "passes_low":passes_low,"passes_pullback":passes_pullback,
            "passes_recent_return":passes_recent_return}


def evaluate_candidate(history: pd.DataFrame, limit_dates: list[str], params: ScanParams) -> dict | None:
    metrics = evaluate_stages(history, limit_dates, params)
    if not metrics or not metrics["passes_low"] or not metrics["passes_pullback"] or not metrics["passes_recent_return"]:
        return None
    return {k:v for k,v in metrics.items() if not k.startswith("passes_")}


def verify_independent_source(pool_row: dict, raw_history: pd.DataFrame, source_name: str) -> tuple[str,str]:
    if raw_history.empty or "raw_close" not in raw_history:
        return "DATA_MISMATCH", f"{source_name} 无目标日未复权行情"
    row = raw_history.sort_values("trade_date").iloc[-1]
    if str(row["trade_date"]) != str(pool_row["trade_date"]):
        return "DATA_MISMATCH", f"交易日不一致: {source_name}={row['trade_date']}"
    pool_close, other_close = float(pool_row["close"]), float(row["raw_close"])
    price_diff = abs(other_close-pool_close)/max(pool_close,.01)
    pct_message = ""
    if pd.notna(row.get("pct_chg")) and pool_row.get("pct_chg") is not None:
        pct_diff = abs(float(row["pct_chg"])-float(pool_row["pct_chg"]))
        pct_message = f"; 涨幅差={pct_diff:.2f}个百分点"
    else:
        pct_diff = 0
    if price_diff > settings.price_tolerance_pct or pct_diff > settings.pct_chg_tolerance_points:
        return "DATA_MISMATCH", f"收盘价 东方财富={pool_close:.2f}/{source_name}={other_close:.2f}{pct_message}"
    return "VERIFIED", f"{source_name} 未复权收盘={other_close:.2f}{pct_message}"
