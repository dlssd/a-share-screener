from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd


UNKNOWN_LIMIT_RULE = "UNKNOWN_LIMIT_RULE"
INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"


def market_for_symbol(symbol: str) -> str:
    symbol=str(symbol).zfill(6)
    if symbol.startswith(("688","689")): return "STAR"
    if symbol.startswith(("300","301","302")): return "CHINEXT"
    if symbol.startswith(("4","8","92")): return "BSE"
    if symbol.startswith(("000","001","002","003","600","601","603","605")): return "MAIN"
    return "UNKNOWN"


def limit_rate(symbol: str, name: str="") -> Decimal | None:
    if "ST" in str(name).upper(): return Decimal("0.05")
    market=market_for_symbol(symbol)
    return {"MAIN":Decimal("0.10"),"CHINEXT":Decimal("0.20"),
            "STAR":Decimal("0.20"),"BSE":Decimal("0.30")}.get(market)


def theoretical_limit_price(previous_close: float, rate: Decimal) -> float:
    # A股价格最小单位为 0.01 元；交易所涨停价按价格档位四舍五入。
    value=Decimal(str(previous_close))*(Decimal("1")+rate)
    return float(value.quantize(Decimal("0.01"),rounding=ROUND_HALF_UP))


@dataclass(frozen=True)
class LimitDetection:
    dates: list[str]
    status: str
    unknown_dates: list[str]


def detect_limit_up_days(raw_history: pd.DataFrame, symbol: str, name: str="", *,
                         listing_date: str | None=None) -> LimitDetection:
    """Rebuild limit-up sessions from unadjusted closes only."""
    if "raw_close" not in raw_history.columns:
        raise ValueError("limit-up detection requires raw_close")
    rate=limit_rate(symbol,name)
    if rate is None:
        return LimitDetection([],UNKNOWN_LIMIT_RULE,raw_history["trade_date"].astype(str).tolist())
    h=raw_history.sort_values("trade_date").drop_duplicates("trade_date").copy()
    h["raw_close"]=pd.to_numeric(h["raw_close"],errors="coerce")
    dates=[]; unknown=[]
    no_limit_sessions=1 if market_for_symbol(symbol)=="BSE" else 5
    listing_date=str(listing_date).replace("-","") if listing_date else None
    first_history_day=str(h.iloc[0]["trade_date"]) if not h.empty else ""
    listing_inside_window=bool(listing_date and listing_date>=first_history_day)
    for i in range(1,len(h)):
        day=str(h.iloc[i]["trade_date"])
        previous=float(h.iloc[i-1]["raw_close"]); close=float(h.iloc[i]["raw_close"])
        if pd.isna(previous) or pd.isna(close) or previous<=0:
            unknown.append(day); continue
        listed_sessions=int((h.iloc[:i+1]["trade_date"].astype(str)>=listing_date).sum()) if listing_inside_window else no_limit_sessions+1
        if listing_inside_window and day>=listing_date and listed_sessions<=no_limit_sessions:
            unknown.append(day); continue
        expected=theoretical_limit_price(previous,rate)
        if abs(close-expected)<=0.0051: dates.append(day)
    status=UNKNOWN_LIMIT_RULE if unknown else "OK"
    return LimitDetection(dates,status,unknown)
