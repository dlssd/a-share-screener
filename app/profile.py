from __future__ import annotations

import pandas as pd


def _window(values: pd.Series, dates: pd.Series, size: int, label: str) -> dict:
    if len(values)<size: return {"available":False,"message":f"历史不足{label}"}
    sample=values.tail(size); current=float(sample.iloc[-1]); low=float(sample.min()); high=float(sample.max())
    return {"available":True,"days":size,"position":(current-low)/(high-low) if high>low else 0.0,
            "distance_from_high":current/high-1,"distance_from_low":current/low-1}


def maximum_rise(values: pd.Series, dates: pd.Series) -> dict | None:
    """Maximum low-to-later-high gain, with the low required to precede the high."""
    if len(values)<2: return None
    min_value=float(values.iloc[0]); min_date=str(dates.iloc[0]); best=None
    for value,date in zip(values.iloc[1:],dates.iloc[1:]):
        value=float(value); gain=value/min_value-1
        if best is None or gain>best["gain"]:
            best={"gain":gain,"low_date":min_date,"high_date":str(date),"high_price":value}
        if value<min_value: min_value=value; min_date=str(date)
    return best


def build_profile(frame: pd.DataFrame, as_of_date: str) -> dict:
    data=frame.loc[frame["trade_date"].astype(str)<=as_of_date].sort_values("trade_date").copy()
    if data.empty: raise ValueError("目标日期前没有历史行情")
    # Every metric below uses the same Tencent qfq adjusted-close sequence.
    values=pd.to_numeric(data["adjusted_close"],errors="coerce"); valid=values.notna()
    values=values[valid].reset_index(drop=True); dates=data.loc[valid,"trade_date"].astype(str).reset_index(drop=True)
    result={"as_of_date":as_of_date,"last_trade_date":str(dates.iloc[-1]),"observations":len(values),
            "one_year":_window(values,dates,250,"1年"),"three_year":_window(values,dates,750,"3年"),
            "five_year":_window(values,dates,1250,"5年")}
    if len(values)>=270:
        ma=values.rolling(250).mean(); current_ma=float(ma.iloc[-1]); previous_ma=float(ma.iloc[-21]); change=current_ma/previous_ma-1
        result["ma250"]={"available":True,"distance":float(values.iloc[-1])/current_ma-1,"change_20d":change,
                         "direction":"上行" if change>.01 else ("下行" if change<-.01 else "基本走平")}
    else: result["ma250"]={"available":False,"message":"历史不足250日"}
    five_values=values.tail(1250); five_dates=dates.tail(1250); rise=maximum_rise(five_values,five_dates)
    if rise:
        rise["current_from_high"]=float(values.iloc[-1])/rise["high_price"]-1
    result["maximum_rise_5y"]=rise if len(values)>=2 else None
    return result
