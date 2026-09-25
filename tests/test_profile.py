import pandas as pd

from app.profile import build_profile, maximum_rise


def _frame(n=1250):
    dates=pd.bdate_range("2021-01-01",periods=n)
    values=pd.Series([10+i*.01 for i in range(n)])
    return pd.DataFrame({"trade_date":dates.strftime("%Y%m%d"),"adjusted_close":values})


def test_one_three_five_year_positions_and_no_future_data():
    frame=_frame(1300); cutoff=frame.iloc[1249]["trade_date"]
    profile=build_profile(frame,cutoff)
    assert profile["last_trade_date"]==cutoff
    assert profile["observations"]==1250
    assert profile["one_year"]["position"]==1.0
    assert profile["three_year"]["position"]==1.0
    assert profile["five_year"]["position"]==1.0


def test_ma250_direction_and_insufficient_history():
    up=build_profile(_frame(300),_frame(300).iloc[-1]["trade_date"])
    assert up["ma250"]["direction"]=="上行"
    assert not up["three_year"]["available"] and up["three_year"]["message"]=="历史不足3年"


def test_maximum_rise_requires_low_before_high():
    values=pd.Series([10,8,12,9,18],dtype=float); dates=pd.Series(["1","2","3","4","5"])
    result=maximum_rise(values,dates)
    assert result["gain"]==1.25
    assert result["low_date"]=="2" and result["high_date"]=="5"
