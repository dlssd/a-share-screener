from __future__ import annotations

import pandas as pd

from app.config import settings
from app.scanner import ScanParams, evaluate_candidate, has_consecutive_limit_ups, verify_independent_source


def params():
    return ScanParams(8_000_000_000,20,2,250,.30,.03,.25,.40)


def history():
    dates = pd.bdate_range("2025-01-01",periods=260)
    close = [18-i*.06 if i<120 else 10.2+((i%7)-3)*.03 for i in range(260)]
    close[-8] = 10.5
    close[-7:-1] = [9.95]*6
    close[-1] = 10.2
    return pd.DataFrame({"trade_date":[d.strftime("%Y%m%d") for d in dates],"adjusted_close":close})


def test_market_cap_unit_is_yuan():
    assert settings.market_cap_min_cny == settings.market_cap_min_yi * 100_000_000


def test_trading_day_consecutive_detection():
    days = ["20250103","20250106","20250107"]
    assert has_consecutive_limit_ups(["20250103","20250106"],days)
    assert not has_consecutive_limit_ups(["20250103","20250107"],days)


def test_position_pullback_and_return():
    h = history()
    result = evaluate_candidate(h,[h.trade_date.iloc[-8],h.trade_date.iloc[-1]],params())
    assert result is not None
    assert 0 <= result["low_position_pct"] <= .30
    assert .03 <= result["pullback_pct"] <= .25
    assert result["recent_return_pct"] <= .40


def test_rejects_insufficient_history():
    assert evaluate_candidate(history().tail(249),["20250101","20250102"],params()) is None


def test_cross_source_mismatch():
    ak = {"trade_date":"20250102","close":10.0,"pct_chg":10.0}
    bs = pd.DataFrame([{"trade_date":"20250102","close":9.0,"pct_chg":8.0}])
    bs = bs.rename(columns={"close":"raw_close"})
    assert verify_independent_source(ak,bs,"腾讯")[0] == "DATA_MISMATCH"


def test_raw_price_cannot_be_used_for_adjusted_metrics():
    raw = history().rename(columns={"adjusted_close":"raw_close"})
    import pytest
    with pytest.raises(ValueError):
        evaluate_candidate(raw,["20250101","20250102"],params())
