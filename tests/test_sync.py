import pandas as pd
import pytest
from datetime import datetime
from zoneinfo import ZoneInfo

from app.sync import DataValidationError, backfill, missing_required_days, resolve_latest_completed_trade_date, validate_limit_pool
import app.sync as sync_module


def test_empty_api_response_fails_closed():
    with pytest.raises(DataValidationError):
        validate_limit_pool(pd.DataFrame(),"20250102")


def test_duplicate_symbol_fails_closed():
    df = pd.DataFrame({"trade_date":["20250102"]*2,"symbol":["000001"]*2,"name":["A","A"],
                       "close":[10,10],"total_market_cap":[8e9,8e9]})
    with pytest.raises(DataValidationError):
        validate_limit_pool(df,"20250102")


class FakeCalendar:
    def trading_days(self, start, end):
        return ["20260921","20260922","20260923","20260924"]


def test_noon_does_not_publish_current_trading_day():
    now=datetime(2026,9,24,12,0,tzinfo=ZoneInfo("Asia/Shanghai"))
    assert resolve_latest_completed_trade_date(FakeCalendar(),now)=="20260923"


def test_weekend_uses_latest_trading_day():
    now=datetime(2026,9,26,12,0,tzinfo=ZoneInfo("Asia/Shanghai"))
    assert resolve_latest_completed_trade_date(FakeCalendar(),now)=="20260924"


def test_missing_day_prevents_complete_window():
    required=["20260921","20260922","20260923"]
    assert missing_required_days(required,{"20260921","20260923"})==["20260922"]


def test_backfill_trading_days_processes_requested_count(monkeypatch):
    days=["20260918","20260921","20260922","20260923","20260924"]
    monkeypatch.setattr(sync_module,"resolve_latest_completed_trade_date",lambda:"20260924")
    monkeypatch.setattr(sync_module,"trade_dates_between",lambda start,end:days)
    called=[]
    monkeypatch.setattr(sync_module,"sync_market_day",lambda day,force=False:called.append((day,force)))
    assert backfill(5,force=True)==days
    assert called==[(day,True) for day in days]
