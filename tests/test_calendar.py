from datetime import date, timedelta

from app import calendar as market_calendar
from app import db
from app.config import settings


class FakeSource:
    def trading_days(self,start,end):
        return ["20260921","20260922","20260923","20260924","20260925"]


def test_exchange_calendar_cache_and_navigation(tmp_path):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"calendar.db"))
    try:
        assert market_calendar.is_trade_date("20260921",FakeSource())
        assert not market_calendar.is_trade_date("20260926",FakeSource())
        assert market_calendar.previous_trade_date("20260923",FakeSource())=="20260922"
        assert market_calendar.next_trade_date("20260923",FakeSource())=="20260924"
        with db.connect() as conn:
            assert conn.execute("select is_open from trading_calendar where trade_date='20260926'").fetchone()[0]==0
    finally:
        object.__setattr__(settings,"db_path",original)
