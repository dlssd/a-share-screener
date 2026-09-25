import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app.audit import _enrich_industries, _is_current_completed_day
from app.config import settings
from app import db


class NoDetailSource:
    def individual_info(self,symbol):
        raise AssertionError("pool industry should be used before detail lookup")


def test_success_universe_merges_industry_from_limit_pool(tmp_path):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"industry.db"))
    try:
        db.init_db()
        universe=[{"symbol":"000001","name":"测试","industry":None}]
        pool=pd.DataFrame([{"symbol":"000001","industry":"银行"}])
        _enrich_industries(NoDetailSource(),universe,pool)
        assert universe[0]["industry"]=="银行"
    finally:
        object.__setattr__(settings,"db_path",original)


def test_current_completed_day_uses_china_timezone_not_host_timezone():
    # The same instant is 18:30 in Shanghai, regardless of how the host/runtime
    # represents it. Both must select the current-day post-close audit path.
    utc_now=datetime(2026,9,24,10,30,tzinfo=timezone.utc)
    berlin_now=utc_now.astimezone(ZoneInfo("Europe/Berlin"))
    assert _is_current_completed_day("20260924",utc_now)
    assert _is_current_completed_day("20260924",berlin_now)
    assert not _is_current_completed_day("20260923",utc_now)


def test_before_china_publish_threshold_is_not_completed():
    utc_now=datetime(2026,9,24,9,59,tzinfo=timezone.utc)  # 17:59 Asia/Shanghai
    assert not _is_current_completed_day("20260924",utc_now)
