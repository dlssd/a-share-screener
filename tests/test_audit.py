import pandas as pd

from app.audit import _enrich_industries
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
