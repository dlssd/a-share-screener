import pandas as pd

from app import db
from app.config import settings


def test_pool_upsert_is_idempotent(tmp_path):
    original=settings.db_path
    object.__setattr__(settings,"db_path",str(tmp_path/"test.db"))
    try:
        db.init_db()
        frame=pd.DataFrame([{"trade_date":"20260923","symbol":"000001","name":"平安银行","close":10.0,
            "total_market_cap":1e10,"industry":"银行","limit_up_count":1,"pct_chg":10.0,
            "first_limit_time":"093000","last_limit_time":"145000","source":"test","fetched_at":db.utcnow()}])
        cols=list(frame.columns)
        with db.transaction() as conn: db.upsert_dataframe(conn,"limit_up_pool",frame,cols,["trade_date","symbol"])
        with db.transaction() as conn: db.upsert_dataframe(conn,"limit_up_pool",frame,cols,["trade_date","symbol"])
        with db.connect() as conn:
            assert conn.execute("SELECT COUNT(*) FROM limit_up_pool").fetchone()[0]==1
    finally:
        object.__setattr__(settings,"db_path",original)
