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


def test_stage_results_replace_is_idempotent(tmp_path):
    original=settings.db_path
    object.__setattr__(settings,"db_path",str(tmp_path/"stages.db"))
    row={"symbol":"601567","name":"三星电气","industry":"电气设备","close":42.3,
         "total_market_cap_yi":228.3,"limit_up_count":2,
         "limit_dates":["20260914","20260924"],"has_consecutive_limit_up":False,
         "low_position_pct":.17,"distance_from_low_pct":.26,"pullback_pct":-.003,
         "recent_return_pct":.098,"passes_market_cap":True,"passes_repeat_limit":True,
         "passes_nonconsecutive":True,"passes_low":True,"passes_pullback":False,
         "passes_recent_return":True,"passes_final":False,"verification_status":"VERIFIED",
         "verification_message":"双源一致","reject_reasons":["前次涨停后回撤-0.3%，要求3%～25%"]}
    try:
        db.init_db()
        assert db.save_stage_results("20260924",[row])==0
        assert db.save_stage_results("20260924",[row])==0
        with db.connect() as conn:
            assert conn.execute("SELECT COUNT(*) FROM scan_stage_results").fetchone()[0]==1
            assert conn.execute("SELECT COUNT(*) FROM scan_results").fetchone()[0]==0
    finally:
        object.__setattr__(settings,"db_path",original)
