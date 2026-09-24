import pandas as pd
from pathlib import Path

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
         "verification_message":"双源一致","reject_reasons":["未发生回撤（最低价仍高于前次涨停收盘0.3%）"]}
    try:
        db.init_db()
        assert db.save_stage_results("20260924",[row])==0
        assert db.save_stage_results("20260924",[row])==0
        with db.connect() as conn:
            assert conn.execute("SELECT COUNT(*) FROM scan_stage_results").fetchone()[0]==1
            assert conn.execute("SELECT COUNT(*) FROM scan_results").fetchone()[0]==0
    finally:
        object.__setattr__(settings,"db_path",original)


def _stage(symbol="000001", final=False):
    return {"symbol":symbol,"name":"测试","industry":"银行","close":10.0,"total_market_cap_yi":100.0,
            "limit_up_count":2,"limit_dates":["20260922","20260924"],"has_consecutive_limit_up":False,
            "low_position_pct":.2,"distance_from_low_pct":.3,"pullback_pct":.05,"recent_return_pct":.1,
            "passes_market_cap":True,"passes_repeat_limit":True,"passes_nonconsecutive":True,"passes_low":True,
            "passes_pullback":True,"passes_recent_return":True,"passes_final":final,"verification_status":"VERIFIED",
            "verification_message":"ok","reject_reasons":[]}


def test_published_snapshot_survives_running_and_failed_rerun(tmp_path):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"snapshots.db"))
    try:
        db.init_db(); first=db.start_scan_run("20260924","2026-09-24 20:00 Asia/Shanghai")
        db.finish_scan_run(first,"20260924","WARNING","first",{"candidate_rows":0},"WARNING","SUCCESS","now",[_stage()])
        second=db.start_scan_run("20260924","now")
        with db.connect() as conn:
            assert conn.execute("select run_id from published_snapshots where trade_date='20260924'").fetchone()[0]==first
            assert conn.execute("select count(*) from scan_stage_results_v06 where run_id=?",(first,)).fetchone()[0]==1
        db.fail_scan_run(second,"20260924","network failed","now")
        assert db.published_run_for_date("20260924")["run_id"]==first
        third=db.start_scan_run("20260924","now")
        db.finish_scan_run(third,"20260924","SUCCESS","new",{"candidate_rows":1},"SUCCESS","SUCCESS","now",[_stage("000002",True)])
        published=db.published_run_for_date("20260924")
        assert published["run_id"]==third
        assert db.stage_rows_for_run(third)[0]["symbol"]=="000002"
    finally:
        object.__setattr__(settings,"db_path",original)


def test_default_database_path_is_project_root_based(monkeypatch, tmp_path):
    from app.config import PROJECT_ROOT
    original=settings.db_path
    try:
        object.__setattr__(settings,"db_path",str(PROJECT_ROOT/"data"/"screener.db"))
        before=Path(settings.db_path).resolve()
        monkeypatch.chdir(tmp_path)
        assert Path(settings.db_path).resolve()==before
        assert before.parent==PROJECT_ROOT/"data"
    finally:
        object.__setattr__(settings,"db_path",original)
