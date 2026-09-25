from fastapi.testclient import TestClient

from app import db
from app.config import settings
from app.datasource import AKShareSource
from app.sync import DataValidationError
from app.web import app, refresh_lock
import app.web as web_module


def test_zero_candidates_still_shows_review_stages(tmp_path):
    original=settings.db_path
    object.__setattr__(settings,"db_path",str(tmp_path/"web.db"))
    row={"symbol":"601567","name":"三星电气","industry":"电气设备","close":42.3,
         "total_market_cap_yi":228.3,"limit_up_count":2,
         "limit_dates":["20260914","20260924"],"has_consecutive_limit_up":False,
         "low_position_pct":.17,"distance_from_low_pct":.26,"pullback_pct":-.003,
         "recent_return_pct":.098,"passes_market_cap":True,"passes_repeat_limit":True,
         "passes_nonconsecutive":True,"passes_low":True,"passes_pullback":False,
         "passes_recent_return":True,"passes_final":False,"verification_status":"VERIFIED",
         "verification_message":"双源一致","reject_reasons":["未发生回撤（最低价仍高于前次涨停收盘0.3%）"]}
    try:
        db.init_db(); db.mark_pipeline_start("20260924"); db.save_stage_results("20260924",[row])
        counts={"pool_rows":52,"cap_rows":11,"repeated_rows":6,"nonconsecutive_rows":3,
                "low_rows":1,"pullback_rows":0,"pattern_rows":0,"candidate_rows":0}
        db.mark_pipeline_finish("20260924","WARNING","DataSourceError",counts,"WARNING","SUCCESS","2026-09-24 20:00 Asia/Shanghai")
        response=TestClient(app).get("/")
        assert response.status_code==200
        assert "今天没有完全满足所有条件的股票" in response.text
        assert "三星电气" in response.text
        assert "未发生回撤（最低价仍高于前次涨停收盘0.3%）" in response.text
        assert "查看技术详情" in response.text
        assert "重新抓取 09-24" in response.text
        assert refresh_lock.acquire(blocking=False)
        try:
            busy=TestClient(app).post("/api/refresh")
            assert busy.status_code==409
            assert busy.json()["message"]=="数据正在抓取，请稍候。"
        finally:
            refresh_lock.release()
    finally:
        object.__setattr__(settings,"db_path",original)


def test_refresh_uses_browsed_date_and_news_is_cached(tmp_path, monkeypatch):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"dated.db"))
    try:
        db.init_db()
        for date in ("20260923","20260924"):
            run=db.start_scan_run(date,"now")
            db.finish_scan_run(run,date,"WARNING","ok",{"candidate_rows":0},"WARNING","SUCCESS","now",[])
        called=[]
        def fake_sync(date,run_scan=True,force=False):
            called.append((date,force))
        monkeypatch.setattr(web_module,"sync_market_day",fake_sync)
        response=TestClient(app).post("/api/refresh?date=20260923")
        assert response.status_code==200
        assert called==[("20260923",True)]
        response=TestClient(app).post("/api/refresh?date=20260924")
        assert response.status_code==200
        assert called[-1]==("20260924",True)

        calls=[]
        def fake_news(self,symbol):
            calls.append(symbol)
            return [{"title":"测试新闻","published_at":"2026-09-24 20:00","source":"测试来源"}]
        monkeypatch.setattr(AKShareSource,"news",fake_news)
        first=TestClient(app).get("/api/news/601567")
        second=TestClient(app).get("/api/news/601567")
        assert first.status_code==200 and second.status_code==200
        assert first.json()["items"]==second.json()["items"]
        assert calls==["601567"]

        def reject_future(date,run_scan=True,force=False):
            raise DataValidationError("20270101 尚非完整盘后交易日")
        monkeypatch.setattr(web_module,"sync_market_day",reject_future)
        future=TestClient(app).post("/api/refresh?date=20270101")
        assert future.status_code==400
    finally:
        object.__setattr__(settings,"db_path",original)


def test_running_and_failed_update_keep_published_page(tmp_path):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"published-web.db"))
    row={"symbol":"601567","name":"三星电气","industry":"电网设备","close":42.3,
         "total_market_cap_yi":228.3,"limit_up_count":2,"limit_dates":["20260914","20260924"],
         "has_consecutive_limit_up":False,"low_position_pct":.17,"distance_from_low_pct":.26,
         "pullback_pct":-.003,"recent_return_pct":.098,"passes_market_cap":True,"passes_repeat_limit":True,
         "passes_nonconsecutive":True,"passes_low":True,"passes_pullback":False,"passes_recent_return":True,
         "passes_final":False,"verification_status":"VERIFIED","verification_message":"双源一致",
         "reject_reasons":["未发生回撤（最低价仍高于前次涨停收盘0.3%）"]}
    try:
        db.init_db(); first=db.start_scan_run("20260924","now")
        db.finish_scan_run(first,"20260924","WARNING","published",{"candidate_rows":0},"WARNING","SUCCESS","now",[row])
        second=db.start_scan_run("20260924","now")
        page=TestClient(app).get("/?date=20260924")
        assert page.status_code==200 and "正在重新抓取" in page.text and "三星电气" in page.text
        db.fail_scan_run(second,"20260924","模拟网络失败","now")
        page=TestClient(app).get("/?date=20260924")
        assert "本次更新失败" in page.text and "当前展示上一次成功结果" in page.text and "三星电气" in page.text
    finally:
        object.__setattr__(settings,"db_path",original)


def test_unpublished_trade_date_is_not_replaced_by_latest(tmp_path, monkeypatch):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"calendar-web.db"))
    try:
        db.init_db(); run=db.start_scan_run("20260924","now")
        db.finish_scan_run(run,"20260924","SUCCESS","ok",{"candidate_rows":0},"SUCCESS","SUCCESS","now",[])
        monkeypatch.setattr(web_module,"is_trade_date",lambda date: date=="20260921")
        monkeypatch.setattr(web_module,"previous_trade_date",lambda date:"20260918")
        monkeypatch.setattr(web_module,"next_trade_date",lambda date:"20260922")
        monkeypatch.setattr(web_module,"month_days",lambda month:[
            {"date":"20260921","is_trade":True},{"date":"20260922","is_trade":True}])
        page=TestClient(app).get("/?date=20260921")
        assert page.status_code==200
        assert "2026-09-21 尚未生成盘后复盘" in page.text
        assert "生成 09-21 复盘" in page.text
        assert "2026-09-24 盘后复盘" not in page.text
    finally:
        object.__setattr__(settings,"db_path",original)


def test_closed_date_has_no_run_button(tmp_path, monkeypatch):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"closed-web.db"))
    try:
        db.init_db()
        monkeypatch.setattr(web_module,"is_trade_date",lambda date:False)
        monkeypatch.setattr(web_module,"previous_trade_date",lambda date:"20260925")
        monkeypatch.setattr(web_module,"next_trade_date",lambda date:"20260928")
        monkeypatch.setattr(web_module,"month_days",lambda month:[{"date":"20260926","is_trade":False}])
        page=TestClient(app).get("/?date=20260926")
        assert "当日A股休市，无需生成复盘" in page.text
        assert "生成 09-26 复盘" not in page.text
    finally:
        object.__setattr__(settings,"db_path",original)
