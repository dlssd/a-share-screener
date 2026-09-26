import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

from app import db
from app.config import settings
from app.datasource import AKShareSource
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
            assert "后台任务" in busy.json()["message"]
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
        monkeypatch.setattr(web_module,"is_trade_date",lambda date:True)
        monkeypatch.setattr(web_module,"_launch_refresh_job",lambda job_id,date:(called.append(date) or 12345))
        response=TestClient(app).post("/api/refresh?date=20260923")
        assert response.status_code==202 and response.json()["status"]=="RUNNING"
        assert called==["20260923"]
        db.finish_background_refresh(response.json()["job_id"],"SUCCESS","done")
        response=TestClient(app).post("/api/refresh?date=20260924")
        assert response.status_code==202 and called[-1]=="20260924"
        db.finish_background_refresh(response.json()["job_id"],"SUCCESS","done")

        calls=[]
        def fake_news(self,symbol):
            calls.append(symbol)
            return [{"title":"测试新闻","published_at":"2026-09-24 20:00","source":"测试来源"}]
        monkeypatch.setattr(AKShareSource,"news",fake_news)
        monkeypatch.setattr(web_module,"current_news_allowed",lambda *args,**kwargs:True)
        first=TestClient(app).get("/api/news/601567?as_of_date=20260924")
        second=TestClient(app).get("/api/news/601567?as_of_date=20260924")
        assert first.status_code==200 and second.status_code==200
        assert first.json()["items"]==second.json()["items"]
        assert calls==["601567"]

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


def test_historical_warning_never_claims_complete_market(tmp_path, monkeypatch):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"confidence.db"))
    try:
        db.init_db(); run=db.start_scan_run("20260921","now")
        db.finish_scan_run(run,"20260921","WARNING","历史日期无可回放的全市场快照",{},
                           "WARNING","SUCCESS","now",[],warning_type="HISTORICAL_REBUILD")
        monkeypatch.setattr(web_module,"is_trade_date",lambda date:True)
        monkeypatch.setattr(web_module,"previous_trade_date",lambda date:None)
        monkeypatch.setattr(web_module,"next_trade_date",lambda date:None)
        monkeypatch.setattr(web_module,"month_days",lambda month:[{"date":"20260921","is_trade":True}])
        page=TestClient(app).get("/?date=20260921")
        assert page.status_code==200
        assert "已识别涨停" in page.text
        assert "历史重建结果，可能受免费数据源历史覆盖限制" in page.text
        assert "全市场涨停 / 完整" not in page.text
    finally:
        object.__setattr__(settings,"db_path",original)


def test_warning_type_friendly_labels():
    assert web_module.WARNING_LABELS["HISTORICAL_REBUILD"].startswith("历史重建结果")
    assert "主数据源异常" in web_module.WARNING_LABELS["CURRENT_SOURCE_DEGRADED"]
    assert "双源核验" in web_module.WARNING_LABELS["PARTIAL_VERIFICATION"]


def test_manual_review_api_and_cached_detail(tmp_path, monkeypatch):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"detail.db"))
    market={"symbol":"601567","name":"三星电气","industry":"电网设备","market":"MAIN","close":42.3,
            "total_market_cap_yi":228.3,"pct_chg":10.0,"recent_limit_count":2,"consecutive_boards":1,
            "board_label":"首板","is_t_board":False,"is_one_word":False,"limit_dates":["20260914","20260924"],
            "verification_status":"VERIFIED"}
    profile={"as_of_date":"20260924","last_trade_date":"20260924","observations":1250,
             "one_year":{"available":True,"position":.2,"distance_from_high":-.3,"distance_from_low":.1},
             "three_year":{"available":True,"position":.3,"distance_from_high":-.4,"distance_from_low":.2},
             "five_year":{"available":True,"position":.4,"distance_from_high":-.5,"distance_from_low":.3},
             "ma250":{"available":True,"distance":.01,"direction":"上行"},"maximum_rise_5y":None}
    try:
        db.init_db(); run=db.start_scan_run("20260924","now")
        db.finish_scan_run(run,"20260924","SUCCESS","ok",{},"SUCCESS","SUCCESS","now",[],[market])
        db.save_profile_cache("601567","20260924",profile,"test")
        db.save_fundamental_cache("601567","20260924",{"report_date":"2026-06-30","revenue":1e9,
            "revenue_yoy":5,"net_profit":1e8,"net_profit_yoy":3,"profit_status":"盈利"},"test")
        client=TestClient(app)
        saved=client.post("/api/reviews",json={"trade_date":"20260924","symbol":"601567","rating":"FOCUS",
                                                "reasons":["位置好","业绩好"],"note":"等回调"})
        assert saved.status_code==200 and saved.json()["review"]["rating"]=="FOCUS"
        updated=client.post("/api/reviews",json={"trade_date":"20260924","symbol":"601567","rating":"NORMAL",
                                                  "reasons":[],"note":"再观察"})
        assert updated.status_code==200 and db.reviews_for_date("20260924")["601567"]["note"]=="再观察"
        detail=client.get("/api/details/601567?date=20260924")
        assert detail.status_code==200
        assert detail.json()["profile"]["last_trade_date"]=="20260924"
        assert detail.json()["fundamental"]["profit_status"]=="盈利"
        page=client.get("/?date=20260924")
        assert page.status_code==200 and "人工复盘进度" in page.text and "查看详情" in page.text
    finally:
        object.__setattr__(settings,"db_path",original)


def test_fundamental_failure_does_not_affect_homepage(tmp_path, monkeypatch):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"fundamental-failure.db"))
    try:
        db.init_db(); run=db.start_scan_run("20260924","now")
        db.finish_scan_run(run,"20260924","SUCCESS","ok",{},"SUCCESS","SUCCESS","now",[])
        monkeypatch.setattr(AKShareSource,"fundamental_summary",lambda *args,**kwargs: (_ for _ in ()).throw(RuntimeError("down")))
        assert TestClient(app).get("/?date=20260924").status_code==200
    finally:
        object.__setattr__(settings,"db_path",original)


def test_historical_news_is_withheld_but_latest_news_still_loads(tmp_path, monkeypatch):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"news-lookahead.db"))
    try:
        db.init_db()
        for date in ("20260921","20260924"):
            run=db.start_scan_run(date,"now")
            db.finish_scan_run(run,date,"SUCCESS","ok",{},"SUCCESS","SUCCESS","now",[])
        calls=[]
        monkeypatch.setattr(AKShareSource,"news",lambda self,symbol:(calls.append(symbol) or
            [{"title":"09-24新闻","published_at":"2026-09-24 10:00","source":"测试"}]))
        client=TestClient(app)
        historical=client.get("/api/news/601567?as_of_date=20260921")
        assert historical.status_code==200 and historical.json()["withheld"] is True
        assert historical.json()["items"]==[] and calls==[]
        monkeypatch.setattr(web_module,"current_news_allowed",lambda date,now=None:date=="20260924")
        latest=client.get("/api/news/601567?as_of_date=20260924")
        assert latest.status_code==200 and latest.json()["items"][0]["published_at"].startswith("2026-09-24")
        assert calls==["601567"]
    finally:
        object.__setattr__(settings,"db_path",original)


def test_news_requires_today_trade_day_after_publish_hour(monkeypatch):
    tz=ZoneInfo("Asia/Shanghai")
    monkeypatch.setattr(web_module,"is_trade_date",lambda date:date=="20260928")
    assert not web_module.current_news_allowed("20260925",datetime(2026,9,26,20,tzinfo=tz))  # 周六看周五
    assert not web_module.current_news_allowed("20260925",datetime(2026,9,28,10,tzinfo=tz))  # 周一上午看周五
    assert web_module.current_news_allowed("20260928",datetime(2026,9,28,20,tzinfo=tz))
    assert not web_module.current_news_allowed("20260921",datetime(2026,9,28,20,tzinfo=tz))


def test_background_refresh_is_immediate_and_pages_remain_responsive(tmp_path, monkeypatch):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"async.db"))
    try:
        db.init_db(); run=db.start_scan_run("20260924","now")
        db.finish_scan_run(run,"20260924","SUCCESS","ok",{},"SUCCESS","SUCCESS","now",[])
        launched=threading.Event()
        def fake_launch(job_id,date):
            launched.set(); return 77777
        monkeypatch.setattr(web_module,"_launch_refresh_job",fake_launch)
        monkeypatch.setattr(web_module,"is_trade_date",lambda date:True)
        monkeypatch.setattr(web_module,"previous_trade_date",lambda date:"20260917")
        monkeypatch.setattr(web_module,"next_trade_date",lambda date:"20260921")
        monkeypatch.setattr(web_module,"month_days",lambda month:[{"date":"20260918","is_trade":True},{"date":"20260924","is_trade":True}])
        client=TestClient(app); started=time.monotonic()
        response=client.post("/api/refresh?date=20260918")
        assert response.status_code==202 and time.monotonic()-started<0.5 and launched.is_set()
        assert client.get("/?date=20260924").status_code==200
        assert client.get("/?date=20260918").status_code==200
        assert client.get("/healthz").status_code==200
        status=client.get("/api/refresh-status").json()
        assert status["running"] and status["trade_date"]=="20260918"
        second=client.post("/api/refresh?date=20260915")
        assert second.status_code==409 and "20260918 正在后台生成" in second.json()["message"]
    finally:
        object.__setattr__(settings,"db_path",original)


def test_background_worker_finishes_after_user_leaves_and_publishes(tmp_path, monkeypatch):
    from app import refresh_worker
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"worker.db"))
    try:
        db.init_db(); job=db.start_background_refresh("20260918")
        def fake_sync(date,run_scan=True,force=False):
            run=db.start_scan_run(date,"now")
            db.finish_scan_run(run,date,"WARNING","historical",{},"WARNING","SUCCESS","now",[])
        monkeypatch.setattr(refresh_worker,"sync_market_day",fake_sync)
        thread=threading.Thread(target=refresh_worker.run,args=(job,"20260918")); thread.start()
        assert TestClient(app).get("/healthz").status_code==200
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert db.published_run_for_date("20260918")["status"]=="WARNING"
        assert db.latest_background_refresh()["status"]=="WARNING"
    finally:
        object.__setattr__(settings,"db_path",original)


def test_failed_background_worker_keeps_old_snapshot_and_manual_review(tmp_path, monkeypatch):
    from app import refresh_worker
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"worker-fail.db"))
    try:
        db.init_db(); old=db.start_scan_run("20260924","now")
        db.finish_scan_run(old,"20260924","SUCCESS","old",{},"SUCCESS","SUCCESS","now",[])
        with db.connect() as conn:
            now=db.utcnow(); conn.execute("INSERT INTO manual_reviews VALUES(?,?,?,?,?,?,?,?)",
                ("20260924","601567","FOCUS","[]","等回调",now,now,"HINDSIGHT")); conn.commit()
        job=db.start_background_refresh("20260924")
        monkeypatch.setattr(refresh_worker,"sync_market_day",lambda *args,**kwargs:(_ for _ in ()).throw(RuntimeError("network down")))
        refresh_worker.run(job,"20260924")
        assert db.latest_background_refresh()["status"]=="FAILED"
        assert db.published_run_for_date("20260924")["run_id"]==old
        assert db.reviews_for_date("20260924")["601567"]["note"]=="等回调"
    finally:
        object.__setattr__(settings,"db_path",original)


def test_startup_recovers_dead_background_job_without_losing_snapshot(tmp_path, monkeypatch):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"recover.db"))
    try:
        db.init_db(); first=db.start_scan_run("20260924","now")
        db.finish_scan_run(first,"20260924","SUCCESS","old",{},"SUCCESS","SUCCESS","now",[])
        stale=db.start_scan_run("20260924","now")
        job=db.start_background_refresh("20260924"); db.set_background_refresh_pid(job,99999999)
        monkeypatch.setattr(web_module,"_pid_alive",lambda pid:False)
        web_module._startup()
        assert db.latest_background_refresh()["status"]=="INTERRUPTED"
        assert db.active_run_for_date("20260924")["status"]=="FAILED"
        assert db.published_run_for_date("20260924")["run_id"]==first
    finally:
        object.__setattr__(settings,"db_path",original)


def test_static_assets_and_dashboard_are_served(tmp_path, monkeypatch):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"assets.db"))
    try:
        db.init_db(); monkeypatch.setattr(web_module,"is_trade_date",lambda date:False)
        monkeypatch.setattr(web_module,"previous_trade_date",lambda date:None)
        monkeypatch.setattr(web_module,"next_trade_date",lambda date:None)
        monkeypatch.setattr(web_module,"month_days",lambda month:[])
        client=TestClient(app)
        assert client.get("/?date=20260926").status_code==200
        for path in ("/static/app.css","/static/review.js","/static/refresh.js","/static/echarts.min.js"):
            assert client.get(path).status_code==200
    finally:
        object.__setattr__(settings,"db_path",original)
