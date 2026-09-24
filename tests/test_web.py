from fastapi.testclient import TestClient

from app import db
from app.config import settings
from app.web import app, refresh_lock


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
         "verification_message":"双源一致","reject_reasons":["前次涨停后回撤-0.3%，要求3%～25%"]}
    try:
        db.init_db(); db.mark_pipeline_start("20260924"); db.save_stage_results("20260924",[row])
        counts={"pool_rows":52,"cap_rows":11,"repeated_rows":6,"nonconsecutive_rows":3,
                "low_rows":1,"pullback_rows":0,"pattern_rows":0,"candidate_rows":0}
        db.mark_pipeline_finish("20260924","WARNING","DataSourceError",counts,"WARNING","SUCCESS","2026-09-24 20:00 Asia/Shanghai")
        response=TestClient(app).get("/")
        assert response.status_code==200
        assert "今天没有完全满足所有条件的股票" in response.text
        assert "三星电气" in response.text
        assert "前次涨停后回撤-0.3%" in response.text
        assert "查看技术详情" in response.text
        assert "重新抓取今日数据" in response.text
        assert refresh_lock.acquire(blocking=False)
        try:
            busy=TestClient(app).post("/api/refresh")
            assert busy.status_code==409
            assert busy.json()["message"]=="数据正在抓取，请稍候。"
        finally:
            refresh_lock.release()
    finally:
        object.__setattr__(settings,"db_path",original)
