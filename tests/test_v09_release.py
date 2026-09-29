import json
import subprocess
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from app import db
from app.config import settings
from app.profile import build_profile
from app.web import app
import app.web as web_module


def _market(symbol="601567", date="20260924"):
    return {"symbol":symbol,"name":"三星电气","industry":"电网设备","market":"MAIN","close":11.0,
            "total_market_cap_yi":128.0,"pct_chg":10.0,"recent_limit_count":2,"consecutive_boards":1,
            "board_label":"首板","is_t_board":True,"is_one_word":False,"limit_dates":[date],
            "verification_status":"VERIFIED"}


def _publish(date, market_rows=None):
    run=db.start_scan_run(date,"now")
    db.finish_scan_run(run,date,"SUCCESS","ok",{},"SUCCESS","SUCCESS","now",[],market_rows or [])


def _history(as_of="20260924"):
    dates=pd.bdate_range(end=pd.Timestamp(as_of)+pd.Timedelta(days=4),periods=282).strftime("%Y%m%d").tolist()
    dates[-3]=as_of
    dates=sorted(set(dates))
    n=len(dates); close=[10.0]*n; volume=[100.0]*n
    target=dates.index(as_of); close[target]=11.0; volume[target]=300.0
    if target+1<n: close[target+1]=99.0  # must never leak through historical as_of
    return pd.DataFrame({"trade_date":dates,"open":[10.0]*n,"high":[10.2]*n,"low":[9.8]*n,
                         "close":close,"adjusted_close":close,"raw_close":close,"volume":volume})


def test_watchlist_full_lifecycle_immutable_trigger_and_survives_next_day_scans(tmp_path):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"watch.db"))
    try:
        db.init_db(); _publish("20260924",[_market()])
        first=db.save_watchlist_item("601567","三星电气","20260924",11.0,"T_BOARD",["T字板"])
        assert first["status"]=="ACTIVE"
        assert db.update_watchlist_item("601567","PAUSED")["status"]=="PAUSED"
        assert db.update_watchlist_item("601567","ACTIVE")["status"]=="ACTIVE"
        _publish("20260925",[])  # no limit-up on the following trading day
        assert db.get_watchlist_item("601567")["status"]=="ACTIVE"
        # Same semantics as refresh/backfill: a new published scan cannot replace watchlist identity fields.
        _publish("20260926",[])
        again=db.save_watchlist_item("601567","三星电气","20260926",99.0,"MANUAL",[])
        assert again["added_date"]=="20260924" and again["trigger_price"]==11.0
        assert db.update_watchlist_item("601567","ARCHIVED")["status"]=="ARCHIVED"
        assert db.watchlist_items()==[]
        assert db.watchlist_items(include_archived=True)[0]["symbol"]=="601567"
    finally:
        object.__setattr__(settings,"db_path",original)


def test_custom_preset_api_save_load_delete_and_system_presets_are_not_deletable(tmp_path):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"preset.db"))
    try:
        db.init_db(); client=TestClient(app)
        created=client.post("/api/filter-presets",json={"name":"盈利T字","scope":"BOTH",
            "filters":{"board":"T","fundamental":"PROFIT","cap":"80_200"}})
        assert created.status_code==200; preset_id=created.json()["item"]["id"]
        loaded=client.get("/api/filter-presets").json()["rows"]
        assert loaded==[{**created.json()["item"]}]
        assert client.delete(f"/api/filter-presets/{preset_id}").status_code==200
        assert client.get("/api/filter-presets").json()["rows"]==[]
        # System presets live in immutable client definitions, never in the deletable DB table/API.
        js=(Path(__file__).parents[1]/"app/static/review.js").read_text()
        assert js.count('id:"sys-')==6
        assert 'String(p.id).startsWith("sys-")' in js
        assert client.delete("/api/filter-presets/sys-all").status_code==422
    finally:
        object.__setattr__(settings,"db_path",original)


def test_combined_frontend_filters_execute_together():
    root=Path(__file__).parents[1]; js_path=root/"app/static/review.js"
    node_script=r'''
const fs=require("fs"),vm=require("vm");
const ids=["filterSearch","filterBoard","filterIndustry","filterActivity","filterPosition","filterHigh","filterMa","filterFundamental","filterCap","filterRating","presetSelect","presetManager","dailyList","watchlistList","filterCount"];
const controls={}; for(const id of ids) controls[id]={value:"",innerHTML:"",textContent:"",addEventListener(){}};
const ctx={console,window:{REVIEWS:{},MARKET_ROWS:[],WATCHLIST_ROWS:[],USER_PRESETS:[{id:9,name:"自定义",scope:"BOTH",filters:{}}]},
 document:{getElementById:id=>controls[id],querySelectorAll:()=>[],querySelector:()=>null,createElement:()=>({textContent:"",innerHTML:""})},
 alert(){},prompt(){return null},confirm(){return false},fetch(){},navigator:{},echarts:{}};
for(const [k,v] of Object.entries(controls))ctx[k]=v; vm.createContext(ctx);vm.runInContext(fs.readFileSync(process.argv[1],"utf8"),ctx);
controls.filterSearch.value="三星";controls.filterBoard.value="T";controls.filterPosition.value="3Y30";
controls.filterFundamental.value="PROFIT";controls.filterCap.value="80_200";
const row={symbol:"601567",name:"三星电气",industry:"电网设备",is_t_board:true,total_market_cap_yi:128,
 profile:{three_year:{available:true,position:.2}},fundamental:{profit_status:"盈利"},review:{rating:"UNSET"}};
if(!ctx.matches(row))process.exit(2); if(ctx.matches({...row,total_market_cap_yi:220}))process.exit(3);
if((controls.presetManager.innerHTML.match(/系统预设/g)||[]).length!==6)process.exit(4);
if((controls.presetManager.innerHTML.match(/删除/g)||[]).length!==1)process.exit(5);
'''
    result=subprocess.run(["node","-e",node_script,str(js_path)],cwd=root,capture_output=True,text=True)
    assert result.returncode==0, result.stderr


def test_kline_profile_events_and_historical_as_of_have_no_future_data(tmp_path,monkeypatch):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"kline.db"))
    calls=[]; frame=_history()
    def fake_history(self,symbol,start_date,end_date,*,adjusted):
        calls.append((end_date,adjusted)); out=frame.copy()
        if adjusted: out=out.drop(columns=["raw_close"])
        else: out=out.drop(columns=["adjusted_close"])
        return out
    try:
        db.init_db(); _publish("20260924",[_market()])
        monkeypatch.setattr(web_module.AKShareSource,"tencent_history",fake_history)
        monkeypatch.setattr(web_module.AKShareSource,"fundamental_summary",lambda self,symbol,as_of:
            {"report_date":"2026-06-30","revenue":1,"revenue_yoy":1,"net_profit":1,"net_profit_yoy":1,"profit_status":"盈利"})
        payload=TestClient(app).get("/api/details/601567?date=20260924").json()
        assert calls and all(end=="20260924" for end,_ in calls)
        assert payload["profile"]["as_of_date"]=="20260924" and payload["profile"]["ma250"]["available"]
        assert max(bar[0] for bar in payload["kline"]["bars"])=="20260924"
        events={(e["date"],e["type"]) for e in payload["kline"]["events"]}
        assert ("20260924","T") in events and ("20260924","V") in events
        assert payload["kline"]["volume_events"][0]["ratio"]==3.0
        assert payload["fundamental"]["report_date"]=="2026-06-30"
    finally:
        object.__setattr__(settings,"db_path",original)


def test_historical_strong_pullback_label_inputs_and_watchlist_current_date(tmp_path):
    dates=pd.bdate_range("2025-01-01",periods=300).strftime("%Y%m%d")
    values=[10.0]*50+[10+i*(20/99) for i in range(100)]+[30-i*(15/149) for i in range(150)]
    frame=pd.DataFrame({"trade_date":dates,"adjusted_close":values})
    profile=build_profile(frame,dates[-1]); rise=profile["maximum_rise_5y"]
    assert rise["gain"]>=1 and rise["current_from_high"]<=-.4
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"current.db"))
    try:
        db.init_db(); _publish("20260924",[_market()]); _publish("20260925",[])
        db.save_watchlist_item("601567","三星电气","20260924",11.0)
        db.save_profile_cache("601567","20260925",{"current_close":10.0,"high_since_watch":12.0},"test")
        response=TestClient(app).get("/api/watchlist").json()
        assert response["as_of_date"]=="20260925"
        assert response["rows"][0]["latest_close"]==10.0
    finally:
        object.__setattr__(settings,"db_path",original)


def test_background_refresh_compatibility_keeps_watchlist(tmp_path,monkeypatch):
    original=settings.db_path; object.__setattr__(settings,"db_path",str(tmp_path/"refresh.db"))
    try:
        db.init_db(); _publish("20260924",[_market()]); db.save_watchlist_item("601567","三星电气","20260924",11.0)
        monkeypatch.setattr(web_module,"is_trade_date",lambda date:True)
        monkeypatch.setattr(web_module,"_launch_refresh_job",lambda job_id,date:12345)
        response=TestClient(app).post("/api/refresh?date=20260924")
        assert response.status_code==202 and db.get_watchlist_item("601567")["trigger_price"]==11.0
    finally:
        object.__setattr__(settings,"db_path",original)
