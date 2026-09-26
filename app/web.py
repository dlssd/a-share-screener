from __future__ import annotations

import json
import os
import secrets
import subprocess
import sys
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import PROJECT_ROOT, settings
from .calendar import CalendarUnavailable, is_trade_date, month_days, next_trade_date, previous_trade_date
from .db import (active_run_for_date, connect, get_news_cache, init_db, published_dates,
                 published_run_for_date, save_news_cache, stage_rows_for_run,
                 market_rows_for_run, market_environment_for_run)
from .db import (reviews_for_date, save_manual_review, get_profile_cache, save_profile_cache,
                 get_fundamental_cache, save_fundamental_cache)
from .db import (finish_background_refresh, latest_background_refresh,
                 recover_interrupted_refreshes,
                 set_background_refresh_pid, start_background_refresh)
from .datasource import AKShareSource
from .profile import build_profile

app = FastAPI(title="A股低位多涨停筛选器", version="0.8.3")
app.mount("/static",StaticFiles(directory=str(Path(__file__).parent / "static")),name="static")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
security = HTTPBasic(auto_error=False)
refresh_lock = threading.Lock()
RATINGS={"FOCUS","NORMAL","REJECT","UNSET"}
REASONS={"位置好","T字板","题材好","业绩好","前期有大行情","放量明显","位置偏高","连板过多","其他"}


class ReviewPayload(BaseModel):
    trade_date: str
    symbol: str
    rating: str
    reasons: list[str]=Field(default_factory=list)
    note: str=""
WARNING_LABELS={
    "HISTORICAL_REBUILD":"历史重建结果，可能受免费数据源历史覆盖限制",
    "CURRENT_SOURCE_DEGRADED":"当前主数据源异常，已使用降级核验路径",
    "PARTIAL_VERIFICATION":"部分股票或字段未能完成双源核验",
}


def _warning_type(selected) -> str | None:
    if not selected or selected["status"]!="WARNING": return None
    value=selected["warning_type"] if "warning_type" in selected.keys() else None
    if value: return value
    message=str(selected["message"] or "")
    if "历史日期" in message or "历史重建" in message: return "HISTORICAL_REBUILD"
    if "全市场收盘快照失败" in message: return "CURRENT_SOURCE_DEGRADED"
    return "PARTIAL_VERIFICATION"


def auth(credentials: Optional[HTTPBasicCredentials] = Depends(security)) -> None:
    if not settings.app_username and not settings.app_password:
        return
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Basic"},
        )
    ok_user = secrets.compare_digest(credentials.username, settings.app_username)
    ok_pass = secrets.compare_digest(credentials.password, settings.app_password)
    if not (ok_user and ok_pass):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Basic"},
        )


@app.on_event("startup")
def _startup() -> None:
    init_db()
    recover_interrupted_refreshes(_pid_alive)


def _windows_pid_alive(pid: int, kernel32=None) -> bool:
    """Check a Windows process without sending a signal or terminating it."""
    import ctypes
    if pid<=0:
        return False
    if kernel32 is None:
        from ctypes import wintypes
        kernel32=ctypes.WinDLL("kernel32",use_last_error=True)
        kernel32.OpenProcess.argtypes=(wintypes.DWORD,wintypes.BOOL,wintypes.DWORD)
        kernel32.OpenProcess.restype=wintypes.HANDLE
        kernel32.GetExitCodeProcess.argtypes=(wintypes.HANDLE,ctypes.POINTER(wintypes.DWORD))
        kernel32.GetExitCodeProcess.restype=wintypes.BOOL
        kernel32.CloseHandle.argtypes=(wintypes.HANDLE,)
        kernel32.CloseHandle.restype=wintypes.BOOL
    # PROCESS_QUERY_LIMITED_INFORMATION is enough for a worker owned by the
    # current user and has no side effects on that process.
    handle=kernel32.OpenProcess(0x1000,False,pid)
    if not handle:
        return False
    try:
        exit_code=ctypes.c_ulong()
        return bool(kernel32.GetExitCodeProcess(handle,ctypes.byref(exit_code)) and exit_code.value==259)
    finally:
        kernel32.CloseHandle(handle)


def _pid_alive(pid: int, platform_name: str | None=None) -> bool:
    platform_name=platform_name or os.name
    if platform_name=="nt":
        try:
            return _windows_pid_alive(pid)
        except (OSError,ValueError):
            return False
    try:
        os.kill(pid,0)
        return True
    except (OSError,ValueError):
        return False


def _worker_process_options(platform_name: str | None=None) -> dict:
    platform_name=platform_name or os.name
    if platform_name=="nt":
        return {"creationflags":(
            getattr(subprocess,"CREATE_NEW_PROCESS_GROUP",0x00000200) |
            getattr(subprocess,"CREATE_NO_WINDOW",0x08000000)
        )}
    return {"start_new_session":True}


def _launch_refresh_job(job_id: int, trade_date: str) -> int:
    """Start a detached scan process and return immediately."""
    env=os.environ.copy(); env["DB_PATH"]=settings.db_path
    process=subprocess.Popen(
        [sys.executable,"-m","app.refresh_worker",str(job_id),trade_date],
        cwd=str(PROJECT_ROOT),env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
        **_worker_process_options(),
    )
    set_background_refresh_pid(job_id,process.pid)
    return process.pid


def _rows_for_date(trade_date: str) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM scan_results
            WHERE trade_date=?
            ORDER BY low_position_pct ASC, limit_up_count DESC, total_market_cap_yi DESC
            """,
            (trade_date,),
        ).fetchall()
    out = []
    for row in rows:
        r = dict(row)
        r["limit_dates"] = json.loads(r.pop("limit_dates_json"))
        out.append(r)
    return out


def _stage_rows_for_date(trade_date: str) -> list[dict]:
    published=published_run_for_date(trade_date)
    if not published:
        return []
    rows=stage_rows_for_run(int(published["run_id"]))
    out=[]
    boolean_fields=("has_consecutive_limit_up","passes_market_cap","passes_repeat_limit",
                    "passes_nonconsecutive","passes_low","passes_pullback",
                    "passes_recent_return","passes_final")
    for row in rows:
        item=dict(row)
        item["limit_dates"]=json.loads(item.pop("limit_dates_json"))
        item["reject_reasons"]=json.loads(item.pop("reject_reasons_json"))
        for field in boolean_fields:
            item[field]=None if item[field] is None else bool(item[field])
        out.append(item)
    return out


def _watch_score(row: dict) -> tuple:
    """Fewest failed final rules first, then smallest normalized miss."""
    failed=sum(not row[key] for key in ("passes_low","passes_pullback","passes_recent_return"))
    low_gap=max(0.0,(row.get("low_position_pct") or 0)-settings.max_low_position)
    pullback=row.get("pullback_pct")
    pullback_gap=(settings.min_pullback_pct if pullback is None else
                  max(0.0,settings.min_pullback_pct-pullback,pullback-settings.max_pullback_pct))
    return failed,low_gap+pullback_gap,-row.get("limit_up_count",0)


@app.get("/", response_class=HTMLResponse, dependencies=[Depends(auth)])
def index(request: Request, date: Optional[str] = None):
    dates=published_dates()
    chosen=(date.replace("-", "") if date else (dates[-1] if dates else datetime.now(ZoneInfo(settings.timezone)).strftime("%Y%m%d")))
    if len(chosen)!=8 or not chosen.isdigit():
        raise HTTPException(400,"日期格式应为 YYYYMMDD")
    try:
        trade_day=is_trade_date(chosen)
        previous_date=previous_trade_date(chosen)
        next_date=next_trade_date(chosen)
        days=month_days(chosen[:6])
    except CalendarUnavailable:
        trade_day=chosen in dates; previous_date=next_date=None; days=[]
    published=set(dates)
    for item in days: item["published"]=item["date"] in published
    first=datetime.strptime(chosen[:6]+"01","%Y%m%d"); leading=first.weekday()
    weeks=[]; cells=[None]*leading+days
    while cells:
        weeks.append(cells[:7]); cells=cells[7:]
    now=datetime.now(ZoneInfo(settings.timezone)); today=now.strftime("%Y%m%d")
    completed=chosen<today or (chosen==today and trade_day and now.hour>=settings.publish_after_hour)
    selected=published_run_for_date(chosen) if chosen else None
    active=active_run_for_date(chosen) if chosen else None
    valid=bool(selected)
    stages=_stage_rows_for_date(chosen) if chosen and valid else []
    official=[r for r in stages if r["passes_final"]]
    watch=sorted((r for r in stages if r["passes_nonconsecutive"] and not r["passes_final"]),key=_watch_score)
    repeated=sorted((r for r in stages if r["passes_repeat_limit"]),key=lambda r:(r["has_consecutive_limit_up"],-r["limit_up_count"],r["symbol"]))
    market_rows=market_rows_for_run(int(selected["run_id"])) if selected else []
    reviews=reviews_for_date(chosen) if chosen else {}
    for row in market_rows:
        review=reviews.get(row["symbol"],{"rating":"UNSET","reasons":[],"note":""})
        row["review"]=review
    review_counts={rating:sum(r["review"]["rating"]==rating for r in market_rows) for rating in RATINGS}
    review_counts["reviewed"]=len(market_rows)-review_counts["UNSET"]
    environment=market_environment_for_run(int(selected["run_id"])) if selected else None
    warning_type=_warning_type(selected); warning_label=WARNING_LABELS.get(warning_type)
    background_job=latest_background_refresh()
    market_complete=bool(selected and selected["status"]=="SUCCESS" and selected["akshare_status"]=="SUCCESS")
    board_stats={"first":sum(r["consecutive_boards"]<=1 for r in market_rows),
                 "two":sum(r["consecutive_boards"]==2 for r in market_rows),
                 "three":sum(r["consecutive_boards"]==3 for r in market_rows),
                 "four_plus":sum(r["consecutive_boards"]>=4 for r in market_rows),
                 "t":sum(bool(r["is_t_board"]) for r in market_rows),
                 "one":sum(bool(r["is_one_word"]) for r in market_rows),
                 "nonconsecutive":sum(r["recent_limit_count"]>=2 and r["consecutive_boards"]<=1 for r in market_rows)}
    industry_counts={}
    for row in market_rows:
        key=row.get("industry") or "行业未知"; industry_counts[key]=industry_counts.get(key,0)+1
    industries=sorted(industry_counts.items(),key=lambda x:(-x[1],x[0]))
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "official":official,"watch":watch,"repeated":repeated,"cap_rows":stages,
            "market_rows":market_rows,"board_stats":board_stats,"industries":industries,"environment":environment,
            "warning_type":warning_type,"warning_label":warning_label,"market_complete":market_complete,
            "review_counts":review_counts,"review_reasons":sorted(REASONS),
            "reviews":reviews,
            "background_job":dict(background_job) if background_job else None,
            "chosen_date": chosen,
            "selected":dict(selected) if selected else None,
            "active":dict(active) if active else None,
            "refreshing":bool(active and active["status"]=="RUNNING" and (not selected or active["run_id"]!=selected["run_id"])),
            "refresh_failed":bool(active and active["status"]=="FAILED" and selected and active["run_id"]!=selected["run_id"]),
            "previous_date":previous_date,"next_date":next_date,
            "is_trade_day":trade_day,"is_completed":completed,"calendar_weeks":weeks,
            "calendar_month":chosen[:6],"published_dates":published,
            "previous_month":(first-timedelta(days=1)).strftime('%Y%m'),
            "next_month":((first.replace(day=28)+timedelta(days=4)).replace(day=1)).strftime('%Y%m'),
            "settings": settings,
        },
    )


@app.get("/api/results", dependencies=[Depends(auth)])
def api_results(date: Optional[str] = None):
    dates=published_dates(); chosen=date or (dates[-1] if dates else None)
    published=published_run_for_date(chosen) if chosen else None
    rows=[]
    if published:
        rows=[r for r in _stage_rows_for_date(chosen) if r["passes_final"]]
    return {"trade_date":chosen,"status":published["status"] if published else "NOT_RUN","rows":rows}


@app.post("/api/refresh", dependencies=[Depends(auth)])
def refresh_latest(date: Optional[str] = None):
    if not refresh_lock.acquire(blocking=False):
        return JSONResponse({"ok":False,"message":"已有后台任务正在启动，请稍候。"},status_code=409)
    try:
        trade_date=(date or (published_dates()[-1] if published_dates() else None))
        if not trade_date or len(trade_date.replace("-","")) != 8:
            return JSONResponse({"ok":False,"message":"请先选择日期。"},status_code=400)
        trade_date=trade_date.replace("-","")
        now=datetime.now(ZoneInfo(settings.timezone)); today=now.strftime("%Y%m%d")
        if not is_trade_date(trade_date):
            return JSONResponse({"ok":False,"message":f"{trade_date} 为A股休市日，不能生成盘后复盘。"},status_code=400)
        if trade_date>today or (trade_date==today and now.hour<settings.publish_after_hour):
            return JSONResponse({"ok":False,"message":f"{trade_date} 尚非完整盘后交易日。"},status_code=400)
        try:
            job_id=start_background_refresh(trade_date)
        except RuntimeError as exc:
            return JSONResponse({"ok":False,"message":str(exc)},status_code=409)
        try:
            _launch_refresh_job(job_id,trade_date)
        except Exception as exc:
            finish_background_refresh(job_id,"FAILED",f"后台进程启动失败: {type(exc).__name__}: {exc}")
            return JSONResponse({"ok":False,"message":"后台任务启动失败，请查看技术详情。",
                                 "detail":str(exc)},status_code=503)
        return JSONResponse({"ok":True,"status":"RUNNING","trade_date":trade_date,"job_id":job_id},status_code=202)
    except Exception as exc:
        return JSONResponse({"ok":False,"message":"重新抓取失败，请稍后再试或检查行情网络直连设置。",
                             "detail":f"{type(exc).__name__}: {exc}"},status_code=503)
    finally:
        refresh_lock.release()


@app.get("/api/refresh-status", dependencies=[Depends(auth)])
def refresh_status():
    job=latest_background_refresh()
    if job and job["status"]=="RUNNING" and job["worker_pid"] and not _pid_alive(int(job["worker_pid"])):
        recover_interrupted_refreshes(_pid_alive)
        job=latest_background_refresh()
    if not job:
        return {"running":False,"trade_date":None,"started_at":None,"status":"IDLE","message":""}
    data=dict(job)
    return {"running":data["status"]=="RUNNING","trade_date":data["trade_date"],
            "started_at":data["started_at"],"finished_at":data["finished_at"],
            "status":data["status"],"message":data["message"] or "","job_id":data["job_id"]}


def current_news_allowed(as_of_date: str, now: datetime | None=None) -> bool:
    """Current-news feeds are safe only on today's completed trading page."""
    now=now or datetime.now(ZoneInfo(settings.timezone))
    if now.tzinfo is None:
        now=now.replace(tzinfo=ZoneInfo(settings.timezone))
    else:
        now=now.astimezone(ZoneInfo(settings.timezone))
    today=now.strftime("%Y%m%d")
    return (as_of_date==today and now.hour>=settings.publish_after_hour and
            is_trade_date(today))


@app.get("/api/news/{symbol}", dependencies=[Depends(auth)])
def news(symbol: str, as_of_date: Optional[str]=None):
    symbol=str(symbol).zfill(6)
    now=datetime.now(ZoneInfo(settings.timezone))
    as_of_date=(as_of_date or now.strftime("%Y%m%d")).replace("-","")
    if not current_news_allowed(as_of_date,now):
        return {"symbol":symbol,"as_of_date":as_of_date,"withheld":True,"items":[],
                "message":"为避免未来信息影响复盘，仅当日盘后页面展示当前资讯。"}
    query_date=now.strftime("%Y%m%d")
    cached=get_news_cache(symbol,query_date)
    if cached:
        return {"symbol":symbol,"cached":True,"items":json.loads(cached["news_json"])}
    try:
        items=AKShareSource().news(symbol)
        save_news_cache(symbol,query_date,items,"akshare_stock_news_em")
        return {"symbol":symbol,"cached":False,"items":items}
    except Exception:
        return JSONResponse({"symbol":symbol,"message":"资讯暂时无法获取"},status_code=502)


@app.post("/api/reviews", dependencies=[Depends(auth)])
def save_review(payload: ReviewPayload):
    trade_date=payload.trade_date.replace("-",""); symbol=payload.symbol.zfill(6); rating=payload.rating.upper()
    if len(trade_date)!=8 or not trade_date.isdigit() or rating not in RATINGS:
        raise HTTPException(400,"人工复盘参数无效")
    invalid=set(payload.reasons)-REASONS
    if invalid: raise HTTPException(400,f"未知复盘原因: {', '.join(sorted(invalid))}")
    with connect() as conn:
        exists=conn.execute("""SELECT 1 FROM published_snapshots p JOIN market_limit_results m ON m.run_id=p.run_id
                               WHERE p.trade_date=? AND m.symbol=?""",(trade_date,symbol)).fetchone()
    if not exists: raise HTTPException(404,"该股票不在所选日期的已识别涨停列表")
    return {"ok":True,"review":save_manual_review(trade_date,symbol,rating,payload.reasons,payload.note[:1000])}


@app.get("/api/details/{symbol}", dependencies=[Depends(auth)])
def stock_details(symbol: str, date: str):
    symbol=str(symbol).zfill(6); as_of=date.replace("-","")
    with connect() as conn:
        row=conn.execute("""SELECT m.* FROM published_snapshots p JOIN market_limit_results m ON m.run_id=p.run_id
                            WHERE p.trade_date=? AND m.symbol=?""",(as_of,symbol)).fetchone()
    if not row: raise HTTPException(404,"未找到该日股票记录")
    profile_cached=get_profile_cache(symbol,as_of)
    if profile_cached:
        profile=json.loads(profile_cached["profile_json"])
        rise=profile.get("maximum_rise_5y")
        if rise and "label" not in rise:
            days=min(int(profile.get("observations",0)),1250)
            rise.update({"label":"近5年最大上涨" if days>=1250 else "可用历史最大上涨",
                         "observation_days":days,"approx_years":round(days/250,1)})
    else:
        try:
            history=AKShareSource().tencent_history(symbol,"20100101",as_of,adjusted=True)
            profile=build_profile(history,as_of); save_profile_cache(symbol,as_of,profile,"tencent_qfq")
        except Exception as exc:
            profile={"available":False,"message":"历史画像暂不可用","technical":f"{type(exc).__name__}: {exc}"}
    fundamental_cached=get_fundamental_cache(symbol,as_of)
    if fundamental_cached:
        fundamental=dict(fundamental_cached); fundamental["available"]=True
    else:
        try:
            fundamental=AKShareSource().fundamental_summary(symbol,as_of)
            save_fundamental_cache(symbol,as_of,fundamental,"eastmoney_financial_indicator_em")
            fundamental={**fundamental,"available":True}
        except Exception as exc:
            fundamental={"available":False,"message":"基本面暂不可用","technical":f"{type(exc).__name__}: {exc}"}
    item=dict(row); item["limit_dates"]=json.loads(item.pop("limit_dates_json"))
    return {"stock":item,"profile":profile,"fundamental":fundamental,
            "review":reviews_for_date(as_of).get(symbol,{"rating":"UNSET","reasons":[],"note":""})}


@app.get("/healthz")
def healthz():
    dates=published_dates(); latest=published_run_for_date(dates[-1]) if dates else None
    return {"ok": bool(latest and latest["status"] in ("SUCCESS","WARNING")),
            "latest_trade_date": latest["trade_date"] if latest else None,
            "status": latest["status"] if latest else "NOT_RUN"}
