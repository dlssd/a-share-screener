from __future__ import annotations

import json
import secrets
import threading
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates

from .config import settings
from .db import connect, init_db, latest_run, run_for_date, scanned_dates
from .sync import resolve_latest_completed_trade_date, sync_market_day

app = FastAPI(title="A股低位多涨停筛选器", version="0.5.0")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
security = HTTPBasic(auto_error=False)
refresh_lock = threading.Lock()


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
    with connect() as conn:
        rows=conn.execute("SELECT * FROM scan_stage_results WHERE trade_date=?",(trade_date,)).fetchall()
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
    latest=latest_run(); dates=scanned_dates()
    chosen=(date or (latest["trade_date"] if latest else None))
    if chosen not in dates and dates: chosen=dates[-1]
    selected=run_for_date(chosen) if chosen else None
    valid=bool(selected and selected["status"] in ("SUCCESS","WARNING"))
    stages=_stage_rows_for_date(chosen) if chosen and valid else []
    official=[r for r in stages if r["passes_final"]]
    watch=sorted((r for r in stages if r["passes_nonconsecutive"] and not r["passes_final"]),key=_watch_score)
    repeated=sorted((r for r in stages if r["passes_repeat_limit"]),key=lambda r:(r["has_consecutive_limit_up"],-r["limit_up_count"],r["symbol"]))
    index_pos=dates.index(chosen) if chosen in dates else -1
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "official":official,"watch":watch,"repeated":repeated,"cap_rows":stages,
            "chosen_date": chosen,
            "selected":dict(selected) if selected else None,
            "previous_date":dates[index_pos-1] if index_pos>0 else None,
            "next_date":dates[index_pos+1] if 0<=index_pos<len(dates)-1 else None,
            "settings": settings,
        },
    )


@app.get("/api/results", dependencies=[Depends(auth)])
def api_results(date: Optional[str] = None):
    latest = latest_run()
    chosen = date or (latest["trade_date"] if latest else None)
    failed = bool(latest and chosen == latest["trade_date"] and latest["status"] == "FAILED")
    return {"trade_date": chosen, "status": latest["status"] if latest else "NOT_RUN",
            "rows": _rows_for_date(chosen) if chosen and not failed else []}


@app.post("/api/refresh", dependencies=[Depends(auth)])
def refresh_latest():
    if not refresh_lock.acquire(blocking=False):
        return JSONResponse({"ok":False,"message":"数据正在抓取，请稍候。"},status_code=409)
    try:
        trade_date=resolve_latest_completed_trade_date()
        sync_market_day(trade_date,run_scan=True,force=True)
        run=run_for_date(trade_date)
        if not run or run["status"]=="FAILED":
            return JSONResponse({"ok":False,"message":"关键数据缺失，本次未生成可信复盘结果。"},status_code=503)
        return {"ok":True,"trade_date":trade_date,"status":run["status"]}
    except Exception as exc:
        return JSONResponse({"ok":False,"message":"重新抓取失败，请稍后再试或检查行情网络直连设置。",
                             "detail":f"{type(exc).__name__}: {exc}"},status_code=503)
    finally:
        refresh_lock.release()


@app.get("/healthz")
def healthz():
    latest = latest_run()
    return {"ok": bool(latest and latest["status"] in ("SUCCESS","WARNING")),
            "latest_trade_date": latest["trade_date"] if latest else None,
            "status": latest["status"] if latest else "NOT_RUN"}
