from __future__ import annotations

import json
import secrets
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates

from .config import settings
from .db import connect, init_db, latest_run, latest_successful_run

app = FastAPI(title="A股低位多涨停筛选器", version="0.4.0")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
security = HTTPBasic(auto_error=False)


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


@app.get("/", response_class=HTMLResponse, dependencies=[Depends(auth)])
def index(request: Request, date: Optional[str] = None):
    latest = latest_run()
    chosen = date or (latest["trade_date"] if latest else None)
    # A failed rerun may deliberately preserve old verified rows in SQLite, but
    # they must never be presented as the current run's official candidates.
    rows = _rows_for_date(chosen) if chosen and (not latest or chosen != latest["trade_date"] or latest["status"] != "FAILED") else []
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "rows": rows,
            "chosen_date": chosen,
            "latest": dict(latest) if latest else None,
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


@app.get("/healthz")
def healthz():
    latest = latest_run()
    return {"ok": bool(latest and latest["status"] in ("SUCCESS","WARNING")),
            "latest_trade_date": latest["trade_date"] if latest else None,
            "status": latest["status"] if latest else "NOT_RUN"}
