from __future__ import annotations

from datetime import datetime, timedelta

from .datasource import AKShareSource
from .db import connect, init_db, transaction, utcnow


class CalendarUnavailable(RuntimeError):
    pass


def _clean(value: str) -> str:
    return str(value).replace("-", "")


def ensure_calendar(start_date: str, end_date: str, source: AKShareSource | None = None) -> None:
    """Cache every natural date in the requested range, including closed days."""
    start_date, end_date = _clean(start_date), _clean(end_date)
    init_db()
    start=datetime.strptime(start_date,"%Y%m%d").date(); end=datetime.strptime(end_date,"%Y%m%d").date()
    expected=(end-start).days+1
    with connect() as conn:
        cached=conn.execute("SELECT COUNT(*) FROM trading_calendar WHERE trade_date BETWEEN ? AND ?",(start_date,end_date)).fetchone()[0]
    if cached==expected:
        return
    source=source or AKShareSource()
    try:
        open_days=set(source.trading_days(start_date,end_date))
    except Exception as exc:
        # A complete cached subrange remains usable during a network outage.
        with connect() as conn:
            cached=conn.execute("SELECT COUNT(*) FROM trading_calendar WHERE trade_date BETWEEN ? AND ?",(start_date,end_date)).fetchone()[0]
        if cached==expected:
            return
        raise CalendarUnavailable(f"交易日历暂时无法更新: {type(exc).__name__}") from exc
    rows=[]; day=start; fetched=utcnow()
    while day<=end:
        text=day.strftime("%Y%m%d")
        rows.append((text,1 if text in open_days else 0,"akshare_sina_calendar",fetched))
        day+=timedelta(days=1)
    with transaction() as conn:
        conn.executemany("""INSERT INTO trading_calendar(trade_date,is_open,source,fetched_at) VALUES(?,?,?,?)
          ON CONFLICT(trade_date) DO UPDATE SET is_open=excluded.is_open,source=excluded.source,fetched_at=excluded.fetched_at""",rows)


def is_trade_date(trade_date: str, source: AKShareSource | None = None) -> bool:
    trade_date=_clean(trade_date)
    ensure_calendar(trade_date,trade_date,source)
    with connect() as conn:
        row=conn.execute("SELECT is_open FROM trading_calendar WHERE trade_date=?",(trade_date,)).fetchone()
    return bool(row and row["is_open"])


def previous_trade_date(trade_date: str, source: AKShareSource | None = None) -> str | None:
    trade_date=_clean(trade_date); target=datetime.strptime(trade_date,"%Y%m%d")
    start=(target-timedelta(days=60)).strftime("%Y%m%d")
    ensure_calendar(start,trade_date,source)
    with connect() as conn:
        row=conn.execute("SELECT trade_date FROM trading_calendar WHERE is_open=1 AND trade_date<? ORDER BY trade_date DESC LIMIT 1",(trade_date,)).fetchone()
    return row["trade_date"] if row else None


def next_trade_date(trade_date: str, source: AKShareSource | None = None) -> str | None:
    trade_date=_clean(trade_date); target=datetime.strptime(trade_date,"%Y%m%d")
    end=(target+timedelta(days=60)).strftime("%Y%m%d")
    ensure_calendar(trade_date,end,source)
    with connect() as conn:
        row=conn.execute("SELECT trade_date FROM trading_calendar WHERE is_open=1 AND trade_date>? ORDER BY trade_date LIMIT 1",(trade_date,)).fetchone()
    return row["trade_date"] if row else None


def trade_dates_between(start_date: str, end_date: str, source: AKShareSource | None = None) -> list[str]:
    start_date,end_date=_clean(start_date),_clean(end_date)
    ensure_calendar(start_date,end_date,source)
    with connect() as conn:
        return [r[0] for r in conn.execute("SELECT trade_date FROM trading_calendar WHERE is_open=1 AND trade_date BETWEEN ? AND ? ORDER BY trade_date",(start_date,end_date))]


def month_days(month: str, source: AKShareSource | None = None) -> list[dict]:
    first=datetime.strptime(month,"%Y%m").date().replace(day=1)
    next_month=(first.replace(day=28)+timedelta(days=4)).replace(day=1)
    last=next_month-timedelta(days=1)
    ensure_calendar(first.strftime("%Y%m%d"),last.strftime("%Y%m%d"),source)
    with connect() as conn:
        rows=conn.execute("SELECT trade_date,is_open FROM trading_calendar WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date",
                          (first.strftime("%Y%m%d"),last.strftime("%Y%m%d"))).fetchall()
    return [{"date":r["trade_date"],"is_trade":bool(r["is_open"])} for r in rows]
