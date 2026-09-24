from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Mapping, Sequence

import pandas as pd

from .config import settings

SCHEMA = """
PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS limit_up_pool (
 trade_date TEXT NOT NULL, symbol TEXT NOT NULL, name TEXT NOT NULL, close REAL NOT NULL,
 total_market_cap REAL NOT NULL, industry TEXT, limit_up_count INTEGER, pct_chg REAL,
 first_limit_time TEXT, last_limit_time TEXT, source TEXT NOT NULL, fetched_at TEXT NOT NULL,
 PRIMARY KEY(trade_date,symbol));
CREATE INDEX IF NOT EXISTS idx_limit_pool_symbol_date ON limit_up_pool(symbol,trade_date);
CREATE TABLE IF NOT EXISTS trading_calendar (
 trade_date TEXT PRIMARY KEY, is_open INTEGER NOT NULL, source TEXT NOT NULL, fetched_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS history_bars (
 symbol TEXT NOT NULL, trade_date TEXT NOT NULL, open REAL, high REAL, low REAL, close REAL,
 preclose REAL, volume REAL, amount REAL, pct_chg REAL, trade_status TEXT, is_st TEXT,
 adjust_status TEXT NOT NULL, source TEXT NOT NULL, fetched_at TEXT NOT NULL,
 PRIMARY KEY(symbol,trade_date,adjust_status));
CREATE INDEX IF NOT EXISTS idx_history_symbol_date ON history_bars(symbol,trade_date);
CREATE TABLE IF NOT EXISTS pipeline_runs (
 trade_date TEXT PRIMARY KEY, status TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT,
 message TEXT, fetched_at TEXT, akshare_status TEXT, baostock_status TEXT,
 current_time TEXT, history_status TEXT,
 pool_rows INTEGER DEFAULT 0, cap_rows INTEGER DEFAULT 0, repeated_rows INTEGER DEFAULT 0,
 nonconsecutive_rows INTEGER DEFAULT 0, low_rows INTEGER DEFAULT 0, pullback_rows INTEGER DEFAULT 0,
 pattern_rows INTEGER DEFAULT 0, candidate_rows INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS missing_data (
 trade_date TEXT NOT NULL, dataset TEXT NOT NULL, status TEXT NOT NULL, message TEXT,
 checked_at TEXT NOT NULL, PRIMARY KEY(trade_date,dataset));
CREATE TABLE IF NOT EXISTS scan_results (
 trade_date TEXT NOT NULL, symbol TEXT NOT NULL, name TEXT NOT NULL, industry TEXT, close REAL NOT NULL,
 total_market_cap_yi REAL NOT NULL, limit_up_count INTEGER NOT NULL, limit_dates_json TEXT NOT NULL,
 low_position_pct REAL NOT NULL, distance_from_low_pct REAL NOT NULL, pullback_pct REAL,
 recent_return_pct REAL, verification_status TEXT NOT NULL, verification_message TEXT,
 source TEXT NOT NULL, fetched_at TEXT NOT NULL, created_at TEXT NOT NULL,
 PRIMARY KEY(trade_date,symbol));
CREATE INDEX IF NOT EXISTS idx_scan_results_date ON scan_results(trade_date);
CREATE TABLE IF NOT EXISTS scan_stage_results (
 trade_date TEXT NOT NULL, symbol TEXT NOT NULL, name TEXT NOT NULL, industry TEXT,
 close REAL NOT NULL, total_market_cap_yi REAL NOT NULL,
 limit_up_count INTEGER NOT NULL DEFAULT 0, limit_dates_json TEXT NOT NULL DEFAULT '[]',
 has_consecutive_limit_up INTEGER,
 low_position_pct REAL, distance_from_low_pct REAL, pullback_pct REAL, recent_return_pct REAL,
 passes_market_cap INTEGER NOT NULL DEFAULT 1, passes_repeat_limit INTEGER NOT NULL DEFAULT 0,
 passes_nonconsecutive INTEGER NOT NULL DEFAULT 0, passes_low INTEGER NOT NULL DEFAULT 0,
 passes_pullback INTEGER NOT NULL DEFAULT 0, passes_recent_return INTEGER NOT NULL DEFAULT 0,
 passes_final INTEGER NOT NULL DEFAULT 0, verification_status TEXT NOT NULL,
 verification_message TEXT, reject_reasons_json TEXT NOT NULL DEFAULT '[]',
 source TEXT NOT NULL, fetched_at TEXT NOT NULL,
 PRIMARY KEY(trade_date,symbol));
CREATE INDEX IF NOT EXISTS idx_stage_results_date ON scan_stage_results(trade_date);
CREATE TABLE IF NOT EXISTS market_audits (
 trade_date TEXT PRIMARY KEY, status TEXT NOT NULL, market_rows INTEGER NOT NULL,
 theoretical_rows INTEGER NOT NULL, pool_rows INTEGER NOT NULL, common_rows INTEGER NOT NULL,
 only_theoretical_json TEXT NOT NULL, only_pool_json TEXT NOT NULL,
 main_rows INTEGER NOT NULL, chinext_rows INTEGER NOT NULL, star_rows INTEGER NOT NULL,
 bse_rows INTEGER NOT NULL, st_excluded_rows INTEGER NOT NULL, unknown_rows INTEGER NOT NULL,
 message TEXT, fetched_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS daily_limit_universe (
 trade_date TEXT NOT NULL, symbol TEXT NOT NULL, name TEXT NOT NULL, raw_close REAL NOT NULL,
 previous_close REAL, total_market_cap REAL, industry TEXT, market TEXT NOT NULL,
 detection_status TEXT NOT NULL, source TEXT NOT NULL, fetched_at TEXT NOT NULL,
 PRIMARY KEY(trade_date,symbol));
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(settings.db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db() -> None:
    with connect() as conn:
        # Preserve a V0.1 Tushare database instead of deleting it.  Only the two
        # incompatible result/status tables are archived before creating V0.2.
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "pipeline_runs" in tables:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(pipeline_runs)")}
            if "pool_rows" not in cols:
                conn.execute("ALTER TABLE pipeline_runs RENAME TO pipeline_runs_v01_archive")
        if "scan_results" in tables:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(scan_results)")}
            if "verification_status" not in cols:
                conn.execute("ALTER TABLE scan_results RENAME TO scan_results_v01_archive")
        conn.executescript(SCHEMA)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(pipeline_runs)")}
        for name, declaration in {
            "current_time":"TEXT", "history_status":"TEXT",
            "low_rows":"INTEGER DEFAULT 0", "pullback_rows":"INTEGER DEFAULT 0",
        }.items():
            if name not in cols:
                conn.execute(f"ALTER TABLE pipeline_runs ADD COLUMN {name} {declaration}")


@contextmanager
def transaction():
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_dataframe(conn, table: str, df: pd.DataFrame, columns: Sequence[str], conflict_columns: Sequence[str]) -> int:
    if df.empty:
        return 0
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"{table}: missing columns: {missing}")
    updates = [c for c in columns if c not in conflict_columns]
    sql = (f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)}) "
           f"ON CONFLICT({','.join(conflict_columns)}) DO UPDATE SET " + ",".join(f"{c}=excluded.{c}" for c in updates))
    rows = [tuple(None if pd.isna(row[c]) else row[c].item() if hasattr(row[c], "item") else row[c] for c in columns)
            for _, row in df.iterrows()]
    conn.executemany(sql, rows)
    return len(rows)


def mark_pipeline_start(trade_date: str) -> None:
    init_db()
    with connect() as conn:
        conn.execute("""INSERT INTO pipeline_runs(trade_date,status,started_at,message,akshare_status,baostock_status)
          VALUES(?,?,?,?,?,?) ON CONFLICT(trade_date) DO UPDATE SET status=excluded.status,
          started_at=excluded.started_at,finished_at=NULL,message=excluded.message,
          akshare_status=excluded.akshare_status,baostock_status=excluded.baostock_status""",
          (trade_date,"RUNNING",utcnow(),"同步开始","PENDING","PENDING"))
        conn.commit()


def mark_pipeline_finish(trade_date: str, status: str, message: str, counts: Mapping[str, int],
                         akshare_status: str, history_status: str, current_time: str) -> None:
    with connect() as conn:
        conn.execute("""UPDATE pipeline_runs SET status=?,finished_at=?,message=?,fetched_at=?,
          akshare_status=?,baostock_status='OPTIONAL',history_status=?,current_time=?,pool_rows=?,cap_rows=?,repeated_rows=?,
          nonconsecutive_rows=?,low_rows=?,pullback_rows=?,pattern_rows=?,candidate_rows=? WHERE trade_date=?""",
          (status,utcnow(),message,utcnow(),akshare_status,history_status,current_time,
           counts.get("pool_rows",0),counts.get("cap_rows",0),counts.get("repeated_rows",0),
           counts.get("nonconsecutive_rows",0),counts.get("low_rows",0),counts.get("pullback_rows",0),
           counts.get("pattern_rows",0),counts.get("candidate_rows",0),trade_date))
        conn.commit()


def pipeline_success(trade_date: str) -> bool:
    init_db()
    with connect() as conn:
        row = conn.execute("SELECT status FROM pipeline_runs WHERE trade_date=?",(trade_date,)).fetchone()
        # WARNING may be caused by a transient/missing upstream day, so a later
        # run must retry it. Only a fully successful day is a no-op.
        return bool(row and row["status"] == "SUCCESS")


def latest_run():
    init_db()
    with connect() as conn:
        return conn.execute("SELECT * FROM pipeline_runs ORDER BY trade_date DESC LIMIT 1").fetchone()


def latest_successful_run():
    init_db()
    with connect() as conn:
        return conn.execute("SELECT * FROM pipeline_runs WHERE status IN ('SUCCESS','WARNING') ORDER BY trade_date DESC LIMIT 1").fetchone()


def run_for_date(trade_date: str):
    init_db()
    with connect() as conn:
        return conn.execute("SELECT * FROM pipeline_runs WHERE trade_date=?", (trade_date,)).fetchone()


def scanned_dates() -> list[str]:
    init_db()
    with connect() as conn:
        return [row[0] for row in conn.execute(
            "SELECT trade_date FROM pipeline_runs WHERE status IN ('SUCCESS','WARNING','FAILED') ORDER BY trade_date"
        )]


def save_scan_results(trade_date: str, rows: list[Mapping]) -> int:
    now = utcnow()
    with transaction() as conn:
        conn.execute("DELETE FROM scan_results WHERE trade_date=?", (trade_date,))
        payload = [(trade_date,r["symbol"],r["name"],r.get("industry"),r["close"],r["total_market_cap_yi"],
                    r["limit_up_count"],json.dumps(r["limit_dates"],ensure_ascii=False),r["low_position_pct"],
                    r["distance_from_low_pct"],r.get("pullback_pct"),r.get("recent_return_pct"),
                    r["verification_status"],r.get("verification_message"),"eastmoney_pool+tencent+sina",now,now) for r in rows]
        conn.executemany("""INSERT INTO scan_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
        return len(payload)


def save_stage_results(trade_date: str, rows: list[Mapping]) -> int:
    """Atomically replace one day's complete stage audit and official candidates."""
    now = utcnow()
    columns = [
        "trade_date","symbol","name","industry","close","total_market_cap_yi",
        "limit_up_count","limit_dates_json","has_consecutive_limit_up",
        "low_position_pct","distance_from_low_pct","pullback_pct","recent_return_pct",
        "passes_market_cap","passes_repeat_limit","passes_nonconsecutive","passes_low",
        "passes_pullback","passes_recent_return","passes_final","verification_status",
        "verification_message","reject_reasons_json","source","fetched_at",
    ]
    values = []
    for row in rows:
        values.append((
            trade_date,row["symbol"],row["name"],row.get("industry"),row["close"],
            row["total_market_cap_yi"],row.get("limit_up_count",0),
            json.dumps(row.get("limit_dates",[]),ensure_ascii=False),
            _sqlite_bool(row.get("has_consecutive_limit_up")),row.get("low_position_pct"),
            row.get("distance_from_low_pct"),row.get("pullback_pct"),row.get("recent_return_pct"),
            _sqlite_bool(row.get("passes_market_cap",True)),_sqlite_bool(row.get("passes_repeat_limit",False)),
            _sqlite_bool(row.get("passes_nonconsecutive",False)),_sqlite_bool(row.get("passes_low",False)),
            _sqlite_bool(row.get("passes_pullback",False)),_sqlite_bool(row.get("passes_recent_return",False)),
            _sqlite_bool(row.get("passes_final",False)),row.get("verification_status","NOT_CHECKED"),
            row.get("verification_message"),json.dumps(row.get("reject_reasons",[]),ensure_ascii=False),
            "eastmoney+tencent+sina",now,
        ))
    with transaction() as conn:
        conn.execute("DELETE FROM scan_stage_results WHERE trade_date=?",(trade_date,))
        conn.execute("DELETE FROM scan_results WHERE trade_date=?",(trade_date,))
        if values:
            placeholders=",".join("?" for _ in columns)
            conn.executemany(f"INSERT INTO scan_stage_results ({','.join(columns)}) VALUES ({placeholders})",values)
        final_rows=[row for row in rows if row.get("passes_final")]
        payload=[(trade_date,r["symbol"],r["name"],r.get("industry"),r["close"],r["total_market_cap_yi"],
                  r["limit_up_count"],json.dumps(r["limit_dates"],ensure_ascii=False),r["low_position_pct"],
                  r["distance_from_low_pct"],r.get("pullback_pct"),r.get("recent_return_pct"),
                  r["verification_status"],r.get("verification_message"),"eastmoney+tencent+sina",now,now)
                 for r in final_rows]
        if payload:
            conn.executemany("INSERT INTO scan_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",payload)
    return len(final_rows)


def _sqlite_bool(value):
    if value is None:
        return None
    return 1 if value else 0
