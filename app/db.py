from __future__ import annotations

import json
import os
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
CREATE TABLE IF NOT EXISTS scan_runs (
 run_id INTEGER PRIMARY KEY AUTOINCREMENT, trade_date TEXT NOT NULL, status TEXT NOT NULL,
 started_at TEXT NOT NULL, finished_at TEXT, message TEXT, fetched_at TEXT,
 akshare_status TEXT, history_status TEXT, current_time TEXT,
 pool_rows INTEGER DEFAULT 0, cap_rows INTEGER DEFAULT 0, repeated_rows INTEGER DEFAULT 0,
 nonconsecutive_rows INTEGER DEFAULT 0, low_rows INTEGER DEFAULT 0, pullback_rows INTEGER DEFAULT 0,
 pattern_rows INTEGER DEFAULT 0, candidate_rows INTEGER DEFAULT 0, warning_type TEXT,
 owner_pid INTEGER);
CREATE INDEX IF NOT EXISTS idx_scan_runs_date ON scan_runs(trade_date,run_id);
CREATE TABLE IF NOT EXISTS published_snapshots (
 trade_date TEXT PRIMARY KEY, run_id INTEGER NOT NULL, published_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS scan_stage_results_v06 (
 run_id INTEGER NOT NULL, trade_date TEXT NOT NULL, symbol TEXT NOT NULL, name TEXT NOT NULL, industry TEXT,
 close REAL NOT NULL, total_market_cap_yi REAL NOT NULL, limit_up_count INTEGER NOT NULL DEFAULT 0,
 limit_dates_json TEXT NOT NULL DEFAULT '[]', has_consecutive_limit_up INTEGER,
 low_position_pct REAL, distance_from_low_pct REAL, pullback_pct REAL, recent_return_pct REAL,
 passes_market_cap INTEGER NOT NULL DEFAULT 1, passes_repeat_limit INTEGER NOT NULL DEFAULT 0,
 passes_nonconsecutive INTEGER NOT NULL DEFAULT 0, passes_low INTEGER NOT NULL DEFAULT 0,
 passes_pullback INTEGER NOT NULL DEFAULT 0, passes_recent_return INTEGER NOT NULL DEFAULT 0,
 passes_final INTEGER NOT NULL DEFAULT 0, verification_status TEXT NOT NULL,
 verification_message TEXT, reject_reasons_json TEXT NOT NULL DEFAULT '[]',
 source TEXT NOT NULL, fetched_at TEXT NOT NULL, PRIMARY KEY(run_id,symbol));
CREATE INDEX IF NOT EXISTS idx_stage_v06_date ON scan_stage_results_v06(trade_date,run_id);
CREATE TABLE IF NOT EXISTS stock_industry_cache (
 symbol TEXT PRIMARY KEY, name TEXT, industry TEXT, source TEXT NOT NULL, fetched_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS stock_news_cache (
 symbol TEXT NOT NULL, query_date TEXT NOT NULL, news_json TEXT NOT NULL, source TEXT NOT NULL, fetched_at TEXT NOT NULL,
 PRIMARY KEY(symbol,query_date));
CREATE TABLE IF NOT EXISTS market_audits (
 trade_date TEXT PRIMARY KEY, status TEXT NOT NULL, market_rows INTEGER NOT NULL,
 theoretical_rows INTEGER NOT NULL, pool_rows INTEGER NOT NULL, common_rows INTEGER NOT NULL,
 only_theoretical_json TEXT NOT NULL, only_pool_json TEXT NOT NULL,
 main_rows INTEGER NOT NULL, chinext_rows INTEGER NOT NULL, star_rows INTEGER NOT NULL,
 bse_rows INTEGER NOT NULL, st_excluded_rows INTEGER NOT NULL, unknown_rows INTEGER NOT NULL,
 message TEXT, fetched_at TEXT NOT NULL, warning_type TEXT);
CREATE TABLE IF NOT EXISTS daily_limit_universe (
 trade_date TEXT NOT NULL, symbol TEXT NOT NULL, name TEXT NOT NULL, raw_close REAL NOT NULL,
 previous_close REAL, total_market_cap REAL, industry TEXT, market TEXT NOT NULL,
 detection_status TEXT NOT NULL, source TEXT NOT NULL, fetched_at TEXT NOT NULL,
 PRIMARY KEY(trade_date,symbol));
CREATE TABLE IF NOT EXISTS market_limit_results (
 run_id INTEGER NOT NULL, trade_date TEXT NOT NULL, symbol TEXT NOT NULL, name TEXT NOT NULL,
 industry TEXT, market TEXT NOT NULL, close REAL, total_market_cap_yi REAL, pct_chg REAL,
 recent_limit_count INTEGER NOT NULL DEFAULT 0, consecutive_boards INTEGER NOT NULL DEFAULT 1,
 board_label TEXT, is_t_board INTEGER, is_one_word INTEGER, limit_dates_json TEXT NOT NULL DEFAULT '[]',
 verification_status TEXT, source TEXT NOT NULL, fetched_at TEXT NOT NULL,
 PRIMARY KEY(run_id,symbol));
CREATE INDEX IF NOT EXISTS idx_market_limit_date ON market_limit_results(trade_date,run_id);
CREATE TABLE IF NOT EXISTS market_environment (
 run_id INTEGER PRIMARY KEY, trade_date TEXT NOT NULL, symbol TEXT NOT NULL, name TEXT NOT NULL,
 close REAL, low_position_pct REAL, distance_from_high_pct REAL, distance_from_low_pct REAL,
 ma250_distance_pct REAL, recent_return_pct REAL, level_label TEXT, status TEXT NOT NULL,
 message TEXT, source TEXT NOT NULL, fetched_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS manual_reviews (
 trade_date TEXT NOT NULL, symbol TEXT NOT NULL, rating TEXT NOT NULL DEFAULT 'UNSET',
 reasons_json TEXT NOT NULL DEFAULT '[]', note TEXT NOT NULL DEFAULT '',
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, review_timing TEXT NOT NULL DEFAULT 'HINDSIGHT',
 PRIMARY KEY(trade_date,symbol));
CREATE TABLE IF NOT EXISTS stock_profile_cache (
 symbol TEXT NOT NULL, as_of_date TEXT NOT NULL, profile_json TEXT NOT NULL,
 source TEXT NOT NULL, fetched_at TEXT NOT NULL, PRIMARY KEY(symbol,as_of_date));
CREATE TABLE IF NOT EXISTS stock_fundamental_cache (
 symbol TEXT NOT NULL, as_of_date TEXT NOT NULL, report_date TEXT,
 revenue REAL, revenue_yoy REAL, net_profit REAL, net_profit_yoy REAL,
 profit_status TEXT, source TEXT NOT NULL, fetched_at TEXT NOT NULL,
 PRIMARY KEY(symbol,as_of_date));
CREATE TABLE IF NOT EXISTS background_refresh_jobs (
 job_id INTEGER PRIMARY KEY AUTOINCREMENT, trade_date TEXT NOT NULL,
 status TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT,
 message TEXT, worker_pid INTEGER);
CREATE INDEX IF NOT EXISTS idx_refresh_jobs_status ON background_refresh_jobs(status,job_id);
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
        for table in ("scan_runs","market_audits"):
            table_cols={r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
            if "warning_type" not in table_cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN warning_type TEXT")
        scan_cols={r[1] for r in conn.execute("PRAGMA table_info(scan_runs)")}
        if "owner_pid" not in scan_cols:
            conn.execute("ALTER TABLE scan_runs ADD COLUMN owner_pid INTEGER")
        review_cols={r[1] for r in conn.execute("PRAGMA table_info(manual_reviews)")}
        if "review_timing" not in review_cols:
            conn.execute("ALTER TABLE manual_reviews ADD COLUMN review_timing TEXT NOT NULL DEFAULT 'HINDSIGHT'")
        _migrate_v05_snapshots(conn)


def _migrate_v05_snapshots(conn: sqlite3.Connection) -> None:
    """Adopt V0.5's one-row-per-day data as the first immutable published run."""
    old_count=conn.execute("SELECT COUNT(*) FROM scan_runs").fetchone()[0]
    if old_count:
        return
    old_runs=conn.execute("SELECT * FROM pipeline_runs ORDER BY trade_date").fetchall()
    for old in old_runs:
        conn.execute("""INSERT INTO scan_runs(trade_date,status,started_at,finished_at,message,fetched_at,
          akshare_status,history_status,current_time,pool_rows,cap_rows,repeated_rows,nonconsecutive_rows,
          low_rows,pullback_rows,pattern_rows,candidate_rows)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (old["trade_date"],old["status"],old["started_at"],old["finished_at"],old["message"],old["fetched_at"],
           old["akshare_status"],old["history_status"],old["current_time"],old["pool_rows"],old["cap_rows"],
           old["repeated_rows"],old["nonconsecutive_rows"],old["low_rows"],old["pullback_rows"],old["pattern_rows"],old["candidate_rows"]))
        run_id=conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        old_stage=conn.execute("SELECT * FROM scan_stage_results WHERE trade_date=?",(old["trade_date"],)).fetchall()
        for row in old_stage:
            conn.execute("""INSERT INTO scan_stage_results_v06
              (run_id,trade_date,symbol,name,industry,close,total_market_cap_yi,limit_up_count,limit_dates_json,
               has_consecutive_limit_up,low_position_pct,distance_from_low_pct,pullback_pct,recent_return_pct,
               passes_market_cap,passes_repeat_limit,passes_nonconsecutive,passes_low,passes_pullback,
               passes_recent_return,passes_final,verification_status,verification_message,reject_reasons_json,source,fetched_at)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (run_id,row["trade_date"],row["symbol"],row["name"],row["industry"],row["close"],row["total_market_cap_yi"],
               row["limit_up_count"],row["limit_dates_json"],row["has_consecutive_limit_up"],row["low_position_pct"],
               row["distance_from_low_pct"],row["pullback_pct"],row["recent_return_pct"],row["passes_market_cap"],
               row["passes_repeat_limit"],row["passes_nonconsecutive"],row["passes_low"],row["passes_pullback"],
               row["passes_recent_return"],row["passes_final"],row["verification_status"],row["verification_message"],
               row["reject_reasons_json"],row["source"],row["fetched_at"]))
        if old["status"] in ("SUCCESS","WARNING"):
            conn.execute("INSERT OR IGNORE INTO published_snapshots VALUES(?,?,?)",(old["trade_date"],run_id,old["fetched_at"] or utcnow()))


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


def start_scan_run(trade_date: str, current_time: str) -> int:
    init_db()
    with transaction() as conn:
        job=conn.execute("SELECT trade_date,worker_pid FROM background_refresh_jobs WHERE status='RUNNING' ORDER BY job_id DESC LIMIT 1").fetchone()
        if job and int(job["worker_pid"] or -1)!=os.getpid():
            raise RuntimeError(f"{job['trade_date']} 后台扫描正在运行")
        active=conn.execute("SELECT trade_date FROM scan_runs WHERE status='RUNNING' ORDER BY run_id DESC LIMIT 1").fetchone()
        if active:
            raise RuntimeError(f"{active['trade_date']} 扫描正在运行")
        conn.execute("INSERT INTO scan_runs(trade_date,status,started_at,current_time,message,owner_pid) VALUES(?,?,?,?,?,?)",
                     (trade_date,"RUNNING",utcnow(),current_time,"正在重新抓取",os.getpid()))
        run_id=conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    mark_pipeline_start(trade_date)
    return int(run_id)


def published_run_for_date(trade_date: str):
    init_db()
    with connect() as conn:
        return conn.execute("""SELECT r.*,p.published_at FROM published_snapshots p JOIN scan_runs r ON r.run_id=p.run_id
                               WHERE p.trade_date=?""",(trade_date,)).fetchone()


def active_run_for_date(trade_date: str):
    init_db()
    with connect() as conn:
        return conn.execute("SELECT * FROM scan_runs WHERE trade_date=? ORDER BY run_id DESC LIMIT 1",(trade_date,)).fetchone()


def start_background_refresh(trade_date: str) -> int:
    """Atomically reserve the single web refresh slot across processes."""
    init_db()
    with connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        active=conn.execute("SELECT * FROM background_refresh_jobs WHERE status='RUNNING' ORDER BY job_id DESC LIMIT 1").fetchone()
        if active:
            conn.rollback()
            raise RuntimeError(f"{active['trade_date']} 正在后台生成，请完成后再生成其他日期。")
        conn.execute("INSERT INTO background_refresh_jobs(trade_date,status,started_at,message) VALUES(?,?,?,?)",
                     (trade_date,"RUNNING",utcnow(),"正在后台生成"))
        job_id=int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        conn.commit()
        return job_id


def set_background_refresh_pid(job_id: int, pid: int) -> None:
    with connect() as conn:
        conn.execute("UPDATE background_refresh_jobs SET worker_pid=? WHERE job_id=? AND status='RUNNING'",(pid,job_id))
        conn.commit()


def finish_background_refresh(job_id: int, status: str, message: str) -> None:
    if status not in {"SUCCESS","WARNING","FAILED","INTERRUPTED"}:
        raise ValueError(f"invalid background refresh status: {status}")
    with connect() as conn:
        conn.execute("UPDATE background_refresh_jobs SET status=?,finished_at=?,message=? WHERE job_id=?",
                     (status,utcnow(),message,job_id))
        conn.commit()


def latest_background_refresh():
    init_db()
    with connect() as conn:
        return conn.execute("SELECT * FROM background_refresh_jobs ORDER BY job_id DESC LIMIT 1").fetchone()


def running_background_refresh():
    init_db()
    with connect() as conn:
        return conn.execute("SELECT * FROM background_refresh_jobs WHERE status='RUNNING' ORDER BY job_id DESC LIMIT 1").fetchone()


def recover_interrupted_refreshes(pid_alive) -> list[int]:
    """Fail RUNNING jobs whose worker no longer exists, preserving published snapshots."""
    init_db(); interrupted=[]; now=utcnow()
    with transaction() as conn:
        jobs=conn.execute("SELECT * FROM background_refresh_jobs WHERE status='RUNNING'").fetchall()
        live_dates=set()
        for job in jobs:
            pid=job["worker_pid"]
            if pid and pid_alive(int(pid)):
                live_dates.add(job["trade_date"])
                continue
            message="上次后台生成因服务中断未完成，可重新生成。"
            conn.execute("UPDATE background_refresh_jobs SET status='INTERRUPTED',finished_at=?,message=? WHERE job_id=?",
                         (now,message,job["job_id"]))
            conn.execute("UPDATE scan_runs SET status='FAILED',finished_at=?,message=? WHERE trade_date=? AND status='RUNNING'",
                         (now,message,job["trade_date"]))
            interrupted.append(int(job["job_id"]))
        # Pre-V0.8.2 orphans have no job record and cannot be resumed safely.
        running_runs=conn.execute("SELECT run_id,trade_date,owner_pid FROM scan_runs WHERE status='RUNNING'").fetchall()
        for run in running_runs:
            owner=run["owner_pid"]
            if run["trade_date"] not in live_dates and (not owner or not pid_alive(int(owner))):
                conn.execute("UPDATE scan_runs SET status='FAILED',finished_at=?,message=? WHERE run_id=?",
                             (now,"上次后台生成因服务中断未完成，可重新生成。",run["run_id"]))
    return interrupted


def published_dates() -> list[str]:
    init_db()
    with connect() as conn:
        return [r[0] for r in conn.execute("SELECT trade_date FROM published_snapshots ORDER BY trade_date")]


def stage_rows_for_run(run_id: int) -> list[dict]:
    init_db()
    with connect() as conn:
        rows=conn.execute("SELECT * FROM scan_stage_results_v06 WHERE run_id=?",(run_id,)).fetchall()
    return [dict(row) for row in rows]


def finish_scan_run(run_id: int, trade_date: str, status: str, message: str, counts: Mapping[str,int],
                    akshare_status: str, history_status: str, current_time: str, rows: list[Mapping],
                    market_rows: list[Mapping] | None=None, market_environment: Mapping | None=None,
                    warning_type: str | None=None) -> int:
    now=utcnow()
    with transaction() as conn:
        if status in ("SUCCESS","WARNING"):
            for row in rows:
                conn.execute("""INSERT INTO scan_stage_results_v06
                  (run_id,trade_date,symbol,name,industry,close,total_market_cap_yi,limit_up_count,limit_dates_json,
                   has_consecutive_limit_up,low_position_pct,distance_from_low_pct,pullback_pct,recent_return_pct,
                   passes_market_cap,passes_repeat_limit,passes_nonconsecutive,passes_low,passes_pullback,
                   passes_recent_return,passes_final,verification_status,verification_message,reject_reasons_json,source,fetched_at)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (run_id,trade_date,row["symbol"],row["name"],row.get("industry"),row["close"],row["total_market_cap_yi"],
                   row.get("limit_up_count",0),json.dumps(row.get("limit_dates",[]),ensure_ascii=False),
                   _sqlite_bool(row.get("has_consecutive_limit_up")),row.get("low_position_pct"),row.get("distance_from_low_pct"),
                   row.get("pullback_pct"),row.get("recent_return_pct"),_sqlite_bool(row.get("passes_market_cap",True)),
                   _sqlite_bool(row.get("passes_repeat_limit")),_sqlite_bool(row.get("passes_nonconsecutive")),_sqlite_bool(row.get("passes_low")),
                   _sqlite_bool(row.get("passes_pullback")),_sqlite_bool(row.get("passes_recent_return")),_sqlite_bool(row.get("passes_final")),
                   row.get("verification_status","NOT_CHECKED"),row.get("verification_message"),json.dumps(row.get("reject_reasons",[]),ensure_ascii=False),
                   "eastmoney+tencent+sina",now))
            conn.execute("INSERT OR REPLACE INTO published_snapshots(trade_date,run_id,published_at) VALUES(?,?,?)",(trade_date,run_id,now))
            for row in market_rows or []:
                conn.execute("""INSERT INTO market_limit_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (run_id,trade_date,row["symbol"],row["name"],row.get("industry"),row.get("market","UNKNOWN"),
                   row.get("close"),row.get("total_market_cap_yi"),row.get("pct_chg"),row.get("recent_limit_count",0),
                   row.get("consecutive_boards",1),row.get("board_label"),_sqlite_bool(row.get("is_t_board")),
                   _sqlite_bool(row.get("is_one_word")),json.dumps(row.get("limit_dates",[]),ensure_ascii=False),
                   row.get("verification_status"),row.get("source","eastmoney+tencent"),now))
            if market_environment:
                e=market_environment
                conn.execute("""INSERT INTO market_environment VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (run_id,trade_date,e.get("symbol","000001"),e.get("name","上证指数"),e.get("close"),
                   e.get("low_position_pct"),e.get("distance_from_high_pct"),e.get("distance_from_low_pct"),
                   e.get("ma250_distance_pct"),e.get("recent_return_pct"),e.get("level_label"),e.get("status","WARNING"),
                   e.get("message"),e.get("source","tencent"),now))
        conn.execute("""UPDATE scan_runs SET status=?,finished_at=?,message=?,fetched_at=?,akshare_status=?,history_status=?,warning_type=?,
          pool_rows=?,cap_rows=?,repeated_rows=?,nonconsecutive_rows=?,low_rows=?,pullback_rows=?,pattern_rows=?,candidate_rows=? WHERE run_id=?""",
          (status,now,message,now,akshare_status,history_status,warning_type,counts.get("pool_rows",0),counts.get("cap_rows",0),counts.get("repeated_rows",0),
           counts.get("nonconsecutive_rows",0),counts.get("low_rows",0),counts.get("pullback_rows",0),counts.get("pattern_rows",0),counts.get("candidate_rows",0),run_id))
    mark_pipeline_finish(trade_date,status,message,counts,akshare_status,history_status,current_time)
    return int(counts.get("candidate_rows",0))


def fail_scan_run(run_id: int, trade_date: str, message: str, current_time: str) -> None:
    now=utcnow()
    with connect() as conn:
        conn.execute("UPDATE scan_runs SET status='FAILED',finished_at=?,message=?,fetched_at=? WHERE run_id=?",(now,message,now,run_id)); conn.commit()
    # Keep the old summary for compatibility, but this never touches the published snapshot.
    with connect() as conn:
        conn.execute("UPDATE pipeline_runs SET status='FAILED',finished_at=?,message=?,fetched_at=?,current_time=? WHERE trade_date=?",
                     (now,message,now,current_time,trade_date)); conn.commit()


def get_industry_cache(symbol: str):
    init_db()
    with connect() as conn:
        return conn.execute("SELECT * FROM stock_industry_cache WHERE symbol=?",(symbol,)).fetchone()


def save_industry_cache(symbol: str, name: str, industry: str | None, source: str) -> None:
    with connect() as conn:
        conn.execute("INSERT OR REPLACE INTO stock_industry_cache VALUES(?,?,?,?,?)",(symbol,name,industry,source,utcnow())); conn.commit()


def get_news_cache(symbol: str, query_date: str):
    init_db()
    with connect() as conn:
        return conn.execute("SELECT * FROM stock_news_cache WHERE symbol=? AND query_date=?",(symbol,query_date)).fetchone()


def save_news_cache(symbol: str, query_date: str, news: list[dict], source: str) -> None:
    with connect() as conn:
        conn.execute("INSERT OR REPLACE INTO stock_news_cache VALUES(?,?,?,?,?)",(symbol,query_date,json.dumps(news,ensure_ascii=False),source,utcnow())); conn.commit()


def market_rows_for_run(run_id: int) -> list[dict]:
    init_db()
    with connect() as conn:
        rows=conn.execute("SELECT * FROM market_limit_results WHERE run_id=? ORDER BY symbol",(run_id,)).fetchall()
    out=[]
    for row in rows:
        item=dict(row); item["limit_dates"]=json.loads(item.pop("limit_dates_json")); out.append(item)
    return out


def market_environment_for_run(run_id: int):
    init_db()
    with connect() as conn:
        row=conn.execute("SELECT * FROM market_environment WHERE run_id=?",(run_id,)).fetchone()
    return dict(row) if row else None


def reviews_for_date(trade_date: str) -> dict[str,dict]:
    init_db()
    with connect() as conn: rows=conn.execute("SELECT * FROM manual_reviews WHERE trade_date=?",(trade_date,)).fetchall()
    out={}
    for row in rows:
        item=dict(row); item["reasons"]=json.loads(item.pop("reasons_json")); out[item["symbol"]]=item
    return out


def review_timing(trade_date: str, created_at: str | None=None) -> str:
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    created=datetime.fromisoformat(created_at or utcnow()).astimezone(ZoneInfo(settings.timezone)).date()
    target=datetime.strptime(trade_date,"%Y%m%d").date()
    return "LIVE" if target<=created<=target+timedelta(days=1) else "HINDSIGHT"


def save_manual_review(trade_date: str, symbol: str, rating: str, reasons: list[str], note: str) -> dict:
    init_db(); now=utcnow(); timing=review_timing(trade_date,now)
    with connect() as conn:
        conn.execute("""INSERT INTO manual_reviews(trade_date,symbol,rating,reasons_json,note,created_at,updated_at,review_timing)
          VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(trade_date,symbol) DO UPDATE SET rating=excluded.rating,
          reasons_json=excluded.reasons_json,note=excluded.note,updated_at=excluded.updated_at""",
          (trade_date,symbol,rating,json.dumps(reasons,ensure_ascii=False),note,now,now,timing)); conn.commit()
    return reviews_for_date(trade_date)[symbol]


def get_profile_cache(symbol: str, as_of_date: str):
    init_db()
    with connect() as conn: return conn.execute("SELECT * FROM stock_profile_cache WHERE symbol=? AND as_of_date=?",(symbol,as_of_date)).fetchone()


def save_profile_cache(symbol: str, as_of_date: str, profile: Mapping, source: str) -> None:
    with connect() as conn:
        conn.execute("INSERT OR REPLACE INTO stock_profile_cache VALUES(?,?,?,?,?)",
                     (symbol,as_of_date,json.dumps(profile,ensure_ascii=False),source,utcnow())); conn.commit()


def get_fundamental_cache(symbol: str, as_of_date: str):
    init_db()
    with connect() as conn: return conn.execute("SELECT * FROM stock_fundamental_cache WHERE symbol=? AND as_of_date=?",(symbol,as_of_date)).fetchone()


def save_fundamental_cache(symbol: str, as_of_date: str, item: Mapping, source: str) -> None:
    with connect() as conn:
        conn.execute("INSERT OR REPLACE INTO stock_fundamental_cache VALUES(?,?,?,?,?,?,?,?,?,?)",
          (symbol,as_of_date,item.get("report_date"),item.get("revenue"),item.get("revenue_yoy"),
           item.get("net_profit"),item.get("net_profit_yoy"),item.get("profit_status"),source,utcnow())); conn.commit()


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
