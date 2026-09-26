"""Isolated web refresh worker.

Running scans in a child process keeps AKShare's scoped requests patch and slow
network calls out of the FastAPI process, so browsing and detail APIs remain
responsive while a date is rebuilt.
"""
from __future__ import annotations

import argparse
import os

from .db import (active_run_for_date, finish_background_refresh,
                 set_background_refresh_pid)
from .sync import sync_market_day


def run(job_id: int, trade_date: str) -> None:
    set_background_refresh_pid(job_id, os.getpid())
    try:
        sync_market_day(trade_date, run_scan=True, force=True)
        latest=active_run_for_date(trade_date)
        if not latest or latest["status"] not in ("SUCCESS","WARNING"):
            raise RuntimeError("扫描结束但没有可发布的新快照")
        finish_background_refresh(job_id, latest["status"], latest["message"] or "复盘已生成完成")
    except Exception as exc:
        finish_background_refresh(job_id,"FAILED",f"{type(exc).__name__}: {exc}")


def main() -> None:
    parser=argparse.ArgumentParser()
    parser.add_argument("job_id",type=int)
    parser.add_argument("trade_date")
    args=parser.parse_args()
    run(args.job_id,args.trade_date)


if __name__ == "__main__":
    main()
