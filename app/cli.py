from __future__ import annotations

import argparse
import logging

from .db import init_db, latest_run
from .sync import backfill, sync_latest_if_needed, sync_market_day
from .audit import build_market_audit, print_audit


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="A股低位多涨停筛选器")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init-db")

    p_backfill = sub.add_parser("backfill")
    p_backfill.add_argument("--trading-days", type=int, default=320)
    p_backfill.add_argument("--force", action="store_true")

    p_sync = sub.add_parser("sync")
    p_sync.add_argument("--date", help="YYYYMMDD; omitted = latest due trade date")
    p_sync.add_argument("--force", action="store_true")

    sub.add_parser("sync-latest")
    sub.add_parser("run")

    sub.add_parser("status")
    p_audit = sub.add_parser("audit")
    p_audit.add_argument("--date", required=True, help="YYYY-MM-DD or YYYYMMDD")
    p_audit.add_argument("--force", action="store_true")

    args = parser.parse_args()
    if args.cmd == "init-db":
        init_db()
        print("database initialized")
    elif args.cmd == "backfill":
        dates = backfill(args.trading_days, force=args.force)
        print(f"backfilled {len(dates)} trading days; latest={dates[-1] if dates else None}")
    elif args.cmd == "sync":
        if args.date:
            n = sync_market_day(args.date, run_scan=True, force=args.force)
            print(f"{args.date}: {n} candidates")
        else:
            d = sync_latest_if_needed(force=args.force)
            print(f"latest synced: {d}")
    elif args.cmd in ("sync-latest", "run"):
        d = sync_latest_if_needed()
        print(f"同步、校验、扫描完成: {d}")
    elif args.cmd == "status":
        init_db()
        latest = latest_run()
        print(dict(latest) if latest else {"status": "no successful run"})
    elif args.cmd == "audit":
        trade_date=args.date.replace("-","")
        _,report=build_market_audit(trade_date,force=args.force)
        print_audit(report)


if __name__ == "__main__":
    main()
