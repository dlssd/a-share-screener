from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from .audit import build_market_audit
from .config import settings
from .datasource import AKShareSource
from .db import connect, init_db, mark_pipeline_finish, mark_pipeline_start, pipeline_success, save_scan_results
from .limit_rules import INSUFFICIENT_HISTORY, UNKNOWN_LIMIT_RULE, detect_limit_up_days
from .scanner import ScanParams, evaluate_stages, has_consecutive_limit_ups, verify_independent_source


class DataValidationError(RuntimeError): pass


def shanghai_now() -> datetime: return datetime.now(ZoneInfo(settings.timezone))


def resolve_latest_completed_trade_date(source: AKShareSource | None=None, now: datetime | None=None) -> str:
    source=source or AKShareSource(); now=now or shanghai_now()
    if now.tzinfo is None: now=now.replace(tzinfo=ZoneInfo(settings.timezone))
    today=now.strftime("%Y%m%d")
    days=source.trading_days((now.date()-timedelta(days=45)).strftime("%Y%m%d"),today)
    if today in days and now.hour<settings.publish_after_hour: days=[d for d in days if d<today]
    if not days: raise DataValidationError("没有可用的已完成交易日")
    return days[-1]


def validate_limit_pool(frame: pd.DataFrame, trade_date: str) -> str | None:
    if frame.empty: raise DataValidationError(f"{trade_date} 涨停池为空")
    required=["trade_date","symbol","name","close","total_market_cap"]
    if any(c not in frame for c in required): raise DataValidationError("涨停池缺少核心字段")
    if set(frame["trade_date"].astype(str))!={trade_date}: raise DataValidationError("涨停池日期不匹配")
    if frame["symbol"].duplicated().any(): raise DataValidationError("涨停池股票代码重复")
    return None


def missing_required_days(required_days: list[str], available_days: set[str]) -> list[str]:
    return [d for d in required_days if d not in available_days]


def _history(source: AKShareSource, symbol: str, target: str, provider: str, adjusted: bool) -> pd.DataFrame:
    start=(datetime.strptime(target,"%Y%m%d")-timedelta(days=520)).strftime("%Y%m%d")
    method=source.tencent_history if provider=="tencent" else source.sina_history
    frame=method(symbol,start,target,adjusted=adjusted).sort_values("trade_date")
    if frame.empty or str(frame.iloc[-1]["trade_date"])!=target:
        raise DataValidationError(f"{provider} {symbol} 缺少目标日")
    return frame


def sync_market_day(trade_date: str, *, run_scan: bool=True, force: bool=False) -> int:
    init_db(); source=AKShareSource(); now=shanghai_now(); latest=resolve_latest_completed_trade_date(source,now)
    if trade_date>latest: raise DataValidationError(f"{trade_date} 尚非完整盘后交易日；最近完整日为 {latest}")
    if pipeline_success(trade_date) and not force:
        with connect() as conn: return int(conn.execute("SELECT candidate_rows FROM pipeline_runs WHERE trade_date=?",(trade_date,)).fetchone()[0])
    mark_pipeline_start(trade_date); current_time=now.strftime("%Y-%m-%d %H:%M Asia/Shanghai")
    counts={k:0 for k in ("pool_rows","cap_rows","repeated_rows","nonconsecutive_rows","low_rows","pullback_rows","pattern_rows","candidate_rows")}
    warnings=[]; history_status="PENDING"
    try:
        universe,audit=build_market_audit(trade_date,force=force)
        counts["pool_rows"]=len(universe)
        if audit["status"]=="WARNING": warnings.append("全市场审计为 WARNING: "+audit.get("message", ""))
        cap=[r for r in universe if r.get("total_market_cap") is not None and float(r["total_market_cap"])>=settings.market_cap_min_cny]
        counts["cap_rows"]=len(cap); recent_calendar=source.trading_days(
            (datetime.strptime(trade_date,"%Y%m%d")-timedelta(days=60)).strftime("%Y%m%d"),trade_date)[-settings.recent_trading_days:]
        prepared=[]
        for row in cap:
            symbol=row["symbol"]
            tx_raw=_history(source,symbol,trade_date,"tencent",False)
            sina_raw=_history(source,symbol,trade_date,"sina",False)
            if len(tx_raw)<settings.low_window_days or len(sina_raw)<settings.low_window_days:
                warnings.append(f"{symbol} {INSUFFICIENT_HISTORY}"); continue
            tx_detection=detect_limit_up_days(tx_raw,symbol,row["name"])
            sina_detection=detect_limit_up_days(sina_raw,symbol,row["name"])
            if tx_detection.status==UNKNOWN_LIMIT_RULE or sina_detection.status==UNKNOWN_LIMIT_RULE:
                warnings.append(f"{symbol} {UNKNOWN_LIMIT_RULE}"); continue
            tx_dates=[d for d in tx_detection.dates if d in recent_calendar]
            sina_dates=[d for d in sina_detection.dates if d in recent_calendar]
            verification="VERIFIED" if tx_dates==sina_dates else "DATA_MISMATCH"
            if verification=="DATA_MISMATCH": warnings.append(f"{symbol} 历史涨停日期双源不一致")
            if len(tx_dates)>=settings.min_limit_ups:
                prepared.append((row,tx_dates,verification,tx_raw,sina_raw))
        counts["repeated_rows"]=len(prepared)
        prepared=[x for x in prepared if not has_consecutive_limit_ups(x[1],recent_calendar)]
        counts["nonconsecutive_rows"]=len(prepared); results=[]
        if run_scan:
            params=ScanParams.from_settings()
            for row,limit_dates,verification,tx_raw,sina_raw in prepared:
                adjusted=_history(source,row["symbol"],trade_date,"tencent",True)
                if len(adjusted)<settings.low_window_days:
                    warnings.append(f"{row['symbol']} {INSUFFICIENT_HISTORY}"); continue
                metrics=evaluate_stages(adjusted,limit_dates,params)
                if not metrics or not metrics["passes_low"]: continue
                counts["low_rows"]+=1
                if not metrics["passes_pullback"]: continue
                counts["pullback_rows"]+=1; counts["pattern_rows"]+=1
                pool_row={"trade_date":trade_date,"close":row["raw_close"],"pct_chg":None}
                tx_status,tx_msg=verify_independent_source(pool_row,tx_raw,"腾讯")
                si_status,si_msg=verify_independent_source(pool_row,sina_raw,"新浪")
                if tx_status!="VERIFIED" or si_status!="VERIFIED": verification="DATA_MISMATCH"
                clean={k:v for k,v in metrics.items() if not k.startswith("passes_")}
                results.append({"symbol":row["symbol"],"name":row["name"],"industry":row.get("industry"),
                  "close":row["raw_close"],"total_market_cap_yi":float(row["total_market_cap"])/100_000_000,
                  "limit_up_count":len(limit_dates),"limit_dates":limit_dates,**clean,
                  "verification_status":verification,"verification_message":tx_msg+"；"+si_msg})
            counts["candidate_rows"]=save_scan_results(trade_date,results)
        history_status="SUCCESS" if not any(UNKNOWN_LIMIT_RULE in w or INSUFFICIENT_HISTORY in w for w in warnings) else "WARNING"
        status="WARNING" if warnings or any(r["verification_status"]=="DATA_MISMATCH" for r in results) else "SUCCESS"
        message="；".join(warnings[:8]) if warnings else "理论涨停、腾讯历史重建、腾讯/新浪核验完成"
        mark_pipeline_finish(trade_date,status,message,counts,audit["status"],history_status,current_time)
        return counts["candidate_rows"]
    except Exception as exc:
        mark_pipeline_finish(trade_date,"FAILED",f"{type(exc).__name__}: {exc}",counts,"FAILED",history_status,current_time)
        raise


def sync_latest_if_needed(force: bool=False) -> str:
    day=resolve_latest_completed_trade_date(); sync_market_day(day,force=force); return day


def backfill(trading_days: int=25, *, force: bool=False) -> list[str]:
    day=sync_latest_if_needed(force=force); return [day]
