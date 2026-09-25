from __future__ import annotations

from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo

import pandas as pd

from .audit import build_market_audit
from .calendar import is_trade_date, trade_dates_between
from .config import settings
from .datasource import AKShareSource
from .db import (connect, fail_scan_run, finish_scan_run, get_industry_cache, init_db,
                 pipeline_success, save_industry_cache, start_scan_run)
from .limit_rules import (INSUFFICIENT_HISTORY, UNKNOWN_LIMIT_RULE, detect_limit_up_days,
                          theoretical_limit_price, limit_rate)
from .scanner import ScanParams, evaluate_stages, has_consecutive_limit_ups, verify_independent_source


class DataValidationError(RuntimeError): pass


def shanghai_now() -> datetime: return datetime.now(ZoneInfo(settings.timezone))


def resolve_latest_completed_trade_date(source: AKShareSource | None=None, now: datetime | None=None) -> str:
    source=source or AKShareSource(); now=now or shanghai_now()
    if now.tzinfo is None: now=now.replace(tzinfo=ZoneInfo(settings.timezone))
    today=now.strftime("%Y%m%d")
    days=trade_dates_between((now.date()-timedelta(days=45)).strftime("%Y%m%d"),today,source)
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


def _industry_for(source: AKShareSource, row: dict, warnings: list[str]) -> str | None:
    value=row.get("industry")
    if value is not None and str(value).strip() and str(value).strip().lower() not in {"nan","none"}:
        return str(value).strip()
    symbol=str(row["symbol"])
    cached=get_industry_cache(symbol)
    if cached:
        return cached["industry"]
    try:
        info=source.individual_info(symbol)
        value=info.get("行业") or info.get("所属行业")
        value=str(value).strip() if value is not None else None
    except Exception as exc:
        value=None
        warnings.append(f"{symbol} 行业获取失败: {type(exc).__name__}")
    save_industry_cache(symbol,str(row.get("name", "")),value,"akshare_individual_info")
    return value


def _market_panorama(source: AKShareSource, universe: list[dict], trade_date: str,
                     recent_calendar: list[str]) -> tuple[list[dict], dict[str,pd.DataFrame], list[str]]:
    """Enrich every detected limit-up with short raw history; failures stay visible."""
    start=(datetime.strptime(trade_date,"%Y%m%d")-timedelta(days=100)).strftime("%Y%m%d")
    histories={}; errors=[]
    def fetch(row):
        return row["symbol"],source.tencent_history(row["symbol"],start,trade_date,adjusted=False)
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures={pool.submit(fetch,row):row for row in universe}
        for future in as_completed(futures):
            try:
                symbol,frame=future.result(); histories[symbol]=frame
            except Exception as exc:
                errors.append(f"{futures[future]['symbol']} 全景历史失败: {type(exc).__name__}")
    rows=[]
    for item in universe:
        symbol=str(item["symbol"]); frame=histories.get(symbol); dates=[]; status="DATA_UNAVAILABLE"
        open_=high=low=pct=None
        if frame is not None and not frame.empty:
            detection=detect_limit_up_days(frame,symbol,item["name"]); status=detection.status
            dates=[d for d in detection.dates if d in recent_calendar]
            last=frame.iloc[-1]; open_=float(last["open"]); high=float(last["high"]); low=float(last["low"])
            pct=float(last.get("pct_chg")) if pd.notna(last.get("pct_chg")) else None
        trailing=0
        for day in reversed(recent_calendar):
            if day in dates: trailing+=1
            else: break
        rate=limit_rate(symbol,str(item["name"])); expected=None
        previous=item.get("previous_close")
        if rate is not None and previous is not None and pd.notna(previous):
            expected=theoretical_limit_price(float(previous),rate)
        at_limit=lambda value: expected is not None and value is not None and abs(value-expected)<=.0051
        t_board=bool(at_limit(open_) and at_limit(high) and at_limit(float(item["raw_close"])) and low is not None and low<expected-.0051)
        one_word=bool(at_limit(open_) and at_limit(high) and at_limit(low) and at_limit(float(item["raw_close"])))
        board_label="首板" if trailing<=1 else (f"{trailing}板" if trailing<4 else "4板及以上")
        cache=get_industry_cache(symbol)
        industry=item.get("industry") or (cache["industry"] if cache else None)
        rows.append({"symbol":symbol,"name":item["name"],"industry":industry,"market":item.get("market","UNKNOWN"),
          "close":item.get("raw_close"),"total_market_cap_yi":float(item["total_market_cap"])/1e8 if item.get("total_market_cap") else None,
          "pct_chg":pct,"recent_limit_count":len(dates),"consecutive_boards":max(trailing,1),
          "board_label":board_label,"is_t_board":t_board,"is_one_word":one_word,"limit_dates":dates,
          "verification_status":status,"source":item.get("source","eastmoney+tencent")})
    return rows,histories,errors


def _index_environment(source: AKShareSource, trade_date: str) -> dict:
    start=(datetime.strptime(trade_date,"%Y%m%d")-timedelta(days=520)).strftime("%Y%m%d")
    try:
        frame=source.tencent_index_history(start,trade_date).sort_values("trade_date")
        if frame.empty or frame.iloc[-1]["trade_date"]!=trade_date: raise DataValidationError("指数缺少目标日")
        values=frame["raw_close"].dropna().tail(250); close=float(values.iloc[-1]); low=float(values.min()); high=float(values.max())
        position=(close-low)/(high-low) if high>low else 0.0; ma=float(values.mean())
        return {"symbol":"000001","name":"上证指数","close":close,"low_position_pct":position,
          "distance_from_high_pct":close/high-1,"distance_from_low_pct":close/low-1,
          "ma250_distance_pct":close/ma-1,"recent_return_pct":close/float(values.iloc[-20])-1,
          "level_label":"低位" if position<=.3 else ("偏高" if position>=.7 else "中位"),
          "status":"SUCCESS","message":"","source":"tencent_index_raw"}
    except Exception as exc:
        return {"symbol":"000001","name":"上证指数","status":"WARNING",
                "message":f"指数数据暂不可用: {type(exc).__name__}","source":"tencent_index_raw"}


def sync_market_day(trade_date: str, *, run_scan: bool=True, force: bool=False) -> int:
    trade_date=trade_date.replace("-", "")
    init_db(); source=AKShareSource(); now=shanghai_now()
    if not is_trade_date(trade_date,source):
        raise DataValidationError(f"{trade_date} 为A股休市日，不能生成盘后复盘")
    latest=resolve_latest_completed_trade_date(source,now)
    if trade_date>latest: raise DataValidationError(f"{trade_date} 尚非完整盘后交易日；最近完整日为 {latest}")
    if pipeline_success(trade_date) and not force:
        with connect() as conn: return int(conn.execute("SELECT candidate_rows FROM pipeline_runs WHERE trade_date=?",(trade_date,)).fetchone()[0])
    current_time=now.strftime("%Y-%m-%d %H:%M Asia/Shanghai")
    run_id=start_scan_run(trade_date,current_time)
    counts={k:0 for k in ("pool_rows","cap_rows","repeated_rows","nonconsecutive_rows","low_rows","pullback_rows","pattern_rows","candidate_rows")}
    warnings=[]; history_status="PENDING"
    try:
        universe,audit=build_market_audit(trade_date,force=force)
        counts["pool_rows"]=len(universe)
        if audit["status"]=="WARNING": warnings.append("全市场审计为 WARNING: "+audit.get("message", ""))
        cap=[r for r in universe if r.get("total_market_cap") is not None and float(r["total_market_cap"])>=settings.market_cap_min_cny]
        counts["cap_rows"]=len(cap); recent_calendar=trade_dates_between(
            (datetime.strptime(trade_date,"%Y%m%d")-timedelta(days=60)).strftime("%Y%m%d"),trade_date,source)[-settings.recent_trading_days:]
        market_rows,raw_histories,panorama_errors=_market_panorama(source,universe,trade_date,recent_calendar)
        warnings.extend(panorama_errors[:5]); market_environment=_index_environment(source,trade_date)
        if market_environment["status"]!="SUCCESS": warnings.append(market_environment["message"])
        stage_rows=[]
        params=ScanParams.from_settings()
        for row in cap:
            symbol=row["symbol"]
            industry=_industry_for(source,row,warnings)
            stage={"symbol":symbol,"name":row["name"],"industry":row.get("industry"),
              "close":row["raw_close"],"total_market_cap_yi":float(row["total_market_cap"])/100_000_000,
              "limit_up_count":0,"limit_dates":[],"has_consecutive_limit_up":None,
              "passes_market_cap":True,"passes_repeat_limit":False,"passes_nonconsecutive":False,
              "passes_low":False,"passes_pullback":False,"passes_recent_return":False,
              "passes_final":False,"verification_status":"NOT_CHECKED","verification_message":None,
              "reject_reasons":[]}
            stage["industry"]=industry
            try:
                tx_raw=raw_histories.get(symbol)
                if tx_raw is None: tx_raw=_history(source,symbol,trade_date,"tencent",False)
                sina_raw=_history(source,symbol,trade_date,"sina",False)
                tx_detection=detect_limit_up_days(tx_raw,symbol,row["name"])
                sina_detection=detect_limit_up_days(sina_raw,symbol,row["name"])
                if tx_detection.status==UNKNOWN_LIMIT_RULE or sina_detection.status==UNKNOWN_LIMIT_RULE:
                    stage["verification_status"]=UNKNOWN_LIMIT_RULE
                    stage["reject_reasons"].append("涨停规则暂无法可靠确认")
                    warnings.append(f"{symbol} {UNKNOWN_LIMIT_RULE}")
                    stage_rows.append(stage); continue
                tx_dates=[d for d in tx_detection.dates if d in recent_calendar]
                sina_dates=[d for d in sina_detection.dates if d in recent_calendar]
                stage["limit_dates"]=tx_dates; stage["limit_up_count"]=len(tx_dates)
                # A newly listed stock with fewer than 20 observations cannot
                # provide a complete 20-trading-day window, even if it has
                # already hit the limit twice. Keep it visible but fail closed.
                complete_recent_window=(len(tx_raw)>=settings.recent_trading_days and
                                        len(sina_raw)>=settings.recent_trading_days)
                stage["passes_repeat_limit"]=(complete_recent_window and
                                               len(tx_dates)>=settings.min_limit_ups)
                consecutive=has_consecutive_limit_ups(tx_dates,recent_calendar)
                stage["has_consecutive_limit_up"]=consecutive
                stage["passes_nonconsecutive"]=stage["passes_repeat_limit"] and not consecutive
                pool_row={"trade_date":trade_date,"close":row["raw_close"],"pct_chg":None}
                tx_status,tx_msg=verify_independent_source(pool_row,tx_raw,"腾讯")
                si_status,si_msg=verify_independent_source(pool_row,sina_raw,"新浪")
                dates_match=tx_dates==sina_dates
                verified=tx_status=="VERIFIED" and si_status=="VERIFIED" and dates_match
                stage["verification_status"]="VERIFIED" if verified else "DATA_MISMATCH"
                stage["verification_message"]=tx_msg+"；"+si_msg
                if not dates_match:
                    stage["verification_message"] += "；腾讯/新浪历史涨停日期不一致"
                if not verified: warnings.append(f"{symbol} 历史行情双源不一致")
                if not complete_recent_window:
                    stage["reject_reasons"].append("上市时间较短，无法形成完整20交易日统计")
                elif not stage["passes_repeat_limit"]:
                    stage["reject_reasons"].append(
                        f"最近20日涨停{len(tx_dates)}次，要求至少{settings.min_limit_ups}次")
                elif consecutive:
                    stage["reject_reasons"].append("最近20日存在连续交易日涨停")

                if run_scan:
                    adjusted=_history(source,symbol,trade_date,"tencent",True)
                    metrics=evaluate_stages(adjusted,tx_dates,params)
                    if metrics is None:
                        stage["verification_status"]=INSUFFICIENT_HISTORY
                        stage["reject_reasons"].append("上市时间较短，历史不足250日")
                        warnings.append(f"{symbol} {INSUFFICIENT_HISTORY}")
                    else:
                        for key in ("low_position_pct","distance_from_low_pct","pullback_pct","recent_return_pct",
                                    "passes_low","passes_pullback","passes_recent_return"):
                            stage[key]=metrics[key]
                        if stage["passes_repeat_limit"] and stage["passes_nonconsecutive"]:
                            if not stage["passes_low"]:
                                stage["reject_reasons"].append(
                                    f"250日位置{metrics['low_position_pct']*100:.1f}%，要求≤{settings.max_low_position*100:.0f}%")
                            if not stage["passes_pullback"]:
                                value=metrics["pullback_pct"]
                                if value is None:
                                    reason="前次涨停后没有可计算的回撤区间"
                                elif value < 0:
                                    reason=(f"未发生回撤（最低价仍高于前次涨停收盘"
                                            f"{(-value)*100:.1f}%）")
                                else:
                                    reason=(f"前次涨停后回撤{value*100:.1f}%，要求"
                                            f"{settings.min_pullback_pct*100:.0f}%～{settings.max_pullback_pct*100:.0f}%")
                                stage["reject_reasons"].append(reason)
                            if not stage["passes_recent_return"]:
                                stage["reject_reasons"].append(
                                    f"最近20日涨幅{metrics['recent_return_pct']*100:.1f}%，要求≤{settings.max_recent_return_pct*100:.0f}%")
                        stage["passes_final"]=all((stage["passes_market_cap"],stage["passes_repeat_limit"],
                            stage["passes_nonconsecutive"],stage["passes_low"],stage["passes_pullback"],
                            stage["passes_recent_return"]))
            except Exception as exc:
                stage["verification_status"]="DATA_UNAVAILABLE"
                stage["verification_message"]=f"{type(exc).__name__}: {exc}"
                stage["reject_reasons"].append("历史行情暂时无法完整获取")
                warnings.append(f"{symbol} {type(exc).__name__}")
            stage_rows.append(stage)

        counts["repeated_rows"]=sum(r["passes_repeat_limit"] for r in stage_rows)
        counts["nonconsecutive_rows"]=sum(r["passes_nonconsecutive"] for r in stage_rows)
        counts["low_rows"]=sum(r["passes_nonconsecutive"] and r["passes_low"] for r in stage_rows)
        counts["pullback_rows"]=sum(r["passes_nonconsecutive"] and r["passes_low"] and r["passes_pullback"] for r in stage_rows)
        counts["pattern_rows"]=counts["pullback_rows"]
        counts["candidate_rows"]=sum(1 for r in stage_rows if r.get("passes_final"))
        history_status="SUCCESS" if not any(UNKNOWN_LIMIT_RULE in w or INSUFFICIENT_HISTORY in w for w in warnings) else "WARNING"
        status="WARNING" if warnings or any(r["verification_status"]=="DATA_MISMATCH" for r in stage_rows) else "SUCCESS"
        message="；".join(warnings[:8]) if warnings else "理论涨停、腾讯历史重建、腾讯/新浪核验完成"
        return finish_scan_run(run_id,trade_date,status,message,counts,audit["status"],history_status,current_time,stage_rows,
                               market_rows,market_environment)
    except Exception as exc:
        fail_scan_run(run_id,trade_date,f"{type(exc).__name__}: {exc}",current_time)
        raise


def sync_latest_if_needed(force: bool=False) -> str:
    day=resolve_latest_completed_trade_date(); sync_market_day(day,force=force); return day


def backfill(trading_days: int=25, *, force: bool=False) -> list[str]:
    day=sync_latest_if_needed(force=force); return [day]


def backfill_range(start_date: str, end_date: str, *, force: bool=False) -> list[str]:
    """Run only exchange trading days; published snapshots remain immutable until replacement succeeds."""
    days=trade_dates_between(start_date,end_date)
    completed=resolve_latest_completed_trade_date()
    done=[]
    for day in days:
        if day>completed:
            continue
        sync_market_day(day,force=force)
        done.append(day)
    return done
