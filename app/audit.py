from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd

from .config import settings
from .datasource import AKShareSource
from .db import (connect, get_industry_cache, init_db, save_industry_cache,
                 transaction, upsert_dataframe, utcnow)
from .limit_rules import detect_limit_up_days, market_for_symbol, theoretical_limit_price, limit_rate


def _enabled(symbol: str) -> bool:
    market=market_for_symbol(symbol)
    return ((market=="MAIN") or (market=="STAR" and settings.include_star_market) or
            (market=="CHINEXT" and settings.include_chinext) or (market=="BSE" and settings.include_bse))


def _save(trade_date: str, universe: list[dict], report: dict) -> None:
    frame=pd.DataFrame(universe)
    if not frame.empty:
        frame["trade_date"]=trade_date; frame["fetched_at"]=utcnow()
        cols=["trade_date","symbol","name","raw_close","previous_close","total_market_cap","industry",
              "market","detection_status","source","fetched_at"]
        with transaction() as conn:
            upsert_dataframe(conn,"daily_limit_universe",frame,cols,["trade_date","symbol"])
    with connect() as conn:
        conn.execute("""INSERT OR REPLACE INTO market_audits
          (trade_date,status,market_rows,theoretical_rows,pool_rows,common_rows,only_theoretical_json,only_pool_json,
           main_rows,chinext_rows,star_rows,bse_rows,st_excluded_rows,unknown_rows,message,fetched_at,warning_type)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (trade_date,report["status"],report["market_rows"],report["theoretical_rows"],report["pool_rows"],
           report["common_rows"],json.dumps(report["only_theoretical"],ensure_ascii=False),
           json.dumps(report["only_pool"],ensure_ascii=False),report["main_rows"],report["chinext_rows"],
           report["star_rows"],report["bse_rows"],report["st_excluded_rows"],report["unknown_rows"],
           report["message"],utcnow(),report.get("warning_type"))); conn.commit()


def _enrich_industries(source: AKShareSource, universe: list[dict], pool: pd.DataFrame) -> None:
    """Pool industry wins, then persistent cache, then best-effort detail lookup."""
    pool_industry={str(r["symbol"]):r.get("industry") for r in pool.to_dict("records")}
    def clean(value):
        if value is None or pd.isna(value): return None
        text=str(value).strip()
        return text if text and text.lower() not in {"nan","none","-"} else None
    for row in universe:
        symbol=str(row["symbol"]); value=clean(pool_industry.get(symbol)) or clean(row.get("industry"))
        if not value:
            cached=get_industry_cache(symbol); value=clean(cached["industry"]) if cached else None
        if not value:
            try:
                info=source.individual_info(symbol); value=info.get("行业") or info.get("所属行业")
                value=str(value).strip() if value else None
                save_industry_cache(symbol,str(row.get("name","")),value,"akshare_individual_info")
            except Exception:
                value=None
        row["industry"]=value


def _spot_universe(source: AKShareSource) -> tuple[list[dict],int,int]:
    spot=source.full_market_spot(); out=[]; unknown=0; st_excluded=0
    for row in spot.to_dict("records"):
        symbol=row["symbol"]; name=str(row["name"])
        if not _enabled(symbol): continue
        if "ST" in name.upper() and settings.exclude_st: st_excluded+=1; continue
        rate=limit_rate(symbol,name)
        if rate is None or pd.isna(row["raw_close"]) or pd.isna(row["previous_close"]): unknown+=1; continue
        expected=theoretical_limit_price(float(row["previous_close"]),rate)
        if abs(float(row["raw_close"])-expected)<=.0051:
            out.append({**row,"industry":None,"market":market_for_symbol(symbol),"detection_status":"OK","source":"eastmoney_spot_theory"})
    return out,unknown,st_excluded


def _historical_fallback(source: AKShareSource, trade_date: str, pool: pd.DataFrame) -> tuple[list[dict],int,list[str]]:
    """Past-date fallback: validate pool and independently add STAR, which the pool omits."""
    start=(datetime.strptime(trade_date,"%Y%m%d")-timedelta(days=12)).strftime("%Y%m%d")
    universe=[]; unknown=[]
    def check(symbol,name,listing_date=None):
        try:
            h=AKShareSource().tencent_history(symbol,start,trade_date,adjusted=False)
            result=detect_limit_up_days(h,symbol,name,listing_date=listing_date)
            hit=trade_date in result.dates
            return symbol,name,h,result,hit,None
        except Exception as exc:
            return symbol,name,None,None,False,f"{symbol}: {type(exc).__name__}"
    pool_records=[r for r in pool.to_dict("records") if _enabled(r["symbol"]) and not (settings.exclude_st and "ST" in str(r["name"]).upper())]
    star=source.star_stock_list().to_dict("records") if settings.include_star_market else []
    futures=[]
    with ThreadPoolExecutor(max_workers=20) as executor:
        for row in pool_records: futures.append(executor.submit(check,row["symbol"],row["name"],None))
        for row in star: futures.append(executor.submit(check,str(row["symbol"]),str(row["name"]),row["listing_date"]))
        star_hits=[]; valid_pool=set()
        for future in as_completed(futures):
            symbol,name,h,result,hit,error=future.result()
            if error: unknown.append(error); continue
            if result.status!="OK": unknown.append(f"{symbol}: {result.status}")
            if hit:
                if market_for_symbol(symbol)=="STAR": star_hits.append((symbol,name,h,result))
                else: valid_pool.add(symbol)
    for row in pool_records:
        universe.append({"symbol":row["symbol"],"name":row["name"],"raw_close":row["close"],
          "previous_close":None,"total_market_cap":row["total_market_cap"],"industry":row.get("industry"),
          "market":market_for_symbol(row["symbol"]),"detection_status":"OK" if row["symbol"] in valid_pool else "POOL_ONLY",
          "source":"eastmoney_pool+tencent_theory"})
    for symbol,name,h,result in star_hits:
        try:
            sina=source.sina_history(symbol,start,trade_date,adjusted=False)
            last=sina.iloc[-1]; shares=float(last.get("outstanding_share"))
            market_cap=float(last["raw_close"])*shares
        except Exception:
            market_cap=None; unknown.append(f"{symbol}: market cap unavailable")
        universe.append({"symbol":symbol,"name":name,"raw_close":float(h.iloc[-1]["raw_close"]),
          "previous_close":float(h.iloc[-2]["raw_close"]),"total_market_cap":market_cap,"industry":None,
          "market":"STAR","detection_status":result.status,"source":"tencent_theory+sina"})
    return universe,len(unknown),unknown


def build_market_audit(trade_date: str, *, force: bool=False) -> tuple[list[dict],dict]:
    init_db()
    cached_universe=[]
    cached_report=None
    with connect() as conn:
        cached_universe=[dict(r) for r in conn.execute("SELECT * FROM daily_limit_universe WHERE trade_date=?",(trade_date,))]
        cached_report=conn.execute("SELECT * FROM market_audits WHERE trade_date=?",(trade_date,)).fetchone()
    if not force:
        if cached_universe and cached_report:
            report=dict(cached_report); report["only_theoretical"]=json.loads(report.pop("only_theoretical_json")); report["only_pool"]=json.loads(report.pop("only_pool_json"))
            return cached_universe,report
    source=AKShareSource(); pool=source.limit_up_pool(trade_date); pool_symbols=set(pool["symbol"])
    now=datetime.now().astimezone()
    same_day=trade_date==now.strftime("%Y%m%d") and now.hour>=settings.publish_after_hour
    message=""; warning_type=None
    if same_day:
        try:
            spot=source.full_market_spot()
            # Avoid making the large paginated request twice.
            universe=[]; unknown=0; st_excluded=0
            for row in spot.to_dict("records"):
                symbol=row["symbol"]; name=str(row["name"])
                if not _enabled(symbol): continue
                if "ST" in name.upper() and settings.exclude_st: st_excluded+=1; continue
                rate=limit_rate(symbol,name)
                if rate is None or pd.isna(row["raw_close"]) or pd.isna(row["previous_close"]): unknown+=1; continue
                if abs(float(row["raw_close"])-theoretical_limit_price(float(row["previous_close"]),rate))<=.0051:
                    universe.append({**row,"industry":None,"market":market_for_symbol(symbol),"detection_status":"OK","source":"eastmoney_spot_theory"})
            market_rows=len(spot); status="SUCCESS"
        except Exception as exc:
            # A prior verified audit is a safe baseline when the large snapshot
            # is unavailable. Refreshing the per-stock histories below still
            # produces a new scan run, without re-requesting every STAR symbol.
            if cached_universe:
                universe=cached_universe; unknown=0; errors=[]
            else:
                universe,unknown,errors=_historical_fallback(source,trade_date,pool)
            try:
                import akshare as ak
                listing=ak.stock_info_a_code_name(); market_rows=len(listing)
                st_excluded=int(listing["name"].astype(str).str.contains("ST",case=False).sum())
            except Exception:
                market_rows=0; st_excluded=0
            status="WARNING"; warning_type="CURRENT_SOURCE_DEGRADED"; message=f"全市场收盘快照失败，降级为东方财富池+腾讯科创板逐股审计: {type(exc).__name__}; "+", ".join(errors[:5])
    else:
        if cached_universe:
            universe=cached_universe; unknown=0; errors=[]
        else:
            universe,unknown,errors=_historical_fallback(source,trade_date,pool)
        try:
            import akshare as ak
            market_rows=len(ak.stock_info_a_code_name())
            st_excluded=int(ak.stock_info_a_code_name()["name"].astype(str).str.contains("ST",case=False).sum())
        except Exception:
            market_rows=0; st_excluded=0
        status="WARNING"; warning_type="HISTORICAL_REBUILD"; message="历史日期无可回放的全市场快照：东方财富池作基线，腾讯逐股独立检查科创板；"+", ".join(errors[:5])
    # A current AKShare/东方财富 response may already contain STAR stocks even
    # though older endpoint notes said otherwise; merge by symbol before stats.
    universe=list({r["symbol"]:r for r in universe}.values())
    _enrich_industries(source,universe,pool)
    theoretical={r["symbol"] for r in universe if r["detection_status"]=="OK"}
    common=theoretical & pool_symbols; only_theory=sorted(theoretical-pool_symbols); only_pool=sorted(pool_symbols-theoretical)
    board_counts={m:sum(1 for r in universe if r["market"]==m and r["detection_status"]=="OK")
                  for m in ("MAIN","CHINEXT","STAR","BSE")}
    final_status="WARNING" if unknown or status=="WARNING" or only_pool else "SUCCESS"
    if final_status=="WARNING" and warning_type is None: warning_type="PARTIAL_VERIFICATION"
    report={"status":final_status,"warning_type":warning_type,"market_rows":market_rows,
      "theoretical_rows":len(theoretical),"pool_rows":len(pool),"common_rows":len(common),
      "only_theoretical":only_theory,"only_pool":only_pool,"main_rows":board_counts["MAIN"],
      "chinext_rows":board_counts["CHINEXT"],"star_rows":board_counts["STAR"],"bse_rows":board_counts["BSE"],
      "st_excluded_rows":st_excluded,"unknown_rows":unknown,"message":message}
    _save(trade_date,universe,report)
    return universe,report


def print_audit(report: dict) -> None:
    labels=[("market_rows","当日全市场股票数"),("theoretical_rows","理论涨停股票数"),("pool_rows","东方财富涨停池数量"),
      ("common_rows","两者共同数量"),("main_rows","主板涨停数量"),("chinext_rows","创业板涨停数量"),
      ("star_rows","科创板涨停数量"),("bse_rows","北交所涨停数量"),("st_excluded_rows","ST被排除数量"),
      ("unknown_rows","未知规则/获取失败数量")]
    print(f"状态: {report['status']}")
    for key,label in labels: print(f"{label}: {report[key]}")
    print("仅理论计算识别:",", ".join(report["only_theoretical"]) or "无")
    print("仅东方财富涨停池:",", ".join(report["only_pool"]) or "无")
    if report.get("message"): print("说明:",report["message"])
