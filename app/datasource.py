from __future__ import annotations

import socket
import signal
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Callable

import pandas as pd

from .config import settings

_REQUESTS_PATCH_LOCK = threading.RLock()


class DataSourceError(RuntimeError):
    pass


def profit_status(profit: float | None) -> str:
    return "未知" if profit is None else ("盈利" if profit>0 else ("亏损" if profit<0 else "盈亏平衡"))


@contextmanager
def _deadline(seconds: float):
    """Hard wall-clock timeout for blocking BaoStock socket operations on macOS/Linux."""
    if not hasattr(signal, "SIGALRM"):
        yield
        return
    previous = signal.getsignal(signal.SIGALRM)
    def expired(signum, frame):
        raise TimeoutError(f"external request exceeded {seconds}s")
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def _retry(label: str, fn: Callable):
    last = None
    for attempt in range(settings.request_retries):
        try:
            return fn()
        except Exception as exc:
            last = exc
            if attempt + 1 < settings.request_retries:
                time.sleep(2**attempt)
    domestic=("AKShare","Eastmoney","STAR","individual","Sina","Tencent")
    detail=_network_hint(last) if settings.market_data_direct and label.startswith(domestic) else str(last)
    raise DataSourceError(f"{label} failed after {settings.request_retries} attempts: {detail}") from last


def _timed(fn: Callable):
    with _deadline(settings.request_timeout):
        return fn()


@contextmanager
def _requests_timeout(seconds: float):
    """Give AKShare calls a timeout and optionally ignore proxy environment variables.

    This patch is scoped to the datasource call. It does not alter the process
    environment or the user's system/VPN configuration.
    """
    import requests
    # AKShare does not consistently expose a timeout/session argument.  Keep
    # its compatibility shim serialized inside this process; full scans run in
    # a separate process, so this never blocks ordinary FastAPI page requests.
    with _REQUESTS_PATCH_LOCK:
        original = requests.sessions.Session.request

        def request(session, method, url, **kwargs):
            if settings.market_data_direct:
                session.trust_env = False
            kwargs.setdefault("timeout", seconds)
            return original(session, method, url, **kwargs)

        requests.sessions.Session.request = request
        try:
            yield
        finally:
            requests.sessions.Session.request = original


def _market_session():
    """Independent domestic-market session; direct mode does not inherit HTTP(S)_PROXY."""
    import requests
    session = requests.Session()
    session.trust_env = not settings.market_data_direct
    return session


def _network_hint(exc: Exception) -> str:
    text = str(exc)
    if settings.market_data_direct and "系统/TUN代理" not in text:
        text += ("；当前网络可能通过系统/TUN代理，建议将 eastmoney.com / gtimg.cn / "
                 "sina.com.cn 设置为直连。")
    return text


class AKShareSource:
    source_name = "akshare_eastmoney"

    def limit_up_pool(self, trade_date: str) -> pd.DataFrame:
        def fetch():
            import akshare as ak
            with _requests_timeout(settings.request_timeout):
                return ak.stock_zt_pool_em(date=trade_date)

        raw = _retry(f"AKShare limit-up pool {trade_date}", fetch)
        required = {"代码", "名称", "最新价", "总市值"}
        missing = required - set(raw.columns)
        if missing:
            raise DataSourceError(f"AKShare schema changed; missing columns: {sorted(missing)}")
        mapping = {"代码": "symbol", "名称": "name", "最新价": "close", "总市值": "total_market_cap",
                   "所属行业": "industry", "连板数": "limit_up_count", "涨跌幅": "pct_chg",
                   "首次封板时间": "first_limit_time", "最后封板时间": "last_limit_time"}
        frame = raw.rename(columns=mapping)
        for optional in set(mapping.values()) - set(frame.columns):
            frame[optional] = None
        frame = frame[list(mapping.values())].copy()
        frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
        frame["trade_date"] = trade_date
        for col in ("close", "total_market_cap", "limit_up_count", "pct_chg"):
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return frame

    def full_market_spot(self) -> pd.DataFrame:
        """沪深京全市场当前快照；调用者必须确认当前日已经完整收盘。"""
        def fetch():
            params={"pn":"1","pz":"10000","po":"1","np":"1",
                    "ut":"bd1d9ddb04089700cf9c27f6f7426281","fltt":"2","invt":"2","fid":"f12",
                    "fs":"m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
                    "fields":"f2,f3,f4,f5,f6,f12,f13,f14,f15,f16,f17,f18,f20,f21"}
            last=None
            with _market_session() as session:
                for host in ("push2.eastmoney.com","20.push2.eastmoney.com","82.push2.eastmoney.com"):
                    try:
                        response=session.get(f"https://{host}/api/qt/clist/get",params=params,timeout=settings.request_timeout)
                        response.raise_for_status(); data=response.json().get("data")
                        if data and data.get("diff"): return pd.DataFrame(data["diff"])
                    except Exception as exc: last=exc
            raise DataSourceError(f"full market spot failed: {_network_hint(last)}")
        raw=_retry("Eastmoney full market spot",fetch)
        mapping={"f12":"symbol","f14":"name","f2":"raw_close","f18":"previous_close",
                 "f3":"pct_chg","f20":"total_market_cap","f21":"float_market_cap",
                 "f15":"high","f16":"low","f17":"open","f5":"volume","f6":"amount"}
        frame=raw.rename(columns=mapping)
        missing=set(mapping.values())-set(frame.columns)
        if missing: raise DataSourceError(f"spot schema changed: {sorted(missing)}")
        frame=frame[list(mapping.values())].copy(); frame["symbol"]=frame["symbol"].astype(str).str.zfill(6)
        for col in set(mapping.values())-{"symbol","name"}: frame[col]=pd.to_numeric(frame[col],errors="coerce")
        return frame

    def star_stock_list(self) -> pd.DataFrame:
        def fetch():
            import akshare as ak
            with _requests_timeout(settings.request_timeout): return ak.stock_info_sh_name_code(symbol="科创板")
        raw=_retry("STAR stock list",fetch)
        return raw.rename(columns={"证券代码":"symbol","证券简称":"name","上市日期":"listing_date"})[["symbol","name","listing_date"]]

    def individual_info(self, symbol: str) -> dict:
        def fetch():
            import akshare as ak
            with _requests_timeout(settings.request_timeout): return ak.stock_individual_info_em(symbol=symbol)
        raw=_retry(f"individual info {symbol}",fetch)
        return dict(zip(raw["item"],raw["value"]))

    def trading_days(self, start_date: str, end_date: str) -> list[str]:
        def fetch():
            import akshare as ak
            with _requests_timeout(settings.request_timeout):
                return ak.tool_trade_date_hist_sina()
        raw = _retry("Sina trading calendar", fetch)
        values = pd.to_datetime(raw["trade_date"], errors="coerce").dropna()
        days = values.dt.strftime("%Y%m%d")
        return days[(days >= start_date) & (days <= end_date)].tolist()

    @staticmethod
    def market_symbol(symbol: str) -> str:
        symbol=str(symbol).zfill(6)
        if symbol.startswith("9"):
            return "bj" + symbol
        return ("sh" if symbol.startswith(("5","6")) else "sz") + symbol

    def news(self, symbol: str) -> list[dict]:
        """Fetch on demand only; caller owns caching and presentation."""
        def fetch():
            import akshare as ak
            with _requests_timeout(settings.request_timeout):
                return ak.stock_news_em(symbol=str(symbol).zfill(6))
        raw=_retry(f"AKShare news {symbol}",fetch)
        if raw is None or raw.empty:
            return []
        columns={str(c):c for c in raw.columns}
        def pick(*names):
            for name in names:
                if name in columns: return columns[name]
            return None
        title=pick("新闻标题","标题","title"); published=pick("发布时间","发布时间"); source=pick("文章来源","来源")
        if not title: return []
        result=[]
        for _, row in raw.head(5).iterrows():
            result.append({"title":str(row.get(title,"")),"published_at":str(row.get(published,"")) if published else "",
                           "source":str(row.get(source,"")) if source else ""})
        return result

    def fundamental_summary(self, symbol: str, as_of_date: str) -> dict:
        """Latest report publicly noticed by as_of_date; Eastmoney free financial indicators."""
        def fetch():
            import akshare as ak
            suffix="SH" if str(symbol).startswith(("5","6")) else ("BJ" if str(symbol).startswith("9") else "SZ")
            with _requests_timeout(settings.request_timeout):
                return ak.stock_financial_analysis_indicator_em(symbol=f"{str(symbol).zfill(6)}.{suffix}",indicator="按报告期")
        raw=_retry(f"Eastmoney fundamental {symbol}",fetch)
        if raw is None or raw.empty: raise DataSourceError(f"fundamental {symbol}: empty")
        data=raw.copy(); data["NOTICE_DATE"]=pd.to_datetime(data["NOTICE_DATE"],errors="coerce")
        cutoff=pd.to_datetime(as_of_date); data=data[data["NOTICE_DATE"]<=cutoff].sort_values("REPORT_DATE",ascending=False)
        if data.empty: raise DataSourceError(f"fundamental {symbol}: no report available by {as_of_date}")
        row=data.iloc[0]
        number=lambda key: None if key not in row or pd.isna(row[key]) else float(row[key])
        profit=number("PARENTNETPROFIT")
        status=profit_status(profit)
        return {"report_date":pd.to_datetime(row["REPORT_DATE"]).strftime("%Y-%m-%d"),
                "revenue":number("TOTALOPERATEREVE"),"revenue_yoy":number("TOTALOPERATEREVETZ"),
                "net_profit":profit,"net_profit_yoy":number("PARENTNETPROFITTZ"),
                "profit_status":status}

    def tencent_history(self, symbol: str, start_date: str, end_date: str, *, adjusted: bool) -> pd.DataFrame:
        def fetch():
            import akshare as ak
            if str(symbol).zfill(6).startswith("9"):
                return self._tencent_bse_history(symbol,start_date,end_date,adjusted)
            with _requests_timeout(settings.request_timeout):
                return ak.stock_zh_a_hist_tx(
                    symbol=self.market_symbol(symbol), start_date=start_date, end_date=end_date,
                    adjust="qfq" if adjusted else "", timeout=settings.request_timeout)
        raw = _retry(f"Tencent history {symbol}", fetch)
        return self._normalize_history(raw, symbol, "tencent", adjusted)

    def tencent_index_history(self, start_date: str, end_date: str) -> pd.DataFrame:
        def fetch():
            import akshare as ak
            with _requests_timeout(settings.request_timeout):
                return ak.stock_zh_a_hist_tx(symbol="sh000001",start_date=start_date,end_date=end_date,
                                             adjust="",timeout=settings.request_timeout)
        return self._normalize_history(_retry("Tencent Shanghai index",fetch),"000001","tencent_index",False)

    @staticmethod
    def _tencent_bse_history(symbol: str, start_date: str, end_date: str, adjusted: bool) -> pd.DataFrame:
        """Tencent's public endpoint supports BJ symbols, while AKShare's
        year-loop parser can fail when one year has a different response shape.
        Keep the same endpoint and normalize only this provider edge case.
        """
        import requests
        from akshare.utils import demjson
        key="bj"+str(symbol).zfill(6)
        url="https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
        rows=[]
        with _market_session() as session:
            for year in range(int(start_date[:4]),int(end_date[:4])+1):
                params={"_var":f"kline_day{year}","param":f"{key},day,{year}-01-01,{year+1}-12-31,640,{('qfq' if adjusted else '')}","r":"0.8205512681390605"}
                response=session.get(url,params=params,timeout=settings.request_timeout)
                response.raise_for_status()
                payload=demjson.decode(response.text[response.text.find("={")+1:])
                data=payload.get("data",{}).get(key,{})
                series=data.get("qfqday" if adjusted else "day") or data.get("day") or data.get("hfqday") or []
                rows.extend(series)
        frame=pd.DataFrame(rows)
        if frame.empty:
            raise DataSourceError(f"Tencent history {symbol}: empty BJ response")
        frame=frame.iloc[:,[0,1,2,3,4,5,7,8]]
        frame.columns=["date","open","close","high","low","volume","turnover","amount"]
        frame["date"]=pd.to_datetime(frame["date"],errors="coerce")
        frame=frame[(frame["date"]>=pd.to_datetime(start_date)) & (frame["date"]<=pd.to_datetime(end_date))]
        return frame.reset_index(drop=True)

    def sina_history(self, symbol: str, start_date: str, end_date: str, *, adjusted: bool) -> pd.DataFrame:
        def fetch():
            import akshare as ak
            with _requests_timeout(settings.request_timeout):
                return ak.stock_zh_a_daily(
                    symbol=self.market_symbol(symbol), start_date=start_date, end_date=end_date,
                    adjust="qfq" if adjusted else "")
        raw = _retry(f"Sina history {symbol}", fetch)
        return self._normalize_history(raw, symbol, "sina", adjusted)

    @staticmethod
    def _normalize_history(raw: pd.DataFrame, symbol: str, source: str, adjusted: bool) -> pd.DataFrame:
        required = {"date", "open", "high", "low", "close"}
        if raw.empty or not required.issubset(raw.columns):
            raise DataSourceError(f"{source} history {symbol}: empty or schema changed")
        frame = raw.copy().rename(columns={"date": "trade_date"})
        frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.strftime("%Y%m%d")
        for col in ("open", "high", "low", "close", "volume", "amount"):
            if col in frame: frame[col] = pd.to_numeric(frame[col], errors="coerce")
        close_name = "adjusted_close" if adjusted else "raw_close"
        frame[close_name] = frame["close"]
        frame["pct_chg"] = frame["close"].pct_change() * 100
        frame["symbol"], frame["source"] = symbol, source
        return frame


class BaoStockSource:
    source_name = "baostock"

    @contextmanager
    def session(self):
        import baostock as bs
        import baostock.common.context as bs_context
        import baostock.util.socketutil as socketutil
        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(settings.request_timeout)
        original_connect = socketutil.SocketUtil.connect

        def timed_connect(instance):
            original_connect(instance)
            if hasattr(bs_context, "default_socket"):
                bs_context.default_socket.settimeout(settings.request_timeout)

        socketutil.SocketUtil.connect = timed_connect
        try:
            result = _retry("BaoStock login", lambda: _timed(bs.login))
        finally:
            socketutil.SocketUtil.connect = original_connect
        if result.error_code != "0":
            socket.setdefaulttimeout(old_timeout)
            raise DataSourceError(f"BaoStock login: {result.error_code} {result.error_msg}")
        try:
            yield bs
        finally:
            try:
                bs.logout()
            finally:
                socket.setdefaulttimeout(old_timeout)

    @staticmethod
    def bs_code(symbol: str) -> str:
        return ("sh." if symbol.startswith(("5", "6", "9")) else "sz.") + symbol

    @staticmethod
    def _collect(result, label: str) -> pd.DataFrame:
        if result.error_code != "0":
            raise DataSourceError(f"{label}: {result.error_code} {result.error_msg}")
        rows = []
        while result.next():
            rows.append(result.get_row_data())
        return pd.DataFrame(rows, columns=result.fields)

    def trading_days(self, start_date: str, end_date: str) -> list[str]:
        with self.session() as bs:
            result = _retry("BaoStock trade calendar", lambda: _timed(lambda: bs.query_trade_dates(
                    start_date=datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d"),
                    end_date=datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d"))))
            df = self._collect(result, "BaoStock trade calendar")
        if df.empty:
            return []
        return df.loc[df["is_trading_day"] == "1", "calendar_date"].str.replace("-", "").tolist()

    def history(self, symbol: str, start_date: str, end_date: str, adjustflag: str = "2") -> pd.DataFrame:
        # BaoStock: adjustflag=2 前复权, 1 后复权, 3 不复权。低位序列统一使用前复权。
        fields = "date,code,open,high,low,close,preclose,volume,amount,pctChg,tradestatus,isST"
        with self.session() as bs:
            result = _retry(f"BaoStock history {symbol}", lambda: _timed(lambda: bs.query_history_k_data_plus(
                    self.bs_code(symbol), fields,
                    start_date=datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d"),
                    end_date=datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d"),
                    frequency="d", adjustflag=adjustflag)))
            df = self._collect(result, f"BaoStock history {symbol}")
        if df.empty:
            return df
        df = df.rename(columns={"date": "trade_date", "pctChg": "pct_chg", "tradestatus": "trade_status", "isST": "is_st"})
        df["trade_date"] = df["trade_date"].str.replace("-", "")
        for col in ("open", "high", "low", "close", "preclose", "volume", "amount", "pct_chg"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["symbol"] = symbol
        df["adjust_status"] = {"1": "post", "2": "pre", "3": "none"}[adjustflag]
        return df
