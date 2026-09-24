"""One-off probe: print the live schemas returned by both free data sources."""
from __future__ import annotations

import sys
from datetime import date, timedelta

import akshare as ak
from app.config import settings
from app.datasource import AKShareSource, _requests_timeout


def main() -> int:
    target = sys.argv[1] if len(sys.argv) > 1 else date.today().strftime("%Y%m%d")
    print(f"AKShare version={ak.__version__}, requested date={target}")
    try:
        with _requests_timeout(settings.request_timeout):
            frame = ak.stock_zt_pool_em(date=target)
        print("AKShare columns:", list(frame.columns))
        print("AKShare rows:", len(frame))
        print(frame.head(3).to_string(index=False))
        print("Observed units: 最新价=人民币元; 涨跌幅=百分数; 成交额/流通市值/总市值=人民币元")
    except Exception as exc:
        print(f"AKShare ERROR: {type(exc).__name__}: {exc}")

    source = AKShareSource()
    start = (date.today() - timedelta(days=450)).strftime("%Y%m%d")
    for provider, fetch in (
        ("Tencent raw", lambda: source.tencent_history("000001", start, target, adjusted=False)),
        ("Tencent qfq", lambda: source.tencent_history("000001", start, target, adjusted=True)),
        ("Sina raw", lambda: source.sina_history("000001", start, target, adjusted=False)),
        ("Sina qfq", lambda: source.sina_history("000001", start, target, adjusted=True)),
    ):
        try:
            hist = fetch()
            print(provider, "rows=", len(hist), "columns=", list(hist.columns), "tail=", hist.tail(1).to_dict("records"))
        except Exception as exc:
            print(f"{provider} ERROR: {type(exc).__name__}: {exc}")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
