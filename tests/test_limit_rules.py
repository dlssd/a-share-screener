import pandas as pd
import pytest

from app.limit_rules import detect_limit_up_days, market_for_symbol, theoretical_limit_price
from app.datasource import AKShareSource


def test_board_classification():
    assert market_for_symbol("600000")=="MAIN"
    assert market_for_symbol("300001")=="CHINEXT"
    assert market_for_symbol("688001")=="STAR"
    assert market_for_symbol("920001")=="BSE"


def test_theoretical_limit_round_half_up():
    from decimal import Decimal
    assert theoretical_limit_price(10.05,Decimal("0.10"))==11.06


@pytest.mark.parametrize("symbol,close",[("600000",11.0),("300001",12.0),("688001",12.0),("920001",13.0)])
def test_detects_board_specific_limit(symbol,close):
    h=pd.DataFrame({"trade_date":["20260922","20260923"],"raw_close":[10.0,close]})
    assert detect_limit_up_days(h,symbol).dates==["20260923"]


def test_adjusted_close_rejected_for_limit_detection():
    with pytest.raises(ValueError):
        detect_limit_up_days(pd.DataFrame({"trade_date":["20260922"],"adjusted_close":[10]}),"600000")


def test_tencent_bse_symbol_uses_bj_prefix():
    assert AKShareSource.market_symbol("920748") == "bj920748"
