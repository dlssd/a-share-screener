from app.config import settings
from app.datasource import _market_session, _network_hint, profit_status


def test_market_session_direct_mode_ignores_proxy_environment():
    original=settings.market_data_direct
    try:
        object.__setattr__(settings,"market_data_direct",True)
        with _market_session() as session:
            assert session.trust_env is False
        assert "系统/TUN代理" in _network_hint(RuntimeError("network failed"))
    finally:
        object.__setattr__(settings,"market_data_direct",original)


def test_missing_profit_is_unknown_not_loss():
    assert profit_status(None)=="未知"
    assert profit_status(1)=="盈利"
    assert profit_status(-1)=="亏损"
    assert profit_status(0)=="盈亏平衡"
