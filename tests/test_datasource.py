from app.config import settings
from app.datasource import _market_session, _network_hint


def test_market_session_direct_mode_ignores_proxy_environment():
    original=settings.market_data_direct
    try:
        object.__setattr__(settings,"market_data_direct",True)
        with _market_session() as session:
            assert session.trust_env is False
        assert "系统/TUN代理" in _network_hint(RuntimeError("network failed"))
    finally:
        object.__setattr__(settings,"market_data_direct",original)
