from broker.capital_skcom import CapitalSKCOMAdapter
from broker.factory import make_broker
from broker.sim import SimBroker


def test_factory_builds_sim():
    b = make_broker({"name": "sim", "symbol": "TMFR1"})
    assert isinstance(b, SimBroker)


def test_factory_builds_capital_skcom():
    b = make_broker({
        "name": "capital_skcom",
        "user_id": "u",
        "password": "p",
        "full_account": "F020000-123",
    })
    assert isinstance(b, CapitalSKCOMAdapter)


def test_unknown_broker_raises():
    try:
        make_broker({"name": "nope"})
    except ValueError:
        return
    raise AssertionError("expected ValueError")
