"""Build a broker adapter from config -- the only place that knows broker names.

config.yaml example:

    broker:
      name: capital_skcom        # capital_skcom | yuanta_com | fubon_neo | sim
      user_id: ${BROKER_ID}
      password: ${BROKER_PWD}
      full_account: ${FUT_ACCOUNT}   # Capital/Yuanta futures: broker_code-account
      cert_id: ${CERT_ID}            # Capital signing cert, defaults to user_id

Selected broker: Capital SKCOM (existing account, large dev community, most
public examples -> friendly to Claude Code). Alternatives: yuanta_com (COM),
fubon_neo (native Python). Adding a broker means one more branch here only.
"""
from __future__ import annotations

from typing import Dict

from .base import BrokerAdapter
from .capital_skcom import CapitalSKCOMAdapter
from .fubon_neo import FubonNeoAdapter
from .sim import SimBroker
from .yuanta_com import YuantaComAdapter


def make_broker(cfg: Dict) -> BrokerAdapter:
    name = cfg["name"]

    if name == "capital_skcom":
        return CapitalSKCOMAdapter(
            user_id=cfg["user_id"],
            password=cfg["password"],
            full_account=cfg["full_account"],
            cert_id=cfg.get("cert_id"),
            skcom_dll_path=cfg.get("skcom_dll_path", "SKCOM.dll"),
            test_env=bool(cfg.get("test_env", False)),
        )

    if name == "yuanta_com":
        return YuantaComAdapter(
            user_id=cfg["user_id"],
            password=cfg["password"],
        )

    if name == "fubon_neo":
        return FubonNeoAdapter(
            user_id=cfg["user_id"],
            password=cfg["password"],
            cert_path=cfg["cert_path"],
            cert_pwd=cfg["cert_pwd"],
        )

    if name == "sim":
        return SimBroker(symbol=cfg.get("symbol", "TMFR1"))

    raise ValueError(f"Unknown broker: {name}")
