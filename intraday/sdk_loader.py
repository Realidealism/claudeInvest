"""Load and authenticate a single esun_marketdata SDK instance.

The SDK requires a config.ini (sections: Core / Cert / Api / User) plus an
interactive `sdk.login()` call that unlocks the .p12 certificate and account
password via keyring. We do this ONCE in the main thread before spawning the
worker threads, then pass the `sdk` object down.

Passing the authenticated SDK into the worker threads keeps authentication
out of the hot path and avoids re-prompting for credentials on each reconnect.
"""

from configparser import ConfigParser

from config.settings import ESUN_CONFIG_INI


def load_sdk():
    """Build and log in to an EsunMarketdata SDK.

    Returns the live SDK object. Raises if config.ini is missing or invalid.
    """
    if not ESUN_CONFIG_INI:
        raise RuntimeError(
            "ESUN_CONFIG_INI is not set. Point it at the .ini file that holds the "
            "[Core]/[Cert]/[Api]/[User] sections required by esun_marketdata."
        )

    # Import is deferred so the rest of the package still imports on machines
    # where esun_marketdata isn't installed yet.
    from esun_marketdata import EsunMarketdata  # type: ignore

    config = ConfigParser()
    read_files = config.read(ESUN_CONFIG_INI, encoding="utf-8")
    if not read_files:
        raise RuntimeError(f"Could not read ESUN_CONFIG_INI: {ESUN_CONFIG_INI}")

    sdk = EsunMarketdata(config)
    print(f"[SDK] logging in as {config['User'].get('Account')!r} ...")
    sdk.login()
    print("[SDK] login ok")
    return sdk
