import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "invest"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# E.Sun / esun_marketdata SDK — path to the .ini config file that holds the
# [Core] / [Cert] / [Api] / [User] sections the SDK's EsunMarketdata() needs.
# The .p12 cert password and account password are stored in the OS keyring
# after the first interactive login (see intraday/sdk_loader.py).
ESUN_CONFIG_INI = os.getenv("ESUN_CONFIG_INI", "")

# SinoPac / Shioaji — REST + market data only, no CA needed because we never
# place orders. Used by intraday/sinopac_loader.py.
SINOPAC_API_KEY = os.getenv("SINOPAC_API_KEY", "")
SINOPAC_SECRET_KEY = os.getenv("SINOPAC_SECRET_KEY", "")