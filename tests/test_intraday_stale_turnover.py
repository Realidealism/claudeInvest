"""Around the open E.Sun can still serve a stock's previous-session snapshot
until it first trades, carrying yesterday's whole-day turnover. Summed into
the 09:05 bucket on 14 of 104 days (2026-04-24..10-22) it pushed h(09:05) to
~29% against a true ~14%, and per-stock 09:05 projections read it too."""
from datetime import datetime, timedelta, timezone

from intraday import sweeper

TPE = timezone(timedelta(hours=8))
OPEN = datetime(2026, 10, 22, 9, 1, tzinfo=TPE)
STALE = datetime(2026, 10, 21, 13, 30, tzinfo=TPE)


def _q(sid, at, value, volume=1000):
    return {"stock_id": sid, "last_trade_at": at, "total_value": value, "total_volume": volume}


def test_stale_quotes_lose_their_turnover_and_fresh_ones_keep_it():
    recs = [_q("1101", STALE, 9_000_000), _q("2330", OPEN, 5_000), _q("9999", None, 7_000)]
    assert sweeper._zero_stale_turnover(recs, OPEN.date()) == 1
    assert [(r["total_value"], r["total_volume"]) for r in recs] == [(0, 0), (5_000, 1000), (7_000, 1000)]


def test_utc_stamps_are_judged_on_the_taipei_date():
    # 2026-10-22 01:00 UTC is 09:00 TPE on the 22nd: a trade from today.
    recs = [_q("2330", datetime(2026, 10, 22, 1, 0, tzinfo=timezone.utc), 5_000)]
    assert sweeper._zero_stale_turnover(recs, OPEN.date()) == 0
    assert recs[0]["total_value"] == 5_000


class _OneTick:
    def is_set(self):
        return False

    def wait(self, _seconds):
        return True


def test_the_first_bucket_sums_only_turnover_traded_today(monkeypatch):
    tse = [_q(f"{1000 + i}", OPEN, 100) for i in range(25)] + [_q("1101", STALE, 9_000_000)]
    otc = [_q("6488", STALE, 4_000_000)]
    written, profile = {}, []
    monkeypatch.setattr(sweeper, "_now_tpe", lambda: OPEN)
    monkeypatch.setattr(sweeper.esun_rest, "fetch_snapshot_quotes",
                        lambda sdk, m: tse if m == "TSE" else otc)
    monkeypatch.setattr(sweeper.esun_rest, "fetch_index_quote", lambda sdk, s: None)
    monkeypatch.setattr(sweeper.store, "market_closure_source", lambda d: None)
    monkeypatch.setattr(sweeper.store, "upsert_quotes",
                        lambda recs, market, trade_date: written.update(
                            {r["stock_id"]: r["total_value"] for r in recs}) or len(recs))
    monkeypatch.setattr(sweeper.store, "upsert_value_profile",
                        lambda d, bucket, total, snap=None: profile.append((bucket, total)))
    sweeper.run(_OneTick(), sdk=object())
    assert profile == [("09:05", 25 * 100)]
    assert written["1101"] == 0 and written["6488"] == 0 and written["1000"] == 100
