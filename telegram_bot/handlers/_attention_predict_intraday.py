"""Per-ticker intraday-aware attention prediction.

Reuses the rule helpers from `analysis/attention_predict.py` but synthesizes a
"today row" from `tw.intraday_quotes` (last_price, total_volume) so the same
threshold checks can be applied mid-session — answering "if the market closed
right now, would this stock trigger attention §X?"

The market-deviation gates of §2①/§2②/§4/§10 are SKIPPED here (per-ticker
queries can't afford to recompute market-wide averages on every /score). The
detail string carries a `(粗估)` suffix so callers know precision is reduced.
"""

from __future__ import annotations

from datetime import date

from analysis.attention_predict import (
    _calc_6d_avg_volume_ratio,
    _calc_6d_change_pct,
    _calc_nd_change_pct,
    _calc_volume_ratio,
)
from db.connection import get_cursor
from telegram_bot.handlers._data_freshness import DataState
from utils.classifier import classify_tw_security


# Need >= 90 trading days to compute §3 (90-day change). 60+ for §10 vol ratio.
_HISTORY_LOOKBACK = 90


def closest_untriggered_threshold(ticker: str, today: date) -> str | None:
    """Return one-line description of the nearest §X threshold not yet hit.

    Used when the stock is one attention away from disposal — caller wants
    to show the trader the price/volume level that would trip the next
    attention. Returns the SINGLE rule with the smallest relative gap.
    Skips rules that have already triggered (those become a different code
    path via `predict_today_attention`).
    """
    if classify_tw_security(ticker) != "STOCK":
        return None

    with get_cursor(commit=False) as cur:
        cur.execute("SELECT market FROM tw.stocks WHERE stock_id = %s", (ticker,))
        row = cur.fetchone()
        market = row["market"] if row else "TWSE"
        history = _load_history(cur, ticker, today)
        today_row, _src = _load_today_row(cur, ticker, today)
        outstanding = _load_outstanding_shares(cur, ticker)

    if today_row is None or not history:
        return None
    prices = history + [today_row]
    today_close = prices[-1]["close"]
    today_vol = prices[-1]["volume"]
    is_tpex = market == "TPEx"
    # Market-specific §2① threshold: 32 (TWSE) / 30 (TPEx)
    r2_1_thresh = 30 if is_tpex else 32

    candidates: list[tuple[float, str]] = []  # (gap_ratio, message)

    def _add(gap_ratio: float, msg: str):
        if gap_ratio is None or gap_ratio <= 0:
            return
        candidates.append((gap_ratio, msg))

    # §2① 6-day change > r2_1_thresh% (window includes today as day 1 → base
    # is close 5 trading days ago = prices[-6])
    if len(prices) >= 6:
        c6_ago = prices[-6]["close"]
        if c6_ago > 0:
            chg_now = (today_close / c6_ago - 1) * 100
            if abs(chg_now) <= r2_1_thresh:
                factor = r2_1_thresh / 100
                target = c6_ago * (1 + factor) if chg_now >= 0 else c6_ago * (1 - factor)
                gap = abs(target - today_close)
                _add(gap / today_close, f"離 §2① 還差 {gap:.2f} 元（{('漲到' if chg_now >= 0 else '跌到')} {target:.2f}）")

    # §3 30/60/90-day cumulative — market-specific thresholds:
    #   TWSE: cum 100/130/160% + 差幅 85/110/135%
    #   TPEx: cum 100/140/160% + 差幅 80/80/80%
    # N-day window includes today as day 1 → window = prices[-N:].
    if market == "TPEx":
        r3_thresholds = ((30, 100, 80), (60, 140, 80), (90, 160, 80))
        # 低價股 < 5元 thresholds bump
        if today_close < 5:
            r3_thresholds = ((30, 120, 80), (60, 180, 80), (90, 160, 80))
    else:
        r3_thresholds = ((30, 100, 85), (60, 130, 110), (90, 160, 135))
    today_ref = today_row.get("ref") if today_row else None
    for days, thresh, spread_thresh in r3_thresholds:
        if len(prices) < days:
            continue
        window = prices[-days:]
        cN_ago = window[0]["close"]
        if cN_ago <= 0:
            continue
        closes_win = [p["close"] for p in window]
        hi_win = max(closes_win)
        lo_win = min(closes_win)
        spread_pct = (hi_win / lo_win - 1) * 100 if lo_win > 0 else 0.0
        # If spread alone isn't met, §3 can't trigger regardless of today's
        # close → skip (don't surface as a near miss).
        if spread_pct < spread_thresh:
            continue
        chg_now = (today_close / cN_ago - 1) * 100
        if abs(chg_now) <= thresh:
            target_up = cN_ago * (1 + thresh / 100)
            target_dn = cN_ago * (1 - thresh / 100)
            # Apply directional ref_price gate to target: 漲方需 close > ref,
            # 跌方需 close < ref. Bump target to clear ref by ~0.01 if needed.
            if today_ref is not None:
                if chg_now >= 0:
                    target_up = max(target_up, today_ref + 0.01)
                else:
                    target_dn = min(target_dn, today_ref - 0.01)
            target = target_up if chg_now >= 0 else target_dn
            gap = abs(target - today_close)
            _add(gap / today_close, f"離 §3({days}日) 還差 {gap:.2f} 元（{('漲到' if chg_now >= 0 else '跌到')} {target:.2f}）")

    # §12 6-day 起迄價差 ≥ threshold AND today is the 6-day extreme.
    # 起迄價差 = |today - first_6d| (net change, NOT max-min range).
    # Threshold by market:
    #   TWSE: 100元 base + 25元/500元 above 500元
    #   TPEx:  70元 base + 15元/300元 above 300元
    if len(prices) >= 6:
        closes_6d = [p["close"] for p in prices[-6:]]
        first_6d = closes_6d[0]
        other_5 = closes_6d[:-1]
        high_5 = max(other_5)
        low_5 = min(other_5)
        if market == "TPEx":
            base, step, level = 70, 15, 300
        else:
            base, step, level = 100, 25, 500
        threshold = base
        if today_close >= level:
            threshold = base + int(today_close // level) * step
        up_target = max(first_6d + threshold, high_5)
        dn_target = min(first_6d - threshold, low_5)
        up_gap = up_target - today_close
        dn_gap = today_close - dn_target
        # If either direction already triggered, §12 is already met → skip
        if up_gap <= 0 or dn_gap <= 0:
            pass
        elif up_gap <= dn_gap:
            _add(up_gap / today_close, f"離 §12 還差 {up_gap:.2f} 元（漲到 {up_target:.2f} 創 6 日新高）")
        else:
            _add(dn_gap / today_close, f"離 §12 還差 {dn_gap:.2f} 元（跌到 {dn_target:.2f} 創 6 日新低）")

    # §4 volume gate: today_vol ≥ 5 × 60日均量 (when 6日 cum 也接近 25/27%)
    if len(prices) >= 60 and today_vol > 0:
        avg60_vol = sum(p["volume"] for p in prices[-60:]) / 60
        target_vol_5x = 5 * avg60_vol
        vol_gap = target_vol_5x - today_vol
        if vol_gap > 0:
            _add(
                vol_gap / today_vol,
                f"離 §4 量還差 {vol_gap/1000:.0f} 張（需達 {target_vol_5x/1000:.0f} 張 = 5× 60日均）",
            )

    # §5 turnover gate: today_turnover ≥ 10%/5% of outstanding
    if outstanding and outstanding > 0 and today_vol > 0:
        r5_tr_thresh = 5 if is_tpex else 10
        target_vol_tr = outstanding * r5_tr_thresh / 100
        vol_gap = target_vol_tr - today_vol
        if vol_gap > 0:
            _add(
                vol_gap / today_vol,
                f"離 §5 量還差 {vol_gap/1000:.0f} 張（需達 {target_vol_tr/1000:.0f} 張 = 週轉 {r5_tr_thresh}%）",
            )

    # §11 cum_turnover gate: 6日累積週轉 ≥ 50%/80%
    if outstanding and outstanding > 0 and len(prices) >= 6:
        r11_cum_thresh = 80 if is_tpex else 50
        cum_vol_6d_excl_today = sum(p["volume"] for p in prices[-6:-1])
        target_cum_vol = outstanding * r11_cum_thresh / 100
        target_today_vol = target_cum_vol - cum_vol_6d_excl_today
        if target_today_vol > today_vol and today_vol > 0:
            vol_gap = target_today_vol - today_vol
            _add(
                vol_gap / today_vol,
                f"離 §11 量還差 {vol_gap/1000:.0f} 張（需達 {target_today_vol/1000:.0f} 張 → 6日累積週轉 {r11_cum_thresh}%）",
            )

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def _suffix(rough: bool, state: DataState | None) -> str:
    """Build trailing parenthetical: '(粗估)' / '(盤中)' / '(前日)' combos."""
    parts: list[str] = []
    if rough:
        parts.append("粗估")
    if state == DataState.LIVE:
        parts.append("盤中")
    elif state == DataState.STALE_OVERNIGHT:
        parts.append("前日")
    return f" ({'，'.join(parts)})" if parts else ""


def _load_history(cur, ticker: str, today: date) -> list[dict]:
    cur.execute(
        """
        SELECT trade_date, close_price, volume,
               COALESCE(margin_balance, 0) AS margin_balance,
               COALESCE(short_balance, 0)  AS short_balance,
               COALESCE(sbl_sell, 0)       AS sbl_sell,
               COALESCE(dt_volume, 0)      AS dt_volume
        FROM tw.daily_prices
        WHERE stock_id = %s
          AND trade_date < %s
          AND close_price IS NOT NULL
        ORDER BY trade_date DESC LIMIT %s
        """,
        (ticker, today, _HISTORY_LOOKBACK),
    )
    rows = cur.fetchall()
    return [
        {
            "close": float(r["close_price"]),
            "volume": int(r["volume"] or 0),
            "margin_balance": int(r["margin_balance"]),
            "short_balance": int(r["short_balance"]),
            "sbl_sell": int(r["sbl_sell"]),
            "dt_volume": int(r["dt_volume"]),
        }
        for r in reversed(rows)
    ]


def _load_today_intraday(cur, ticker: str) -> dict | None:
    cur.execute(
        """
        SELECT last_price, total_volume, ref_price,
               COALESCE(margin_balance, 0) AS margin_balance,
               COALESCE(short_balance, 0)  AS short_balance
        FROM tw.intraday_quotes
        WHERE stock_id = %s
        """,
        (ticker,),
    )
    row = cur.fetchone()
    if not row or row["last_price"] is None:
        return None
    return {
        "close": float(row["last_price"]),
        "ref": float(row["ref_price"]) if row["ref_price"] is not None else None,
        "volume": int(row["total_volume"] or 0),
        "margin_balance": int(row["margin_balance"]),
        "short_balance": int(row["short_balance"]),
        # SBL borrow / day-trade volumes aren't settled until end of session
        "sbl_sell": 0,
        "dt_volume": 0,
    }


def _load_today_daily(cur, ticker: str, today: date) -> dict | None:
    """Today's row from daily_prices — only present after daily_update runs."""
    cur.execute(
        """
        SELECT close_price, ref_price, volume,
               COALESCE(margin_balance, 0) AS margin_balance,
               COALESCE(short_balance, 0)  AS short_balance,
               COALESCE(sbl_sell, 0)       AS sbl_sell,
               COALESCE(dt_volume, 0)      AS dt_volume
        FROM tw.daily_prices
        WHERE stock_id = %s AND trade_date = %s
        """,
        (ticker, today),
    )
    row = cur.fetchone()
    if not row or row["close_price"] is None:
        return None
    return {
        "close": float(row["close_price"]),
        "ref": float(row["ref_price"]) if row["ref_price"] is not None else None,
        "volume": int(row["volume"] or 0),
        "margin_balance": int(row["margin_balance"]),
        "short_balance": int(row["short_balance"]),
        "sbl_sell": int(row["sbl_sell"]),
        "dt_volume": int(row["dt_volume"]),
    }


def _load_today_row(cur, ticker: str, today: date) -> tuple[dict | None, str]:
    """Pick the best available today's row. Returns (row, source).
    Prefer daily_prices (definitive: SBL/dt-volume settled) over intraday_quotes."""
    row = _load_today_daily(cur, ticker, today)
    if row is not None:
        return row, "daily"
    return _load_today_intraday(cur, ticker), "intraday"


def consec_rule1_eligible_days(ticker: str, today: date) -> tuple[int, dict | None]:
    """Count consecutive recent trading days where §1 (6日累積漲跌異常) was
    met, walking back from the most recent day. Each day's eligibility uses
    market-specific thresholds (TWSE 32% / TPEx 30%) and our prices[-N]
    convention (window includes today as day 1, so base is prices[i-5]).

    Today's row uses the same source priority as predict_today_attention
    (daily_prices today if exists, else intraday_quotes synthetic).

    Returns (consec_count, today_row or None). today_row is None when no
    intraday/daily data is available yet for today.
    """
    if classify_tw_security(ticker) != "STOCK":
        return 0, None

    # Pick threshold based on listing market
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT market FROM tw.stocks WHERE stock_id = %s", (ticker,))
        row = cur.fetchone()
        market = row["market"] if row else "TWSE"
        history = _load_history(cur, ticker, today)
        today_row, _src = _load_today_row(cur, ticker, today)
    thresh = 30 if market == "TPEx" else 32

    if not history or len(history) < 5:
        return 0, today_row
    prices = history + ([today_row] if today_row is not None else [])

    # Walk back from the last available day, stop at first non-eligible.
    # «6日 cum» uses prices[i-5] (5 trading days back = window of 6 incl. day i).
    count = 0
    for i in range(len(prices) - 1, 4, -1):
        c_now = prices[i]["close"]
        c_base = prices[i - 5]["close"]
        if c_base <= 0:
            break
        chg = (c_now / c_base - 1) * 100
        if abs(chg) > thresh:
            count += 1
        else:
            break
    return count, today_row


def _load_outstanding_shares(cur, ticker: str) -> int | None:
    """Listed shares from shareholder_distribution table. Sum all 17 tiers,
    divided by 2 (the scraper records each share twice — registered +
    bearer perspective). Verified against attstock.tw for 8046 (646,165 張).
    Returns None when no data."""
    cur.execute(
        """
        SELECT (t1_shares+t2_shares+t3_shares+t4_shares+t5_shares+t6_shares
              + t7_shares+t8_shares+t9_shares+t10_shares+t11_shares+t12_shares
              + t13_shares+t14_shares+t15_shares+t16_shares+t17_shares) AS s
        FROM tw.shareholder_distribution
        WHERE stock_id = %s
        ORDER BY data_date DESC LIMIT 1
        """,
        (ticker,),
    )
    row = cur.fetchone()
    if not row or not row["s"]:
        return None
    return int(row["s"]) // 2


# Cache market-wide turnover averages — key: (today_iso, market)
# Recomputed only when first queried per market per day (slow first call,
# instant for subsequent stocks in same session).
_TURNOVER_AVG_CACHE: dict = {}


def _get_market_turnover_avg(today: date) -> dict[str, tuple[float, float]]:
    """Return {"TWSE": (today_tr%, cum6d_tr%), "TPEx": (...)} averaged across
    all common stocks with valid data. Used by §5/§11 差幅 gates."""
    key = today.isoformat()
    if key in _TURNOVER_AVG_CACHE:
        return _TURNOVER_AVG_CACHE[key]

    with get_cursor(commit=False) as cur:
        # All common stocks with their market
        cur.execute(
            "SELECT stock_id, market FROM tw.stocks "
            "WHERE security_type = 'STOCK' AND is_active = TRUE"
        )
        market_by_sid = {r["stock_id"]: r["market"] for r in cur.fetchall()}

        # Latest outstanding shares for each stock (one query)
        cur.execute(
            """
            SELECT DISTINCT ON (stock_id) stock_id,
                   (t1_shares+t2_shares+t3_shares+t4_shares+t5_shares+t6_shares
                  + t7_shares+t8_shares+t9_shares+t10_shares+t11_shares+t12_shares
                  + t13_shares+t14_shares+t15_shares+t16_shares+t17_shares) / 2 AS outstanding
            FROM tw.shareholder_distribution
            ORDER BY stock_id, data_date DESC
            """
        )
        outstanding_by_sid = {r["stock_id"]: r["outstanding"] for r in cur.fetchall()}

        # Today's intraday volume per stock
        cur.execute(
            "SELECT stock_id, total_volume FROM tw.intraday_quotes "
            "WHERE trade_date = %s",
            (today,),
        )
        today_vol_by_sid = {r["stock_id"]: r["total_volume"] or 0 for r in cur.fetchall()}

        # Last 5 days' daily volumes per stock (sum gives the 5-prior days
        # volume; with today's intraday we get 6-day cumulative)
        cur.execute(
            """
            SELECT stock_id, SUM(volume) AS vol5
            FROM (
                SELECT stock_id, volume,
                       ROW_NUMBER() OVER (PARTITION BY stock_id ORDER BY trade_date DESC) rn
                FROM tw.daily_prices
                WHERE trade_date < %s AND volume IS NOT NULL
            ) t WHERE rn <= 5
            GROUP BY stock_id
            """,
            (today,),
        )
        vol5_by_sid = {r["stock_id"]: r["vol5"] or 0 for r in cur.fetchall()}

    # Per stock: today_turnover %, 6day_cum_turnover %
    by_market: dict[str, list] = {"TWSE": [], "TPEx": []}
    for sid, market in market_by_sid.items():
        out = outstanding_by_sid.get(sid)
        if not out or out <= 0:
            continue
        tv = float(today_vol_by_sid.get(sid, 0) or 0)
        v5 = float(vol5_by_sid.get(sid, 0) or 0)
        out_f = float(out)
        today_tr = tv / out_f * 100
        cum6_tr = (tv + v5) / out_f * 100
        if market in by_market:
            by_market[market].append((today_tr, cum6_tr))

    result = {}
    for mkt, vals in by_market.items():
        if vals:
            result[mkt] = (
                float(sum(v[0] for v in vals) / len(vals)),
                float(sum(v[1] for v in vals) / len(vals)),
            )
        else:
            result[mkt] = (0.0, 0.0)
    _TURNOVER_AVG_CACHE[key] = result
    return result


def predict_today_attention(
    ticker: str, today: date, state: DataState | None = None
) -> list[tuple[str, str]]:
    """Return [(rule_code, short_detail), ...] for rules predicted to trigger.

    `state` lets callers tag the detail suffix (粗估/盤中/前日). Empty list
    when: not a common STOCK, no intraday quote, insufficient history, or
    no rule fires at current data.
    """
    if classify_tw_security(ticker) != "STOCK":
        return []

    with get_cursor(commit=False) as cur:
        cur.execute("SELECT market FROM tw.stocks WHERE stock_id = %s", (ticker,))
        row = cur.fetchone()
        market = row["market"] if row else "TWSE"
        history = _load_history(cur, ticker, today)
        today_row, _src = _load_today_row(cur, ticker, today)
        outstanding = _load_outstanding_shares(cur, ticker)

    if today_row is None or not history:
        return []

    prices = history + [today_row]
    out: list[tuple[str, str]] = []

    rough = _suffix(True, state)   # rules with market gate skipped
    clean = _suffix(False, state)  # rules with absolute thresholds only

    # Market-specific thresholds
    is_tpex = market == "TPEx"
    r2_1_thresh = 30 if is_tpex else 32
    r2_2_thresh = 23 if is_tpex else 25
    r2_2_diff_thresh = 40 if is_tpex else 50
    r4_thresh = 27 if is_tpex else 25
    today_close = prices[-1]["close"]
    today_ref = today_row.get("ref")
    if is_tpex and today_close < 5:
        r3_thresholds = ((30, 120), (60, 180), (90, 160))
    elif is_tpex:
        r3_thresholds = ((30, 100), (60, 140), (90, 160))
    else:
        r3_thresholds = ((30, 100), (60, 130), (90, 160))

    def _ref_gate_ok(chg: float) -> bool:
        """§3 needs close > ref for 漲方, close < ref for 跌方."""
        if today_ref is None:
            return True
        return (chg > 0 and today_close > today_ref) or (chg < 0 and today_close < today_ref)

    # §2① 6-day change > X% (uses prices[-6] window-includes-today base)
    chg6 = _calc_6d_change_pct(prices)
    if chg6 is not None and abs(chg6) > r2_1_thresh:
        direction = "漲" if chg6 > 0 else "跌"
        out.append(("§2①", f"6日{direction} {abs(chg6):.1f}%{rough}"))
    # §2② 6-day > X% + 價差 ≥ Y元
    elif chg6 is not None and abs(chg6) > r2_2_thresh and len(prices) >= 6:
        diff = abs(prices[-1]["close"] - prices[-6]["close"])
        if diff >= r2_2_diff_thresh:
            direction = "漲" if chg6 > 0 else "跌"
            out.append(
                ("§2②", f"6日{direction} {abs(chg6):.1f}% 價差 {diff:.0f}元{rough}")
            )

    # §3 30/60/90 day cumulative + ref_price directional gate
    for days, thresh in r3_thresholds:
        chg = _calc_nd_change_pct(prices, days)
        if chg is not None and abs(chg) > thresh and _ref_gate_ok(chg):
            direction = "漲" if chg > 0 else "跌"
            out.append(
                (f"§3({days}日)", f"{days}日{direction} {abs(chg):.1f}%{clean}")
            )

    # §4 6-day change > X% + today's volume ≥ 5× 60-day avg
    today_vr = _calc_volume_ratio(prices)
    if chg6 is not None and abs(chg6) > r4_thresh and today_vr is not None and today_vr >= 5:
        direction = "漲" if chg6 > 0 else "跌"
        out.append(
            ("§4", f"6日{direction} {abs(chg6):.1f}% + 今日量 {today_vr:.1f}× 60日均{rough}")
        )

    # §10 6-day avg volume / 60-day avg ≥ 5x
    vol_ratio = _calc_6d_avg_volume_ratio(prices)
    if vol_ratio is not None and vol_ratio >= 5:
        out.append(("§10", f"6日均量 {vol_ratio:.1f}× 60日均{rough}"))

    # §12 6-day 起迄價差 (net change) — market-specific:
    #   TWSE: ≥100元 base, +25元 per 500元
    #   TPEx: ≥70元 base, +15元 per 300元
    if len(prices) >= 6:
        closes_6d = [p["close"] for p in prices[-6:]]
        first_6d = closes_6d[0]
        high_6d = max(closes_6d)
        low_6d = min(closes_6d)
        diff = abs(today_close - first_6d)
        if is_tpex:
            base, step, level = 70, 15, 300
        else:
            base, step, level = 100, 25, 500
        threshold = base
        if today_close >= level:
            threshold = base + int(today_close // level) * step
        if diff >= threshold:
            is_high = today_close >= high_6d
            is_low = today_close <= low_6d
            if is_high or is_low:
                marker = "新高" if is_high else "新低"
                out.append(
                    ("§12", f"6日起迄價差 {diff:.0f}元（門檻 {threshold}）+ 收{marker}{clean}")
                )

    # §8 6-day change > 25% + 券資比 ≥ 20% + 券資比 ≥ 6日最低 × 4
    if chg6 is not None and abs(chg6) > 25 and len(prices) >= 7:
        prev = prices[-2]
        margin = prev["margin_balance"]
        if margin > 0:
            short = prev["short_balance"]
            short_margin_ratio = short / margin * 100
            ratios_6d = []
            for p in prices[-7:-1]:
                if p["margin_balance"] > 0:
                    ratios_6d.append(p["short_balance"] / p["margin_balance"] * 100)
            if short_margin_ratio >= 20 and ratios_6d:
                min_ratio = min(ratios_6d)
                if min_ratio == 0 or short_margin_ratio >= min_ratio * 4:
                    direction = "漲" if chg6 > 0 else "跌"
                    out.append(
                        ("§8", f"6日{direction} {abs(chg6):.1f}% + 券資比 {short_margin_ratio:.1f}%{rough}")
                    )

    # §13 6-day SBL borrow ratio ≥ 12% + prev day SBL ≥ 5× 60-day avg
    # NB: today's sbl_sell is unknown intraday (synthesized as 0); 6-day sum
    # uses prior 6 sessions (prices[-7:-1]) for predictive accuracy.
    if len(prices) >= 62:
        total_sbl_6d = sum(p["sbl_sell"] for p in prices[-7:-1])
        total_vol_6d = sum(p["volume"] for p in prices[-7:-1])
        if total_vol_6d > 0:
            sbl_ratio = total_sbl_6d / total_vol_6d * 100
            prev_sbl = prices[-2]["sbl_sell"]
            avg60_sbl = sum(p["sbl_sell"] for p in prices[-62:-2]) / 60
            if avg60_sbl > 0:
                sbl_mult = prev_sbl / avg60_sbl
                if sbl_ratio >= 12 and sbl_mult >= 5:
                    out.append(
                        ("§13", f"6日借券占比 {sbl_ratio:.1f}% + 前日 {sbl_mult:.1f}× 60日均{clean}")
                    )

    # §14 6-day day-trade ratio > 60% + prev day > 60%
    # Same intraday caveat as §13 — use prior 6 sessions.
    if len(prices) >= 7:
        total_dt_6d = sum(p["dt_volume"] for p in prices[-7:-1])
        total_vol_6d = sum(p["volume"] for p in prices[-7:-1])
        if total_vol_6d > 0:
            dt_ratio_6d = total_dt_6d / total_vol_6d * 100
            prev = prices[-2]
            if prev["volume"] > 0:
                dt_ratio_prev = prev["dt_volume"] / prev["volume"] * 100
                if dt_ratio_6d > 60 and dt_ratio_prev > 60:
                    out.append(
                        ("§14", f"6日當沖占比 {dt_ratio_6d:.1f}% + 前日 {dt_ratio_prev:.1f}%{clean}")
                    )

    # §5 / §11 share market-wide turnover gate; load lazily
    mkt_avg_today_tr = mkt_avg_cum_tr = None
    if outstanding and outstanding > 0:
        try:
            mkt_avgs = _get_market_turnover_avg(today)
            mkt_avg_today_tr, mkt_avg_cum_tr = mkt_avgs.get(market, (0.0, 0.0))
        except Exception:
            mkt_avg_today_tr = mkt_avg_cum_tr = None

    # §5 6-day change > X% + 當日週轉率 ≥ Y% + 全體 差幅 ≥ Z%
    #   TWSE: cum > 25%, turnover ≥ 10%, 差幅 ≥ 5%
    #   TPEx: cum > 27%, turnover > 5%, 差幅 ≥ 3%
    r5_cum_thresh = 27 if is_tpex else 25
    r5_tr_thresh = 5 if is_tpex else 10
    r5_diff_thresh = 3 if is_tpex else 5
    if outstanding and outstanding > 0 and chg6 is not None and abs(chg6) > r5_cum_thresh:
        today_vol = prices[-1]["volume"]
        turnover_pct = today_vol / outstanding * 100
        diff_ok = (
            mkt_avg_today_tr is None
            or turnover_pct - mkt_avg_today_tr >= r5_diff_thresh
        )
        if turnover_pct >= r5_tr_thresh and diff_ok:
            direction = "漲" if chg6 > 0 else "跌"
            out.append(
                ("§5", f"6日{direction} {abs(chg6):.1f}% + 今日週轉率 {turnover_pct:.1f}%{rough}")
            )

    # §11 6-day cumulative 週轉率 + 當日週轉率 + 兩個全體 差幅
    #   TWSE: 累積 > 50% 差幅 ≥ 40%, 當日 ≥ 10% 差幅 ≥ 5%
    #   TPEx: 累積 > 80% 差幅 ≥ 50%, 當日 > 5% 差幅 ≥ 3%
    r11_cum_thresh = 80 if is_tpex else 50
    r11_cum_diff_thresh = 50 if is_tpex else 40
    r11_tr_thresh = 5 if is_tpex else 10
    r11_tr_diff_thresh = 3 if is_tpex else 5
    if outstanding and outstanding > 0 and len(prices) >= 6:
        sum_vol_6d = sum(p["volume"] for p in prices[-6:])
        cum_tr_6d = sum_vol_6d / outstanding * 100
        today_tr = prices[-1]["volume"] / outstanding * 100
        cum_diff_ok = (
            mkt_avg_cum_tr is None
            or cum_tr_6d - mkt_avg_cum_tr >= r11_cum_diff_thresh
        )
        tr_diff_ok = (
            mkt_avg_today_tr is None
            or today_tr - mkt_avg_today_tr >= r11_tr_diff_thresh
        )
        if (
            cum_tr_6d >= r11_cum_thresh
            and today_tr >= r11_tr_thresh
            and cum_diff_ok
            and tr_diff_ok
        ):
            out.append(("§11", f"6日累積週轉率 {cum_tr_6d:.1f}% + 今日 {today_tr:.1f}%{rough}"))

    return out
