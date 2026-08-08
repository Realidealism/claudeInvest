"""
Attention/disposal stock prediction (注意/處置股票預判).

Predicts which stocks may trigger TWSE/TPEx attention criteria based on
current price, volume, margin, SBL, and day-trading data.

Implemented criteria (from TWSE 公布或通知注意交易資訊暨處置作業要點):
  §2①  6-day cumulative price change >32%
  §2②  6-day cumulative price change >25% + price diff ≥50
  §3    30/60/90-day price change >100/130/160%
  §4    6-day change >25% + volume ≥5x 60-day avg
  §8    6-day change >25% + margin short ratio spike
  §10   6-day avg volume ≥5x 60-day avg
  §12   6-day price diff ≥100 (scaled for high-price stocks; from
        2026-08-10 TWSE only bites above 1,000元 with ≥300 bands)
  §13   6-day SBL ratio + SBL multiple vs 60-day avg (TWSE 12%/5x,
        TPEx 9%/4x)
  §14   6-day day-trade ratio >60% + prev day >60%

Disposal prediction:
  Consecutive attention days ≥2 → likely disposal next day.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, timedelta

from db.connection import get_cursor


@dataclass
class Alert:
    stock_id: str
    name: str
    market: str
    rule: str
    detail: str


@dataclass
class DisposalRisk:
    stock_id: str
    name: str
    market: str
    consecutive_days: int
    recent_dates: list[date]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_prices(trade_date: date, lookback: int = 95) -> dict:
    """Load recent daily prices for all active stocks.
    Returns {stock_id: [(trade_date, close, volume, ...), ...]} sorted by date asc.
    """
    start = trade_date - timedelta(days=int(lookback * 1.6))
    with get_cursor(commit=False) as cur:
        # Load stock metadata first (small table)
        cur.execute("""
            SELECT stock_id, name, market, industry, security_type
            FROM tw.stocks WHERE is_active = TRUE
        """)
        stock_rows = cur.fetchall()

        # Load prices separately (large table, indexed)
        cur.execute("""
            SELECT stock_id, trade_date, close_price, ref_price, volume,
                   COALESCE(dt_volume, 0) AS dt_volume,
                   COALESCE(margin_balance, 0) AS margin_balance,
                   COALESCE(short_balance, 0) AS short_balance,
                   COALESCE(sbl_sell, 0) AS sbl_sell
            FROM tw.daily_prices
            WHERE trade_date >= %s AND trade_date <= %s
              AND close_price IS NOT NULL
            ORDER BY stock_id, trade_date
        """, (start, trade_date))
        rows = cur.fetchall()

    # Build metadata lookup
    meta = {}
    active_ids = set()
    for r in stock_rows:
        sid = r["stock_id"]
        active_ids.add(sid)
        meta[sid] = {
            "name": r["name"],
            "market": r["market"],
            "industry": r["industry"],
            "security_type": r["security_type"],
        }

    # TTM EPS for PE-exemption check (PE<0 or ≥60倍 TWSE / ≥65倍 TPEx
    # → 同類差幅 gate 豁免)
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT stock_id, SUM(eps) AS ttm_eps
            FROM (
                SELECT stock_id, eps,
                       ROW_NUMBER() OVER (PARTITION BY stock_id ORDER BY year DESC, quarter DESC) rn
                FROM tw.income_statements WHERE eps IS NOT NULL
            ) t WHERE rn <= 4
            GROUP BY stock_id
            HAVING COUNT(*) = 4
            """
        )
        for r in cur.fetchall():
            sid = r["stock_id"]
            if sid in meta:
                meta[sid]["ttm_eps"] = float(r["ttm_eps"])

    # Build price series
    stocks = {}
    for r in rows:
        sid = r["stock_id"]
        if sid not in active_ids:
            continue
        if sid not in stocks:
            stocks[sid] = []
        stocks[sid].append({
            "date": r["trade_date"],
            "close": float(r["close_price"]),
            "ref": float(r["ref_price"]) if r["ref_price"] is not None else None,
            "volume": r["volume"] or 0,
            "dt_volume": r["dt_volume"],
            "margin_balance": r["margin_balance"],
            "short_balance": r["short_balance"],
            "sbl_sell": r["sbl_sell"],
        })

    return stocks, meta


# 注意/處置 作業要點 applies to 上市 (TWSE) and 上櫃 (TPEx) only.
ALERT_MARKETS = ("TWSE", "TPEx")


def _is_common_stock(sec_type: str | None) -> bool:
    return sec_type in (None, "STOCK")


# ---------------------------------------------------------------------------
# Market-wide averages
# ---------------------------------------------------------------------------

def _calc_6d_change_pct(prices: list[dict]) -> float | None:
    """6-day cumulative price change % — ex-rights / ex-div adjusted via
    ref_price compounding. Each day's trading-induced return is
    (close / ref - 1); compound over the 5 intervals of the 6-day window.
    Per TWSE/TPEx rule: 因非交易之原因（除權除息）造成價格變動者排除."""
    return _calc_nd_change_pct(prices, 6)


def _calc_nd_change_pct(prices: list[dict], n: int) -> float | None:
    """N-day cumulative price change % adjusted for ex-rights / ex-div.
    Compounds n-1 daily trading-induced returns (close[i] / ref[i] - 1).
    Falls back to raw close-to-close ratio when any ref_price is missing."""
    if len(prices) < n:
        return None
    window = prices[-(n - 1):]
    cum = 1.0
    for p in window:
        ref = p.get("ref")
        close = p["close"]
        if ref is None or ref <= 0 or close <= 0:
            # Fallback: raw close-to-close (no ex-div adjustment)
            base = prices[-n]["close"]
            if base <= 0:
                return None
            return (prices[-1]["close"] / base - 1) * 100
        cum *= close / ref
    return (cum - 1) * 100


def _calc_volume_ratio(prices: list[dict]) -> float | None:
    """Today's volume / 60-day average volume — 60-day window includes today
    as day 1 (today + 59 prior), matching TWSE「最近60個營業日」convention."""
    if len(prices) < 60:
        return None
    avg60 = sum(p["volume"] for p in prices[-60:]) / 60
    if avg60 == 0:
        return None
    return prices[-1]["volume"] / avg60


def _calc_6d_avg_volume_ratio(prices: list[dict]) -> float | None:
    """6-day average volume / 60-day average volume."""
    if len(prices) < 66:
        return None
    avg6 = sum(p["volume"] for p in prices[-6:]) / 6
    avg60 = sum(p["volume"] for p in prices[-66:-6]) / 60
    if avg60 == 0:
        return None
    return avg6 / avg60


def _compute_market_averages(stocks: dict, meta: dict) -> dict:
    """Compute market-wide and industry-level averages for comparison.
    全體均值 is segregated by listing market (TWSE vs TPEx) per attstock.tw
    behavior — a stock is compared only against peers on the same exchange."""
    changes_by_market: dict[str, list] = {"TWSE": [], "TPEx": []}
    all_vol_ratios = []
    industry_changes: dict[str, list] = {}

    for sid, prices in stocks.items():
        if not _is_common_stock(meta[sid].get("security_type")):
            continue

        chg = _calc_6d_change_pct(prices)
        if chg is not None:
            mkt = meta[sid].get("market")
            if mkt in changes_by_market:
                changes_by_market[mkt].append(chg)
            ind = meta[sid].get("industry") or "unknown"
            industry_changes.setdefault(ind, []).append(chg)

        vr = _calc_volume_ratio(prices)
        if vr is not None:
            all_vol_ratios.append(vr)

    def _mean(lst):
        return sum(lst) / len(lst) if lst else 0

    market_avg_vol_ratio = _mean(all_vol_ratios)

    ind_avg_change = {}
    for ind, vals in industry_changes.items():
        ind_avg_change[ind] = _mean(vals)

    return {
        "change_6d_twse": _mean(changes_by_market["TWSE"]),
        "change_6d_tpex": _mean(changes_by_market["TPEx"]),
        # Backwards-compat: combined avg (still emitted but no rule uses it).
        "change_6d": _mean(changes_by_market["TWSE"] + changes_by_market["TPEx"]),
        "vol_ratio": market_avg_vol_ratio,
        "industry_change_6d": ind_avg_change,
        "industry_count": {ind: len(vals) for ind, vals in industry_changes.items()},
    }


def _market_change_6d(mkt: dict, meta: dict) -> float:
    """Pick the right 全體均值 6日 for a stock based on its listing market."""
    if meta.get("market") == "TPEx":
        return mkt.get("change_6d_tpex", 0)
    return mkt.get("change_6d_twse", 0)


# ---------------------------------------------------------------------------
# Rule checks
# ---------------------------------------------------------------------------

# Market-specific thresholds per TWSE FL007226 (上市) vs TPEx 規則 (上櫃).
# TWSE: §2① 32% / §2② 25% + 50元
# TPEx: 第一款一 30% / 第一款二 23% + 40元
_R2_1_PCT = {"TWSE": 32, "TPEx": 30}
_R2_2_PCT = {"TWSE": 25, "TPEx": 23}
_R2_2_DIFF = {"TWSE": 50, "TPEx": 40}


def _r2_thresh(meta: dict, table: dict, default) -> float:
    return table.get(meta.get("market"), default)


def _is_industry_exempt(meta: dict, mkt: dict, close: float) -> bool:
    """Industry 差幅 gate is exempt when:
      - 同類有價證券 < 5 家 (insufficient peers), OR
      - 本益比 PE < 0 (negative TTM earnings) or PE ≥ 60倍 TWSE / 65倍 TPEx."""
    ind = meta.get("industry") or "unknown"
    if mkt.get("industry_count", {}).get(ind, 0) < 5:
        return True
    ttm_eps = meta.get("ttm_eps")
    if ttm_eps is None:
        return False
    if ttm_eps <= 0:
        return True
    if close <= 0:
        return False
    pe = close / ttm_eps
    pe_thresh = 65 if meta.get("market") == "TPEx" else 60
    return pe >= pe_thresh


def _check_rule_2_1(prices: list, meta: dict, mkt: dict) -> Alert | None:
    """§2① 6日累積漲跌幅 >X% AND 全體差幅 ≥20% AND 同類差幅 ≥20%.
    X = 32 (TWSE) / 30 (TPEx). <5元 個股 / PE 異常 / 同類 <5家 → 同類豁免."""
    if not prices or prices[-1]["close"] < 5:
        return None
    chg = _calc_6d_change_pct(prices)
    thresh = _r2_thresh(meta, _R2_1_PCT, 32)
    if chg is None or abs(chg) <= thresh:
        return None
    close = prices[-1]["close"]
    ind = meta.get("industry") or "unknown"
    ind_avg = mkt["industry_change_6d"].get(ind, 0)
    mkt_avg = _market_change_6d(mkt, meta)
    industry_exempt = _is_industry_exempt(meta, mkt, close)
    if abs(chg - mkt_avg) < 20:
        return None
    if not industry_exempt and abs(chg - ind_avg) < 20:
        return None
    if meta.get("security_type") not in (None, "STOCK"):
        return None
    direction = "漲" if chg > 0 else "跌"
    detail_ind = "同類豁免" if industry_exempt else f"同業均 {ind_avg:.1f}%"
    return Alert(
        stock_id=meta["stock_id"], name=meta["name"], market=meta["market"],
        rule="§2①",
        detail=f"6日累積{direction}幅 {abs(chg):.1f}% (市場均 {mkt_avg:.1f}%, {detail_ind})",
    )


def _check_rule_2_2(prices: list, meta: dict, mkt: dict) -> Alert | None:
    """§2② 6日累積漲跌 >X% AND 價差 ≥Y元 AND 全體/同類差幅 ≥20%.
    X/Y = 25/50 (TWSE) / 23/40 (TPEx). <5元 個股不適用; 同類可豁免."""
    if not prices or prices[-1]["close"] < 5:
        return None
    chg = _calc_6d_change_pct(prices)
    pct_thresh = _r2_thresh(meta, _R2_2_PCT, 25)
    if chg is None or abs(chg) <= pct_thresh:
        return None
    if len(prices) < 6:
        return None
    diff = abs(prices[-1]["close"] - prices[-6]["close"])
    diff_thresh = _r2_thresh(meta, _R2_2_DIFF, 50)
    if diff < diff_thresh:
        return None
    close = prices[-1]["close"]
    ind = meta.get("industry") or "unknown"
    ind_avg = mkt["industry_change_6d"].get(ind, 0)
    mkt_avg = _market_change_6d(mkt, meta)
    industry_exempt = _is_industry_exempt(meta, mkt, close)
    if abs(chg - mkt_avg) < 20:
        return None
    if not industry_exempt and abs(chg - ind_avg) < 20:
        return None
    direction = "漲" if chg > 0 else "跌"
    return Alert(
        stock_id=meta["stock_id"], name=meta["name"], market=meta["market"],
        rule="§2②",
        detail=f"6日累積{direction}幅 {abs(chg):.1f}%, 價差 {diff:.0f}元",
    )


_R3_THRESHOLDS = {
    # market → [(window_days, cum_thresh%, spread_thresh%)]
    "TWSE": [(30, 100, 85), (60, 130, 110), (90, 160, 135)],
    "TPEx": [(30, 100, 80), (60, 140, 80), (90, 160, 80)],
}
_R3_LOW_PRICE_TPEX = {30: 120, 60: 180, 90: 160}  # < 5元 個股 threshold bump (TPEx)


def _check_rule_3(prices: list, meta: dict, mkt: dict) -> list[Alert]:
    """§3 30/60/90日起迄漲跌 — market-specific thresholds.
    TWSE: 100/130/160% AND 差幅 85/110/135%
    TPEx: 100/140/160% AND 差幅 80/80/80%
    TPEx <5元 stocks: 30/60/90日 → 120/180/160% (90 unchanged)
    Window includes today as day 1 — N-day window = prices[-N:]."""
    alerts = []
    if not prices:
        return alerts
    market = meta.get("market", "TWSE")
    thresholds = _R3_THRESHOLDS.get(market, _R3_THRESHOLDS["TWSE"])
    today_close = prices[-1]["close"]
    today_ref = prices[-1].get("ref")
    is_low_price = today_close < 5
    for days, pct_thresh, diff_thresh in thresholds:
        if len(prices) < days:
            continue
        # 低價股 TPEx 提高 threshold
        if is_low_price and market == "TPEx":
            pct_thresh = _R3_LOW_PRICE_TPEX.get(days, pct_thresh)
        chg = _calc_nd_change_pct(prices, days)
        if chg is None or abs(chg) <= pct_thresh:
            continue
        # Directional ref_price gate: 漲方需 close > ref, 跌方需 close < ref
        if today_ref is not None:
            if chg > 0 and today_close <= today_ref:
                continue
            if chg < 0 and today_close >= today_ref:
                continue
        win = prices[-days:]
        closes_win = [p["close"] for p in win]
        hi_win = max(closes_win)
        lo_win = min(closes_win)
        spread_pct = (hi_win / lo_win - 1) * 100 if lo_win > 0 else 0.0
        if spread_pct < diff_thresh:
            continue
        direction = "漲" if chg > 0 else "跌"
        alerts.append(Alert(
            stock_id=meta["stock_id"], name=meta["name"], market=meta["market"],
            rule=f"§3({days}日)",
            detail=f"{days}日累積{direction}幅 {abs(chg):.1f}% (門檻 {pct_thresh}%)",
        ))
    return alerts


_R4_PCT = {"TWSE": 25, "TPEx": 27}


def _check_rule_4(prices: list, meta: dict, mkt: dict) -> Alert | None:
    """§4 6日漲跌 > X% + 量 ≥ 5倍60日均 + 倍數差 ≥ 4
    X = 25 (TWSE) / 27 (TPEx). PE 異常 / 同類<5家 → 同類差幅 豁免."""
    if not prices:
        return None
    chg = _calc_6d_change_pct(prices)
    pct_thresh = _R4_PCT.get(meta.get("market"), 25)
    if chg is None or abs(chg) <= pct_thresh:
        return None
    vr = _calc_volume_ratio(prices)
    if vr is None or vr < 5:
        return None
    mkt_vr = mkt["vol_ratio"]
    if abs(vr - mkt_vr) < 4:
        return None
    close = prices[-1]["close"]
    ind = meta.get("industry") or "unknown"
    ind_avg = mkt["industry_change_6d"].get(ind, 0)
    mkt_avg = _market_change_6d(mkt, meta)
    industry_exempt = _is_industry_exempt(meta, mkt, close)
    if abs(chg - mkt_avg) < 20:
        return None
    if not industry_exempt and abs(chg - ind_avg) < 20:
        return None
    direction = "漲" if chg > 0 else "跌"
    return Alert(
        stock_id=meta["stock_id"], name=meta["name"], market=meta["market"],
        rule="§4",
        detail=f"6日{direction}幅 {abs(chg):.1f}% + 量能 {vr:.1f}倍 (市場均 {mkt_vr:.1f}倍)",
    )


def _check_rule_8(prices: list, meta: dict, mkt: dict) -> Alert | None:
    """§8 6日漲跌>25% + 券資比≥20% + 融資使用率≥25% + 融券使用率≥15% + 券資比≥最近6日最低×4"""
    chg = _calc_6d_change_pct(prices)
    if chg is None or abs(chg) <= 25:
        return None
    if len(prices) < 7:
        return None
    ind = meta.get("industry") or "unknown"
    ind_avg = mkt["industry_change_6d"].get(ind, 0)
    mkt_avg = _market_change_6d(mkt, meta)
    if abs(chg - mkt_avg) < 20 or abs(chg - ind_avg) < 20:
        return None

    prev = prices[-2]
    margin = prev["margin_balance"]
    short = prev["short_balance"]
    if margin == 0:
        return None
    short_margin_ratio = short / margin * 100
    if short_margin_ratio < 20:
        return None

    ratios_6d = []
    for p in prices[-7:-1]:
        if p["margin_balance"] > 0:
            ratios_6d.append(p["short_balance"] / p["margin_balance"] * 100)
    if not ratios_6d:
        return None
    min_ratio = min(ratios_6d)
    if min_ratio > 0 and short_margin_ratio < min_ratio * 4:
        return None

    direction = "漲" if chg > 0 else "跌"
    return Alert(
        stock_id=meta["stock_id"], name=meta["name"], market=meta["market"],
        rule="§8",
        detail=f"6日{direction}幅 {abs(chg):.1f}% + 券資比 {short_margin_ratio:.1f}% (6日最低 {min_ratio:.1f}%)",
    )


def _check_rule_10(prices: list, meta: dict, mkt: dict) -> Alert | None:
    """§10 6日均量≥5倍60日均 + 當日≥5倍60日均, 倍數差≥4"""
    avg6r = _calc_6d_avg_volume_ratio(prices)
    if avg6r is None or avg6r < 5:
        return None
    vr = _calc_volume_ratio(prices)
    if vr is None or vr < 5:
        return None
    mkt_vr = mkt["vol_ratio"]
    if abs(vr - mkt_vr) < 4 or abs(avg6r - mkt_vr) < 4:
        return None
    return Alert(
        stock_id=meta["stock_id"], name=meta["name"], market=meta["market"],
        rule="§10",
        detail=f"6日均量 {avg6r:.1f}倍, 當日 {vr:.1f}倍 60日均量 (市場均 {mkt_vr:.1f}倍)",
    )


# 2026-08-10 amendment (TWSE and TPEx alike) rewrote the §12 bands: the rule
# now only bites above 1,000元, and the steps are much coarser than the
# pre-amendment ladders. Our §12 == 要點第十一款 (the two documents number the
# same criterion differently; 詳細規定第十二條 is where the數據 live).
_R12_NEW_RULES_START = date(2026, 8, 10)

# The graduated ladder below is not the original rule either. Reconstructing
# thresholds from the 起迄價差 quoted in historical 注意 announcements, every
# close-price bucket sits flat at 100元 (TWSE) / 70元 (TPEx) through
# 2024-07-03 — 大立光 triggered at 5,780元 on a 150元 spread in 2017, where the
# ladder would demand 375元 — and matches the ladder without exception from
# the 2024-07-18 announcements onward. The boundary is inferred from that
# data, not from a published effective date.
_R12_LADDER_START = date(2024, 7, 18)


def r12_threshold(
    today_close: float, market: str, trade_date: date
) -> float | None:
    """§12 起迄價差 threshold by market. None = rule does not apply at all.
    From 2026-08-10 both markets share one ladder (要點第四條第一項第十一款,
    數據定義於「異常標準之詳細數據及除外情形」第十二條): the 款 only applies
    above 1,000元 — 1,001~2,000 → 300元, then every full 1,000元 band adds
    150元 (2,001~3,000 → 450元, ... 19,001~20,000 → 3,000元).
    From 2024-07-18 to the amendment the two markets had separate ladders:
        TWSE 100元 base + 25元 per 500元 above 500元
        TPEx  70元 base + 15元 per 300元 above 300元
    Before 2024-07-18 those bases applied flat, with no price scaling."""
    if trade_date >= _R12_NEW_RULES_START:
        if today_close <= 1000:
            return None
        # 逾2,000元起每滿1,000元為一級距, 價差標準每級距 +150元, 逐級疊加
        return 300 + max(0, math.ceil(today_close / 1000) - 2) * 150
    if market == "TPEx":
        base, step, level = 70, 15, 300
    else:
        base, step, level = 100, 25, 500
    if trade_date < _R12_LADDER_START or today_close < level:
        return base
    return base + int(today_close // level) * step


def _check_rule_12(prices: list, meta: dict, mkt: dict) -> Alert | None:
    """§12 6日起迄價差≥X元 (高價股分級加碼) 且當日為6日最高或最低.
    TWSE: ≥100元, 每500元 +25元
    TPEx: ≥70元, 每300元 +15元
    Ex-div / ex-rights adjusted via adj_cum × first_close (trading-only diff)."""
    if len(prices) < 6:
        return None
    today = prices[-1]["close"]
    closes_6d = [p["close"] for p in prices[-6:]]
    first_6d = closes_6d[0]
    high_6d = max(closes_6d)
    low_6d = min(closes_6d)
    # Adjusted 起迄 = first × adj_6d_cum / 100 (trading-only NTD change)
    adj_cum_pct = _calc_6d_change_pct(prices)
    if adj_cum_pct is None:
        diff = abs(today - first_6d)
    else:
        diff = abs(first_6d * adj_cum_pct / 100)

    threshold = r12_threshold(
        today, meta.get("market", "TWSE"), mkt["trade_date"]
    )
    if threshold is None or diff < threshold:
        return None

    # Today must be the 6-day high (if 起迄 > 0) or low (if 起迄 < 0)
    is_high = today >= high_6d
    is_low = today <= low_6d
    if not is_high and not is_low:
        return None

    direction = "新高" if is_high else "新低"
    return Alert(
        stock_id=meta["stock_id"], name=meta["name"], market=meta["market"],
        rule="§12",
        detail=f"6日起迄價差 {diff:.0f}元 (門檻 {threshold}元), 收盤{direction} {today:.0f}元",
    )


# 第12款 (our §13) thresholds are market-specific. Reconstructed from the
# ratios quoted in 注意 announcements: the minimum ever published is 12.01% /
# 5.01x on TWSE but 9.00% / 4.00x on TPEx. Applying the TWSE pair to both
# markets missed 326 of 447 TPEx announcements (73%).
_R13_THRESHOLDS = {"TWSE": (12.0, 5.0), "TPEx": (9.0, 4.0)}


def r13_thresholds(market: str) -> tuple[float, float]:
    """§13 6-day SBL ratio (%) and prev-day SBL multiple, by market."""
    return _R13_THRESHOLDS.get(market, _R13_THRESHOLDS["TWSE"])


def _check_rule_13(prices: list, meta: dict, mkt: dict) -> Alert | None:
    """§13 6日借券占比≥門檻 + 前日借券≥門檻倍60日均 (門檻依市場, 見 r13_thresholds)"""
    if len(prices) < 61:
        return None

    total_sbl_6d = sum(p["sbl_sell"] for p in prices[-6:])
    total_vol_6d = sum(p["volume"] for p in prices[-6:])
    if total_vol_6d == 0:
        return None
    ratio_th, mult_th = r13_thresholds(meta.get("market", "TWSE"))
    sbl_ratio = total_sbl_6d / total_vol_6d * 100
    if sbl_ratio < ratio_th:
        return None

    prev_sbl = prices[-2]["sbl_sell"]
    avg60_sbl = sum(p["sbl_sell"] for p in prices[-62:-2]) / 60
    if avg60_sbl == 0:
        return None
    sbl_mult = prev_sbl / avg60_sbl
    if sbl_mult < mult_th:
        return None

    return Alert(
        stock_id=meta["stock_id"], name=meta["name"], market=meta["market"],
        rule="§13",
        detail=f"6日借券占比 {sbl_ratio:.1f}% + 前日借券 {sbl_mult:.1f}倍 60日均",
    )


def _check_rule_14(prices: list, meta: dict, mkt: dict) -> Alert | None:
    """§14 6日當沖占比>60% + 前日當沖占比>60%"""
    if len(prices) < 7:
        return None

    total_dt_6d = sum(p["dt_volume"] for p in prices[-6:])
    total_vol_6d = sum(p["volume"] for p in prices[-6:])
    if total_vol_6d == 0:
        return None
    dt_ratio_6d = total_dt_6d / total_vol_6d * 100
    if dt_ratio_6d <= 60:
        return None

    prev = prices[-2]
    if prev["volume"] == 0:
        return None
    dt_ratio_prev = prev["dt_volume"] / prev["volume"] * 100
    if dt_ratio_prev <= 60:
        return None

    return Alert(
        stock_id=meta["stock_id"], name=meta["name"], market=meta["market"],
        rule="§14",
        detail=f"6日當沖占比 {dt_ratio_6d:.1f}% + 前日 {dt_ratio_prev:.1f}%",
    )


# ---------------------------------------------------------------------------
# Disposal prediction
# ---------------------------------------------------------------------------

def predict_disposal(trade_date: date) -> list[DisposalRisk]:
    """Find stocks with ≥2 consecutive attention days → likely disposal."""
    with get_cursor(commit=False) as cur:
        # Get recent trading days
        cur.execute("""
            SELECT DISTINCT trade_date FROM tw.index_prices
            WHERE index_id = 'TAIEX' AND trade_date <= %s
            ORDER BY trade_date DESC LIMIT 10
        """, (trade_date,))
        # Re-sort ascending so consecutive-day arithmetic below (idx_cur >
        # idx_prev for later dates) matches the natural mental model.
        trading_days = sorted(r["trade_date"] for r in cur.fetchall())

        if len(trading_days) < 3:
            return []

        cur.execute("""
            SELECT sa.stock_id, sa.alert_date, s.name, s.market
            FROM tw.stock_alerts sa
            JOIN tw.stocks s ON s.stock_id = sa.stock_id
            WHERE sa.alert_type = 'attention'
              AND sa.alert_date >= %s
            ORDER BY sa.stock_id, sa.alert_date
        """, (trading_days[0],))
        rows = cur.fetchall()

    by_stock: dict[str, list] = {}
    stock_meta: dict[str, dict] = {}
    for r in rows:
        sid = r["stock_id"]
        by_stock.setdefault(sid, []).append(r["alert_date"])
        stock_meta[sid] = {"name": r["name"], "market": r["market"]}

    results = []
    for sid, dates in by_stock.items():
        unique_dates = sorted(set(dates))
        # Find consecutive runs ending at or near trade_date
        consecutive = 1
        recent = [unique_dates[-1]]
        for i in range(len(unique_dates) - 1, 0, -1):
            idx_cur = trading_days.index(unique_dates[i]) if unique_dates[i] in trading_days else -1
            idx_prev = trading_days.index(unique_dates[i - 1]) if unique_dates[i - 1] in trading_days else -1
            if idx_cur >= 0 and idx_prev >= 0 and idx_cur - idx_prev == 1:
                consecutive += 1
                recent.append(unique_dates[i - 1])
            else:
                break

        if consecutive >= 2:
            results.append(DisposalRisk(
                stock_id=sid,
                name=stock_meta[sid]["name"],
                market=stock_meta[sid]["market"],
                consecutive_days=consecutive,
                recent_dates=sorted(recent),
            ))

    results.sort(key=lambda x: -x.consecutive_days)
    return results


# ---------------------------------------------------------------------------
# Main prediction
# ---------------------------------------------------------------------------

ALL_RULES = [
    _check_rule_2_1,
    _check_rule_2_2,
    _check_rule_4,
    _check_rule_8,
    _check_rule_10,
    _check_rule_12,
    _check_rule_13,
    _check_rule_14,
]


def predict_attention(trade_date: date) -> list[Alert]:
    """Run all attention criteria and return flagged stocks."""
    print(f"Loading price data for {trade_date} ...")
    stocks, meta_map = _load_prices(trade_date)
    print(f"  Loaded {len(stocks)} stocks")

    print("Computing market averages ...")
    mkt = _compute_market_averages(stocks, meta_map)
    # Rules whose thresholds changed by amendment date need the report date,
    # not the stock's own last bar (suspended stocks lag behind it).
    mkt["trade_date"] = trade_date
    print(f"  Market 6d change avg: {mkt['change_6d']:.2f}%")
    print(f"  Market volume ratio avg: {mkt['vol_ratio']:.2f}x")

    alerts = []
    for sid, prices in stocks.items():
        m = {**meta_map[sid], "stock_id": sid}
        # The 作業要點 covers 上市/上櫃 common stocks only. ETFs and 興櫃 (ESB)
        # are outside it entirely — no such security has ever appeared in
        # tw.stock_alerts — but _is_common_stock was only being applied to the
        # market averages, so they were still being flagged.
        if not _is_common_stock(m.get("security_type")):
            continue
        if m.get("market") not in ALERT_MARKETS:
            continue

        # §3 returns a list
        alerts.extend(_check_rule_3(prices, m, mkt))

        for rule_fn in ALL_RULES:
            result = rule_fn(prices, m, mkt)
            if result:
                alerts.append(result)

    # Deduplicate: same stock+rule
    seen = set()
    unique = []
    for a in alerts:
        key = (a.stock_id, a.rule)
        if key not in seen:
            seen.add(key)
            unique.append(a)

    unique.sort(key=lambda a: (a.stock_id, a.rule))
    return unique


def run_report(trade_date: date):
    """Print a full attention/disposal prediction report."""
    print(f"\n{'='*70}")
    print(f"  Attention/Disposal Prediction Report: {trade_date}")
    print(f"{'='*70}\n")

    # Attention prediction
    alerts = predict_attention(trade_date)
    print(f"\n--- Attention Predictions ({len(alerts)} alerts) ---\n")

    by_stock: dict[str, list[Alert]] = {}
    for a in alerts:
        by_stock.setdefault(a.stock_id, []).append(a)

    for sid in sorted(by_stock.keys()):
        group = by_stock[sid]
        first = group[0]
        rules = ", ".join(a.rule for a in group)
        print(f"  {sid} {first.name} ({first.market}) [{rules}]")
        for a in group:
            print(f"    {a.rule}: {a.detail}")

    # Disposal prediction
    print(f"\n--- Disposal Predictions ---\n")
    risks = predict_disposal(trade_date)
    if risks:
        for r in risks:
            dates_str = ", ".join(str(d) for d in r.recent_dates)
            status = "⚠ 極高風險" if r.consecutive_days >= 3 else "⚡ 高風險"
            print(f"  {r.stock_id} {r.name} ({r.market}) "
                  f"連續 {r.consecutive_days} 日注意 {status}")
            print(f"    注意日期: {dates_str}")
    else:
        print("  No disposal risks detected.")

    print(f"\n{'='*70}")
    print(f"  Summary: {len(by_stock)} stocks flagged, {len(risks)} disposal risks")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    import sys
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) >= 2 else date.today()
    run_report(d)
