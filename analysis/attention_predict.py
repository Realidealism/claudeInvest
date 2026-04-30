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
  §12   6-day price diff ≥100 (scaled for high-price stocks)
  §13   6-day SBL ratio ≥12% + SBL ≥5x 60-day avg
  §14   6-day day-trade ratio >60% + prev day >60%

Disposal prediction:
  Consecutive attention days ≥2 → likely disposal next day.
"""

from __future__ import annotations

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
            SELECT stock_id, trade_date, close_price, volume,
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
            "volume": r["volume"] or 0,
            "dt_volume": r["dt_volume"],
            "margin_balance": r["margin_balance"],
            "short_balance": r["short_balance"],
            "sbl_sell": r["sbl_sell"],
        })

    return stocks, meta


def _is_common_stock(sec_type: str | None) -> bool:
    return sec_type in (None, "STOCK")


# ---------------------------------------------------------------------------
# Market-wide averages
# ---------------------------------------------------------------------------

def _calc_6d_change_pct(prices: list[dict]) -> float | None:
    """6-day cumulative close price change %."""
    if len(prices) < 7:
        return None
    return (prices[-1]["close"] / prices[-7]["close"] - 1) * 100


def _calc_nd_change_pct(prices: list[dict], n: int) -> float | None:
    """N-day price change % (close[-1] vs close[-n-1])."""
    if len(prices) < n + 1:
        return None
    return (prices[-1]["close"] / prices[-(n + 1)]["close"] - 1) * 100


def _calc_volume_ratio(prices: list[dict]) -> float | None:
    """Today's volume / 60-day average volume."""
    if len(prices) < 61:
        return None
    avg60 = sum(p["volume"] for p in prices[-61:-1]) / 60
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
    """Compute market-wide and industry-level averages for comparison."""
    all_changes_6d = []
    all_vol_ratios = []
    industry_changes: dict[str, list] = {}

    for sid, prices in stocks.items():
        if not _is_common_stock(meta[sid].get("security_type")):
            continue

        chg = _calc_6d_change_pct(prices)
        if chg is not None:
            all_changes_6d.append(chg)
            ind = meta[sid].get("industry") or "unknown"
            industry_changes.setdefault(ind, []).append(chg)

        vr = _calc_volume_ratio(prices)
        if vr is not None:
            all_vol_ratios.append(vr)

    market_avg_change_6d = sum(all_changes_6d) / len(all_changes_6d) if all_changes_6d else 0
    market_avg_vol_ratio = sum(all_vol_ratios) / len(all_vol_ratios) if all_vol_ratios else 0

    ind_avg_change = {}
    for ind, vals in industry_changes.items():
        ind_avg_change[ind] = sum(vals) / len(vals) if vals else 0

    return {
        "change_6d": market_avg_change_6d,
        "vol_ratio": market_avg_vol_ratio,
        "industry_change_6d": ind_avg_change,
        "industry_count": {ind: len(vals) for ind, vals in industry_changes.items()},
    }


# ---------------------------------------------------------------------------
# Rule checks
# ---------------------------------------------------------------------------

def _check_rule_2_1(prices: list, meta: dict, mkt: dict) -> Alert | None:
    """§2① 6日累積漲跌幅 >32%, 差幅≥20%"""
    chg = _calc_6d_change_pct(prices)
    if chg is None or abs(chg) <= 32:
        return None
    ind = meta.get("industry") or "unknown"
    ind_avg = mkt["industry_change_6d"].get(ind, 0)
    mkt_avg = mkt["change_6d"]
    if mkt["industry_count"].get(ind, 0) < 5:
        return None
    if abs(chg - mkt_avg) < 20 or abs(chg - ind_avg) < 20:
        return None
    if meta.get("security_type") not in (None, "STOCK"):
        return None
    direction = "漲" if chg > 0 else "跌"
    return Alert(
        stock_id=meta["stock_id"], name=meta["name"], market=meta["market"],
        rule="§2①",
        detail=f"6日累積{direction}幅 {abs(chg):.1f}% (市場均 {mkt_avg:.1f}%, 同業均 {ind_avg:.1f}%)",
    )


def _check_rule_2_2(prices: list, meta: dict, mkt: dict) -> Alert | None:
    """§2② 6日累積漲跌 >25% + 價差≥50元, 差幅≥20%"""
    chg = _calc_6d_change_pct(prices)
    if chg is None or abs(chg) <= 25:
        return None
    if len(prices) < 7:
        return None
    diff = abs(prices[-1]["close"] - prices[-7]["close"])
    if diff < 50:
        return None
    ind = meta.get("industry") or "unknown"
    ind_avg = mkt["industry_change_6d"].get(ind, 0)
    mkt_avg = mkt["change_6d"]
    if mkt["industry_count"].get(ind, 0) < 5:
        return None
    if abs(chg - mkt_avg) < 20 or abs(chg - ind_avg) < 20:
        return None
    direction = "漲" if chg > 0 else "跌"
    return Alert(
        stock_id=meta["stock_id"], name=meta["name"], market=meta["market"],
        rule="§2②",
        detail=f"6日累積{direction}幅 {abs(chg):.1f}%, 價差 {diff:.0f}元",
    )


def _check_rule_3(prices: list, meta: dict, mkt: dict) -> list[Alert]:
    """§3 30/60/90日起迄漲跌 >100/130/160%"""
    alerts = []
    thresholds = [(30, 100, 85), (60, 130, 110), (90, 160, 135)]
    for days, pct_thresh, diff_thresh in thresholds:
        chg = _calc_nd_change_pct(prices, days)
        if chg is None or abs(chg) <= pct_thresh:
            continue
        direction = "漲" if chg > 0 else "跌"
        alerts.append(Alert(
            stock_id=meta["stock_id"], name=meta["name"], market=meta["market"],
            rule=f"§3({days}日)",
            detail=f"{days}日累積{direction}幅 {abs(chg):.1f}% (門檻 {pct_thresh}%)",
        ))
    return alerts


def _check_rule_4(prices: list, meta: dict, mkt: dict) -> Alert | None:
    """§4 6日漲跌>25% + 當日成交量≥5倍60日均, 倍數差≥4"""
    chg = _calc_6d_change_pct(prices)
    if chg is None or abs(chg) <= 25:
        return None
    vr = _calc_volume_ratio(prices)
    if vr is None or vr < 5:
        return None
    mkt_vr = mkt["vol_ratio"]
    if abs(vr - mkt_vr) < 4:
        return None
    ind = meta.get("industry") or "unknown"
    ind_avg = mkt["industry_change_6d"].get(ind, 0)
    mkt_avg = mkt["change_6d"]
    if abs(chg - mkt_avg) < 20 or abs(chg - ind_avg) < 20:
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
    mkt_avg = mkt["change_6d"]
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


def _check_rule_12(prices: list, meta: dict, mkt: dict) -> Alert | None:
    """§12 6日起迄價差≥100元 (高價股每500元增加25元門檻)"""
    if len(prices) < 7:
        return None
    today = prices[-1]["close"]
    closes_6d = [p["close"] for p in prices[-7:]]
    high_6d = max(closes_6d)
    low_6d = min(closes_6d)
    diff = high_6d - low_6d

    threshold = 100
    if today >= 500:
        extra_levels = int(today // 500)
        threshold = 100 + (extra_levels - 0) * 25

    if diff < threshold:
        return None

    is_high = today == high_6d or (today == closes_6d[-1] and today > closes_6d[0])
    is_low = today == low_6d or (today == closes_6d[-1] and today < closes_6d[0])
    if not is_high and not is_low:
        return None

    direction = "新高" if is_high else "新低"
    return Alert(
        stock_id=meta["stock_id"], name=meta["name"], market=meta["market"],
        rule="§12",
        detail=f"6日價差 {diff:.0f}元 (門檻 {threshold}元), 收盤{direction} {today:.0f}元",
    )


def _check_rule_13(prices: list, meta: dict, mkt: dict) -> Alert | None:
    """§13 6日借券占比≥12% + 前日借券≥5倍60日均"""
    if len(prices) < 61:
        return None

    total_sbl_6d = sum(p["sbl_sell"] for p in prices[-6:])
    total_vol_6d = sum(p["volume"] for p in prices[-6:])
    if total_vol_6d == 0:
        return None
    sbl_ratio = total_sbl_6d / total_vol_6d * 100
    if sbl_ratio < 12:
        return None

    prev_sbl = prices[-2]["sbl_sell"]
    avg60_sbl = sum(p["sbl_sell"] for p in prices[-62:-2]) / 60
    if avg60_sbl == 0:
        return None
    sbl_mult = prev_sbl / avg60_sbl
    if sbl_mult < 5:
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
        trading_days = [r["trade_date"] for r in cur.fetchall()]

        if len(trading_days) < 3:
            return []

        cur.execute("""
            SELECT sa.stock_id, sa.alert_date, s.name, s.market
            FROM tw.stock_alerts sa
            JOIN tw.stocks s ON s.stock_id = sa.stock_id
            WHERE sa.alert_type = 'attention'
              AND sa.alert_date >= %s
            ORDER BY sa.stock_id, sa.alert_date
        """, (trading_days[-1],))
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
    print(f"  Market 6d change avg: {mkt['change_6d']:.2f}%")
    print(f"  Market volume ratio avg: {mkt['vol_ratio']:.2f}x")

    alerts = []
    for sid, prices in stocks.items():
        m = {**meta_map[sid], "stock_id": sid}

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
