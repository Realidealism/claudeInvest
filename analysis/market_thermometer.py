"""市場溫度計 — descriptive fragility gauge (綜合型 L1).

A 0-100 "how stretched is the market right now" reading, NOT a crash predictor
(see memory project_market_thermometer: simple gates don't time crashes; this only
describes current tension). Two validated-as-meaningful components, equal weight:

  外資期貨定位  foreign TX net open interest — hotter the more net-short foreigns
                are vs their own recent range (survived a fair false-positive test,
                1.47x standalone).
  融資水位      margin balance percentile — leverage backdrop (elevated at 6/7 tops).
  選擇權自滿    low put/call OI ratio — complacency / under-hedging. Weak alone
                (1.18x) but ANDed onto the futures gate lifted precision 1.47x→2.10x
                (semi-independent), so it earns a slot; foreign spot selling was
                tested and rejected (0x).

More components can be appended later. Buckets are temperature words on purpose
(冷靜/溫和/偏緊/過熱), not 攻擊/防守 — we did not validate timing.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

MARGIN_MA_WIN = 55  # 融資餘額金額 55日均線 ±2σ 乖離 (Bollinger z; deviation-from-trend, 不受長多水位長期偏高影響)
FUT_WIN = 90       # 外資期貨 net_oi percentile lookback (swept 2026-07-21: 90 best, bottom-decile
                   # gate 1.53x vs 120's 1.33x, same 6/6 cover; matches P/C's 90; tmp/_fut_window_sweep.py)
PC_WIN = 90        # put/call OI ratio percentile lookback (swept 2026-07-21: 60-90 plateau,
                   # 90 best — low-P/C+futures gate 2.65x vs 120's 2.10x, time-uniform; tmp/_pc_window_sweep.py)
RETAIL_WIN = 120   # 微台散戶淨多空比 percentile lookback
RETAIL_W = 0.5     # 微台散戶 weight (down-weighted: only ~2yr history since 2024-07)
HI_WINDOW = 55     # 位階 context: index near its 55-day high
NEAR_HIGH_TOL = 2.0  # within 2% of the 55d high counts as 近高
HISTORY_DAYS = 250 # sparkline length

BUCKETS = [(80, "過熱"), (60, "偏緊"), (40, "溫和"), (0, "冷靜")]


def _bucket(score: float) -> str:
    for lo, name in BUCKETS:
        if score >= lo:
            return name
    return "冷靜"


def _roll_pctl(s: pd.Series, win: int) -> pd.Series:
    """Percentile rank (0-1) of each value within its trailing `win` window."""
    return s.rolling(win, min_periods=win).apply(lambda w: (w <= w.iloc[-1]).mean(), raw=False)


def _retail_micro(cur) -> pd.DataFrame:
    """微台散戶淨多空比 proxy: retail ≈ total 微台 OI − 三大法人 (微台 ~97% retail, so
    大額-trader contamination is minimal). retail_net = −Σ法人net (futures zero-sum
    identity), as % of OI. Only 2024-07+ (三大法人 微台 start). Higher net-long =
    more retail froth = hotter (contrarian)."""
    cur.execute("""SELECT trade_date, SUM(open_interest) oi FROM tw.taifex_futures_daily
                   WHERE contract='TMF' AND session='一般' AND contract_month NOT LIKE '%%/%%'
                     AND open_interest IS NOT NULL GROUP BY trade_date ORDER BY trade_date""")
    oi = pd.DataFrame(cur.fetchall())
    cur.execute("""SELECT trade_date, SUM(net_oi) instnet FROM tw.taifex_inst_futures
                   WHERE product='微型臺指期貨' GROUP BY trade_date ORDER BY trade_date""")
    inst = pd.DataFrame(cur.fetchall())
    if oi.empty or inst.empty:
        return pd.DataFrame(columns=["d", "retail_hot", "retail_pct"])
    oi["d"] = pd.to_datetime(oi["trade_date"]); oi["oi"] = oi["oi"].astype(float)
    inst["d"] = pd.to_datetime(inst["trade_date"]); inst["instnet"] = inst["instnet"].astype(float)
    r = oi[["d", "oi"]].merge(inst[["d", "instnet"]], on="d").sort_values("d").reset_index(drop=True)
    r["retail_pct"] = -r["instnet"] / r["oi"] * 100
    r["retail_hot"] = _roll_pctl(r["retail_pct"], RETAIL_WIN) * 100
    return r[["d", "retail_hot", "retail_pct"]]


def build_thermometer(cur, today: date | None = None) -> dict:
    cur.execute("SELECT trade_date, margin_balance_value FROM tw.margin_summary ORDER BY trade_date")
    mg = pd.DataFrame(cur.fetchall())
    cur.execute("""SELECT trade_date, net_oi FROM tw.taifex_inst_futures
                   WHERE product='臺股期貨' AND investor='外資及陸資' ORDER BY trade_date""")
    fu = pd.DataFrame(cur.fetchall())
    cur.execute("SELECT trade_date, pc_oi_ratio FROM tw.taifex_pc_ratio ORDER BY trade_date")
    pc = pd.DataFrame(cur.fetchall())
    cur.execute("SELECT trade_date, close_price FROM tw.index_prices WHERE index_id='TAIEX' ORDER BY trade_date")
    ix = pd.DataFrame(cur.fetchall())
    mg["d"] = pd.to_datetime(mg["trade_date"]); mg["mgn"] = mg["margin_balance_value"].astype(float)
    fu["d"] = pd.to_datetime(fu["trade_date"]); fu["noi"] = fu["net_oi"].astype(float)
    pc["d"] = pd.to_datetime(pc["trade_date"]); pc["pc"] = pc["pc_oi_ratio"].astype(float)
    ix["d"] = pd.to_datetime(ix["trade_date"]); ix["tx"] = ix["close_price"].astype(float)
    ix["pct_from_high"] = (ix["tx"] / ix["tx"].rolling(HI_WINDOW).max() - 1) * 100
    m = (mg[["d", "mgn"]].merge(fu[["d", "noi"]], on="d").merge(pc[["d", "pc"]], on="d")
         .merge(ix[["d", "pct_from_high", "tx"]], on="d").sort_values("d").reset_index(drop=True))

    ma = m["mgn"].rolling(MARGIN_MA_WIN).mean()
    sd = m["mgn"].rolling(MARGIN_MA_WIN).std()
    m["margin_z"] = (m["mgn"] - ma) / sd                        # deviation from 55d trend in σ
    m["margin_hot"] = ((m["margin_z"] + 2) / 4).clip(0, 1) * 100  # −2σ→0, 均線→50, +2σ→100
    m["futures_hot"] = (1 - _roll_pctl(m["noi"], FUT_WIN)) * 100   # more net-short -> hotter
    m["pc_hot"] = (1 - _roll_pctl(m["pc"], PC_WIN)) * 100          # lower P/C (complacency) -> hotter
    m["core3"] = m["margin_hot"] + m["futures_hot"] + m["pc_hot"]

    # 微台散戶: down-weighted 4th component, left-joined so the pre-2024 history
    # keeps its 3-component score (no retail data before 2024-07).
    r = _retail_micro(cur)
    m = m.merge(r, on="d", how="left")
    has_r = m["retail_hot"].notna()
    m["score"] = np.where(has_r,
                          (m["core3"] + RETAIL_W * m["retail_hot"]) / (3 + RETAIL_W),
                          m["core3"] / 3)
    m = m.dropna(subset=["core3"]).reset_index(drop=True)
    if m.empty:
        return {"as_of": None, "score": None, "bucket": None, "components": [], "history": []}

    last = m.iloc[-1]
    hist = m.tail(HISTORY_DAYS)
    components = [
        {"key": "futures", "name": "外資期貨定位", "hot": round(last["futures_hot"], 1),
         "detail": f"外資臺股期貨淨未平倉 {int(last['noi']):+,} 口（{FUT_WIN}日百分位越低越淨空＝越熱）"},
        {"key": "margin", "name": "融資水位", "hot": round(last["margin_hot"], 1),
         "detail": f"融資餘額金額 {last['mgn']/1e5:,.0f}億（距 {MARGIN_MA_WIN} 日均 {last['margin_z']:+.2f}σ；+2σ=過熱）"},
        {"key": "pc", "name": "選擇權自滿", "hot": round(last["pc_hot"], 1),
         "detail": f"Put/Call OI 比 {last['pc']:.2f}（{PC_WIN}日百分位越低越自滿＝越熱）"},
    ]
    if pd.notna(last["retail_hot"]):
        components.append({
            "key": "retail", "name": "微台散戶多單", "hot": round(last["retail_hot"], 1),
            "detail": f"微台散戶淨多 {last['retail_pct']:+.0f}% OI（{RETAIL_WIN}日百分位越高越froth＝越熱；降權，史僅2024-07+）"})
    pfh = last["pct_from_high"]
    return {
        "as_of": last["d"].date().isoformat(),
        "score": round(last["score"], 1),
        "bucket": _bucket(last["score"]),
        "near_high": bool(pfh >= -NEAR_HIGH_TOL),
        "pct_from_high": round(float(pfh), 1),
        "hi_window": HI_WINDOW,
        "components": components,
        "history": [{"date": r.d.date().isoformat(), "score": round(r.score, 1), "tx": round(r.tx)}
                    for r in hist.itertuples(index=False)],
    }


if __name__ == "__main__":
    from db.connection import get_cursor
    with get_cursor(commit=False) as cur:
        t = build_thermometer(cur)
        print(f"as_of={t['as_of']} score={t['score']} bucket={t['bucket']}")
        for c in t["components"]:
            print(f"  {c['name']}: hot={c['hot']}  {c['detail']}")
        # sanity: was the gauge running hot before the 6 covered crash peaks?
        cur.execute("SELECT trade_date, margin_balance_value FROM tw.margin_summary ORDER BY trade_date")
        import pandas as pd
        mg = pd.DataFrame(cur.fetchall()); mg["d"] = pd.to_datetime(mg["trade_date"])
        cur.execute("""SELECT trade_date, net_oi FROM tw.taifex_inst_futures
                       WHERE product='臺股期貨' AND investor='外資及陸資' ORDER BY trade_date""")
        fu = pd.DataFrame(cur.fetchall()); fu["d"] = pd.to_datetime(fu["trade_date"])
        cur.execute("SELECT trade_date, pc_oi_ratio FROM tw.taifex_pc_ratio ORDER BY trade_date")
        pc = pd.DataFrame(cur.fetchall()); pc["d"] = pd.to_datetime(pc["trade_date"])
        mg["mgn"] = mg["margin_balance_value"].astype(float); fu["noi"] = fu["net_oi"].astype(float)
        pc["pc"] = pc["pc_oi_ratio"].astype(float)
        m = (mg[["d", "mgn"]].merge(fu[["d", "noi"]], on="d").merge(pc[["d", "pc"]], on="d")
             .sort_values("d").reset_index(drop=True))
        m["mh"] = (((m["mgn"] - m["mgn"].rolling(MARGIN_MA_WIN).mean()) / m["mgn"].rolling(MARGIN_MA_WIN).std() + 2) / 4).clip(0, 1) * 100
        m["fh"] = (1 - _roll_pctl(m["noi"], FUT_WIN)) * 100
        m["ph"] = (1 - _roll_pctl(m["pc"], PC_WIN)) * 100
        m["s"] = (m["mh"] + m["fh"] + m["ph"]) / 3
        m = m.set_index("d")
        print("\nsanity — 崩盤峰當日溫度分數 (vs 全樣本中位):")
        print(f"  全樣本 score 中位={m['s'].median():.0f} p75={m['s'].quantile(.75):.0f}")
        for p in ["2020-01-14", "2021-04-27", "2022-01-04", "2024-07-11", "2026-02-26", "2026-06-22"]:
            i = m.index.searchsorted(pd.Timestamp(p))
            if i < len(m):
                print(f"  {p}: score={m['s'].iloc[i]:.0f}")
