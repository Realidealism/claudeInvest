"""市場溫度計 — descriptive fragility gauge (綜合型 L1).

A 0-100 "how stretched is the market right now" reading, NOT a crash predictor
(see memory project_market_thermometer: simple gates don't time crashes; this only
describes current tension). Two components drive the 定位極端度 score, equal weight:

  外資期貨定位  foreign TX net open interest — hotter the more net-short foreigns
                are vs their own recent range (survived a fair false-positive test,
                1.47x standalone).
  融資水位      margin balance percentile — leverage backdrop (elevated at 6/7 tops).

P/C ratio and 微台散戶 were dropped (2026-07-21): both are extreme at BOTH tops and
bottoms (contrarian, no directional discrimination), so averaging them in only
diluted the score. The actionable signals live outside this gauge — 頂部過熱 uses
外資期貨 fresh-low, 攻防 uses OBV + 排列, 恐慌買進 uses 融資 + 深跌 + 快殺.
"""

from __future__ import annotations

from datetime import date, datetime

import numpy as np
import pandas as pd

from analysis.obv import calculate_obv, PERIOD_PARAMS

MARGIN_MA_WIN = 55  # 融資餘額金額 55日均線 ±2σ 乖離 (Bollinger z; deviation-from-trend, 不受長多水位長期偏高影響)
FUT_WIN = 90       # 外資期貨 net_oi percentile lookback (swept 2026-07-21: 90 best, bottom-decile
                   # gate 1.53x vs 120's 1.33x, same 6/6 cover; tmp/_fut_window_sweep.py)
HI_WINDOW = 55     # 位階 context: index near its 55-day high
NEAR_HIGH_TOL = 2.0  # within 2% of the 55d high counts as 近高
# 攻防狀態: short-timeframe 多空頭排列 (market_breadth.short_trend, -2..+2). 攻擊 =
# 偏多以上 (>0); 防守 = 中性以下 (<=0). More responsive than a 20d MA (defensive ~49%
# of the time) but flips on short bounces.
ST_LABELS = {2: "強多", 1: "偏多", 0: "中性", -1: "偏空", -2: "強空"}
# 恐慌買進 (contrarian V-bottom, discrete): 深跌 + 融資短窗急殺(斷頭 flush). Validated
# 6/8 yrs positive BUT fails in grinding bears (2018/2022 negative, fired most) — a
# SHARP-CRASH-ONLY tool; caveat prominently. 深跌 must be after a fast washout.
PANIC_PFH = -8.0     # 距 55 日高 <= -8% (deep drawdown)
PANIC_MARGIN_WIN = 5  # 融資餘額金額 5-day drop window
PANIC_MARGIN_CHG = -2.0  # 融資 5 日跌 <= -2% (斷頭急殺)
PANIC_SPEED_WIN = 10  # 快殺 window (V-bottoms plunge fast; filters slow grinding bears)
PANIC_SPEED_CHG = -6.0  # 指數 10 日跌 <= -6% (fast washout; +9.3%/71% vs +6.9%/68%)
# 離散警戒燈 (discrete alarm, NOT a prediction): the validated crash gate. 近高 +
# 外資淨空創 60日新低 + 低P/C自滿. Precision 2.98x but ~71% false — a caution flag only.
ALERT_LOW_WIN = 60   # foreign net-OI fresh N-day net-short low (swept: 60 > 40)
# 融資過熱 (independent 2nd top flag): Bollinger z of 融資餘額/55MA成交金額. Normalizing 融資
# by turnover then de-trending gives 2.17x@+1.5σ (matches 外資期貨) with fixed persistence,
# and uniquely caught the 2025-03 -23% top that 外資期貨 missed. NOT OR-merged with fresh_low
# (that diluted clean years); shown as a separate flag. ~70% false, non-uniform, caution only.
# tmp/_overheat_mt_bollinger.py, _overheat_optimize.py.
MARGIN_OVERHEAT_Z = 1.5
TOP_LOOKBACK = 3     # 攻防 entry: 過熱 within the last 3 trading days counts
STANCE_EXIT_DAYS = 3  # 攻防 exit: 排列 連續 N 天回到中性以上(short_trend>=0) 才解除防守
# 大台期貨 OBV 弱勢 = ScoreBoard short-scope OBV in bearish state (trend<0), aligned with
# analysis.obv (short scope only). We use the persistent latched trend, NOT signal_down:
# the sparse down-cross event misses declines with no fresh cross (2026-07-09~16 reopened
# the stance gap), while trend<0 stays weak through the whole decline. The latched-trend
# form was rejected UPSTREAM only for cross-sectional scoring (cross-timeframe redundancy),
# which doesn't apply to a single market dial. tmp/_txf_obv_build_faithful.py.
HISTORY_DAYS = 250 # sparkline length

def _regime(score: float, near_high: bool, fresh_low: bool) -> tuple[str, str, bool]:
    """(label, colour, is_danger). 頂部過熱 (the real top warning) is 外資期貨-driven:
    among near-high days, foreign net-OI (期) is the ONLY component that separates
    tops from ordinary highs (融資/P/C/散戶 are high at almost every high). So the
    red danger = 近高 AND 外資淨空創 60 日新低 (catches 5/6 tops incl. the recent
    wave; still ~82% false — top prediction is inherently hard). The composite score
    stays a descriptive 定位極端度. Below the high we make no bottom claim."""
    if fresh_low:
        return "頂部過熱", "#ef4444", True
    return "無頂部訊號", "#8a8a9a", False


def _roll_pctl(s: pd.Series, win: int) -> pd.Series:
    """Percentile rank (0-1) of each value within its trailing `win` window."""
    return s.rolling(win, min_periods=win).apply(lambda w: (w <= w.iloc[-1]).mean(), raw=False)


def compute_stance(short_trend, hot_wide) -> np.ndarray:
    """攻防 stateful hysteresis → per-day defensive[] boolean array.

    enter 防守: 加寬過熱 (hot_wide within TOP_LOOKBACK days) AND 排列翻空 (short_trend<0)
    exit 防守: 排列回多方 (short_trend>0) 單日, OR 連續 STANCE_EXIT_DAYS 天中性以上 (>=0)
    hot_wide = fresh_low OR obv_weak (OBV widens entry to cover secondary declines).
    Shared by the daily gauge and the intraday live-stance builder so a
    forming last bar produces the exact same latch as the close recompute."""
    st = pd.Series(np.asarray(short_trend))
    hw = pd.Series(np.asarray(hot_wide))
    fl3 = (hw.rolling(TOP_LOOKBACK, min_periods=1).max() > 0).to_numpy()
    stv = st.to_numpy()
    rec3 = (st.rolling(STANCE_EXIT_DAYS, min_periods=STANCE_EXIT_DAYS).min() >= 0).fillna(False)
    rec = (rec3 | (st > 0)).to_numpy()
    out = np.zeros(len(st), dtype=bool)
    state = False
    for i in range(len(st)):
        if not state:
            if fl3[i] and stv[i] < 0:
                state = True
        elif rec[i]:
            state = False
        out[i] = state
    return out


def _load_merged_frame(cur) -> pd.DataFrame:
    """Merged daily frame (margin / foreign net_oi / TAIEX / short 排列 total /
    大台期貨 OHLCV) on the common trading-date spine. Shared by the daily gauge
    and the intraday stance builder so both run the OBV + hysteresis over the
    identical history. 排列 uses short_trend_total (normal ∪ forming base) —
    matches the intraday breadth sidecar so the close boundary doesn't jump."""
    cur.execute("SELECT trade_date, margin_balance_value FROM tw.margin_summary ORDER BY trade_date")
    mg = pd.DataFrame(cur.fetchall())
    cur.execute("""SELECT trade_date, net_oi FROM tw.taifex_inst_futures
                   WHERE product='臺股期貨' AND investor='外資及陸資' ORDER BY trade_date""")
    fu = pd.DataFrame(cur.fetchall())
    cur.execute("SELECT trade_date, close_price, turnover FROM tw.index_prices WHERE index_id='TAIEX' ORDER BY trade_date")
    ix = pd.DataFrame(cur.fetchall())
    cur.execute("SELECT trade_date, short_trend_total FROM tw.market_breadth ORDER BY trade_date")
    br = pd.DataFrame(cur.fetchall())
    # 大台期貨 front-month OHLCV (per day = non-spread 一般 contract with MAX volume)
    cur.execute("""SELECT DISTINCT ON (trade_date) trade_date, contract_month,
                     open_price o, high_price h, low_price l, close_price c, volume v
                   FROM tw.taifex_futures_daily
                   WHERE contract='TX' AND session='一般' AND contract_month NOT LIKE '%%/%%'
                     AND volume IS NOT NULL AND close_price IS NOT NULL
                   ORDER BY trade_date, volume DESC""")
    tv = pd.DataFrame(cur.fetchall())
    mg["d"] = pd.to_datetime(mg["trade_date"]); mg["mgn"] = mg["margin_balance_value"].astype(float)
    fu["d"] = pd.to_datetime(fu["trade_date"]); fu["noi"] = fu["net_oi"].astype(float)
    ix["d"] = pd.to_datetime(ix["trade_date"]); ix["tx"] = ix["close_price"].astype(float)
    ix["to"] = ix["turnover"].astype(float).ffill()   # a few null-turnover days would else NaN the whole 55d window
    ix["pct_from_high"] = (ix["tx"] / ix["tx"].rolling(HI_WINDOW).max() - 1) * 100
    br["d"] = pd.to_datetime(br["trade_date"]); br["st"] = br["short_trend_total"].astype(int)
    tv["d"] = pd.to_datetime(tv["trade_date"])
    for _k in ("o", "h", "l", "c", "v"):
        tv[_k] = tv[_k].astype(float)
    # limit_refer = prev close; on contract rollover use today's open (neutralize gap)
    tv = tv.sort_values("d").reset_index(drop=True)
    tv["roll"] = tv["contract_month"] != tv["contract_month"].shift(1)
    tv["ref"] = np.where(tv["roll"], tv["o"], tv["c"].shift(1))
    tv.loc[0, "ref"] = tv.loc[0, "c"]
    return (mg[["d", "mgn"]].merge(fu[["d", "noi"]], on="d")
            .merge(ix[["d", "pct_from_high", "tx", "to"]], on="d").merge(br[["d", "st"]], on="d")
            .merge(tv[["d", "o", "h", "l", "c", "v", "ref"]], on="d").sort_values("d").reset_index(drop=True))


def build_thermometer(cur, today: date | None = None) -> dict:
    m = _load_merged_frame(cur)
    # 大台期貨 OBV — aligned with ScoreBoard OBV machinery (analysis.obv), short scope only
    _obv = calculate_obv(m["c"].to_numpy(np.float32), m["ref"].to_numpy(np.float32),
                         m["h"].to_numpy(np.float32), m["l"].to_numpy(np.float32),
                         m["v"].to_numpy(np.float32), **PERIOD_PARAMS["short"])
    m["obv_weak"] = _obv.trend < 0   # short-scope OBV 空頭 latch = 量能轉弱 (持續狀態, 撐過整段下跌)
    m["fresh_low"] = m["noi"] <= m["noi"].rolling(ALERT_LOW_WIN).min() + 1e-9  # 頂部過熱 badge (窄, 2.17x)
    m["hot_wide"] = m["fresh_low"] | m["obv_weak"]   # 加寬弱勢 (外資期貨 OR OBV弱) for stance entry
    # 攻防狀態 (stateful hysteresis): enter 防守 when 加寬過熱(3日內) AND 排列翻空(short_trend<0);
    # exit 防守 when 排列 回多方(short_trend>0) 單日, OR 連續 STANCE_EXIT_DAYS 天回到中性以上
    # (short_trend>=0). OBV widens entry to cover secondary declines.
    m["defensive"] = compute_stance(m["st"].to_numpy(), m["hot_wide"].to_numpy())
    m["mchg5"] = m["mgn"].pct_change(PANIC_MARGIN_WIN) * 100
    m["ret10"] = m["tx"].pct_change(PANIC_SPEED_WIN) * 100
    m["panic"] = ((m["pct_from_high"] <= PANIC_PFH) & (m["mchg5"] <= PANIC_MARGIN_CHG)
                  & (m["ret10"] <= PANIC_SPEED_CHG))   # 深跌 + 融資斷頭急殺 + 快殺

    ma = m["mgn"].rolling(MARGIN_MA_WIN).mean()
    sd = m["mgn"].rolling(MARGIN_MA_WIN).std()
    m["margin_z"] = (m["mgn"] - ma) / sd                        # deviation from 55d trend in σ
    m["margin_hot"] = ((m["margin_z"] + 2) / 4).clip(0, 1) * 100  # −2σ→0, 均線→50, +2σ→100
    # 融資過熱: Bollinger z of 融資餘額/55MA成交金額 (normalize by turnover, then de-trend). z is
    # scale-invariant so 融資/成交金額 unit mismatch is harmless. Independent 2nd top flag.
    _mt = m["mgn"] / m["to"].rolling(MARGIN_MA_WIN).mean()
    m["mt_z"] = (_mt - _mt.rolling(MARGIN_MA_WIN).mean()) / _mt.rolling(MARGIN_MA_WIN).std()
    m["margin_overheat"] = m["mt_z"] >= MARGIN_OVERHEAT_Z
    m["futures_hot"] = (1 - _roll_pctl(m["noi"], FUT_WIN)) * 100   # more net-short -> hotter
    # 定位極端度 = 外資期貨 + 融資 only. P/C 與微台散戶在頂/底皆極端(反指標無方向鑑別力),
    # 只會稀釋分數, 已移除 (see memory project_market_thermometer 2026-07-21).
    m["score"] = (m["margin_hot"] + m["futures_hot"]) / 2
    m = m.dropna(subset=["score"]).reset_index(drop=True)
    if m.empty:
        return {"as_of": None, "score": None, "bucket": None, "components": [], "history": []}

    last = m.iloc[-1]
    hist = m.tail(HISTORY_DAYS)
    components = [
        {"key": "futures", "name": "外資期貨定位", "hot": round(last["futures_hot"], 1),
         "detail": f"外資臺股期貨淨未平倉 {int(last['noi']):+,} 口（{FUT_WIN}日百分位越低越淨空＝越熱）"},
        {"key": "margin", "name": "融資水位", "hot": round(last["margin_hot"], 1),
         "detail": f"融資餘額金額 {last['mgn']/1e5:,.0f}億（距 {MARGIN_MA_WIN} 日均 {last['margin_z']:+.2f}σ；+2σ=過熱）"},
    ]
    def _r1(x):
        return None if pd.isna(x) else round(float(x), 1)

    history = []
    for r in hist.itertuples(index=False):
        lbl, col, _ = _regime(r.score, bool(r.pct_from_high >= -NEAR_HIGH_TOL), bool(r.fresh_low))
        history.append({"date": r.d.date().isoformat(), "score": round(r.score, 1),
                        "tx": round(r.tx), "label": lbl, "color": col,
                        "stance": "防守" if r.defensive else "攻擊",
                        "panic": bool(r.panic),
                        "m_alert": bool(r.margin_overheat),
                        "c": {"futures": _r1(r.futures_hot), "margin": _r1(r.margin_hot)}})

    pfh = last["pct_from_high"]
    a_near = bool(pfh >= -NEAR_HIGH_TOL)
    a_fresh = bool(last["fresh_low"])
    label, color, danger = _regime(last["score"], a_near, a_fresh)
    defensive = bool(last["defensive"])
    return {
        "as_of": last["d"].date().isoformat(),
        "score": round(last["score"], 1),
        "bucket": label,
        "bucket_color": color,
        "danger": danger,
        "stance": "防守" if defensive else "攻擊",
        "stance_color": "#ef4444" if defensive else "#22c55e",
        "stance_reason": (f"頂部過熱後排列翻空、續守中（排列：{ST_LABELS.get(int(last['st']), '?')}，回多方或連續 {STANCE_EXIT_DAYS} 天中性以上才解除）"
                          if defensive else f"排列中性以上（{ST_LABELS.get(int(last['st']), '?')}）"),
        "near_high": a_near,
        "pct_from_high": round(float(pfh), 1),
        "hi_window": HI_WINDOW,
        "alert": a_fresh,
        "alert_conditions": [
            {"name": f"外資淨空創 {ALERT_LOW_WIN} 日新低", "met": a_fresh},
        ],
        "margin_alert": bool(last["margin_overheat"]),
        "margin_alert_conditions": [
            {"name": f"融資/成交量 布林 z ≥ +{MARGIN_OVERHEAT_Z:.1f}σ（現 {last['mt_z']:+.1f}σ）",
             "met": bool(last["margin_overheat"])},
        ],
        "panic": bool(last["panic"]),
        "panic_conditions": [
            {"name": f"深跌（距 {HI_WINDOW} 日高 ≤ {PANIC_PFH:.0f}%）", "met": bool(last["pct_from_high"] <= PANIC_PFH)},
            {"name": f"融資 {PANIC_MARGIN_WIN} 日急殺（≤ {PANIC_MARGIN_CHG:.0f}%）", "met": bool(last["mchg5"] <= PANIC_MARGIN_CHG)},
            {"name": f"快殺（指數 {PANIC_SPEED_WIN} 日跌 ≤ {PANIC_SPEED_CHG:.0f}%）", "met": bool(last["ret10"] <= PANIC_SPEED_CHG)},
        ],
        "components": components,
        "history": history,
    }


def _intraday_short_trend_total() -> tuple[int, str] | None:
    """Live short-scope 排列 trend (total base) from the breadth sidecar that
    intraday_snapshot writes each pass. Returns (TREND_CODE, sidecar_date) or None."""
    import json
    from pathlib import Path
    sidecar = Path(__file__).parent.parent / "data" / "breadth_intraday.json"
    if not sidecar.exists():
        return None
    try:
        with open(sidecar, encoding="utf-8") as f:
            ib = json.load(f)
    except Exception:
        return None
    total = ib.get("total") or ib.get("active")
    if not total:
        return None
    from analysis.market_breadth import classify_trend, TREND_CODE
    up = ib["short_up"] / total * 100
    dn = ib["short_down"] / total * 100
    return TREND_CODE[classify_trend(up, dn, 100 - up - dn)], str(ib.get("trade_date"))


def _tx_forming_bar(volume_scale: float, now: datetime) -> tuple | None:
    """Today's forming 大台 bar (o,h,l,c,v) with h(t)-projected volume, taken as
    the last bar of tx_status.build_tx_data (which fetches the live cnyes TXF
    quote and scales its volume). Returns None if no live bar for today."""
    try:
        from analysis.tx_status import build_tx_data
        res = build_tx_data(intraday=True, volume_scale=volume_scale, now=now)
    except Exception:
        return None
    data = res[0] if isinstance(res, tuple) else res
    if data is None or len(data.dates) == 0 or data.dates[-1] != now.date():
        return None
    return (float(data.open[-1]), float(data.high[-1]), float(data.low[-1]),
            float(data.close[-1]), float(data.volume[-1]))


def build_intraday_stance(cur, volume_scale: float, now: datetime) -> dict | None:
    """Live 攻防 for the current intraday pass. Same history + hysteresis as the
    daily gauge (_load_merged_frame + compute_stance) but with a forming last bar:
    short_trend_total from the breadth sidecar, obv_weak from the daily TXF series
    with today's forming bar grafted on, fresh_low from the last available foreign
    net_oi (stale — only the OR side of entry; obv_weak carries it live)."""
    m = _load_merged_frame(cur)
    if m.empty:
        return None

    today = now.date()
    ts = pd.Timestamp(today)
    sc = _intraday_short_trend_total()
    forming = _tx_forming_bar(volume_scale, now)
    if forming is not None and ts not in set(m["d"]):
        o, h, l, c, v = forming
        row = {col: np.nan for col in m.columns}
        row["d"] = ts
        row["o"], row["h"], row["l"], row["c"], row["v"] = o, h, l, c, v
        row["ref"] = m["c"].iloc[-1]                       # prev front-month close
        row["noi"] = m["noi"].iloc[-1]                     # stale (foreign net_oi is close-only)
        row["st"] = (sc[0] if sc is not None and sc[1] == today.isoformat()
                     else int(m["st"].iloc[-1]))
        m = pd.concat([m, pd.DataFrame([row])], ignore_index=True)

    _obv = calculate_obv(m["c"].to_numpy(np.float32), m["ref"].to_numpy(np.float32),
                         m["h"].to_numpy(np.float32), m["l"].to_numpy(np.float32),
                         m["v"].to_numpy(np.float32), **PERIOD_PARAMS["short"])
    m["obv_weak"] = _obv.trend < 0
    m["fresh_low"] = m["noi"] <= m["noi"].rolling(ALERT_LOW_WIN).min() + 1e-9
    m["hot_wide"] = m["fresh_low"].fillna(False) | m["obv_weak"]
    defensive = compute_stance(m["st"].to_numpy(), m["hot_wide"].to_numpy())

    last = m.iloc[-1]
    is_def = bool(defensive[-1])
    st_last = int(last["st"])
    return {
        "as_of": last["d"].date().isoformat(),
        "snapshot_time": now.isoformat(),
        "is_today": bool(last["d"].date() == today),
        "stance": "防守" if is_def else "攻擊",
        "stance_color": "#ef4444" if is_def else "#22c55e",
        "stance_reason": (f"頂部過熱後排列翻空、續守中（排列：{ST_LABELS.get(st_last, '?')}，"
                          f"回多方或連續 {STANCE_EXIT_DAYS} 天中性以上才解除）"
                          if is_def else f"排列中性以上（{ST_LABELS.get(st_last, '?')}）"),
        "short_trend": st_last,
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
        mg["mgn"] = mg["margin_balance_value"].astype(float); fu["noi"] = fu["net_oi"].astype(float)
        m = (mg[["d", "mgn"]].merge(fu[["d", "noi"]], on="d")
             .sort_values("d").reset_index(drop=True))
        m["mh"] = (((m["mgn"] - m["mgn"].rolling(MARGIN_MA_WIN).mean()) / m["mgn"].rolling(MARGIN_MA_WIN).std() + 2) / 4).clip(0, 1) * 100
        m["fh"] = (1 - _roll_pctl(m["noi"], FUT_WIN)) * 100
        m["s"] = (m["mh"] + m["fh"]) / 2
        m = m.set_index("d")
        print("\nsanity — 崩盤峰當日溫度分數 (vs 全樣本中位):")
        print(f"  全樣本 score 中位={m['s'].median():.0f} p75={m['s'].quantile(.75):.0f}")
        for p in ["2020-01-14", "2021-04-27", "2022-01-04", "2024-07-11", "2026-02-26", "2026-06-22"]:
            i = m.index.searchsorted(pd.Timestamp(p))
            if i < len(m):
                print(f"  {p}: score={m['s'].iloc[i]:.0f}")
