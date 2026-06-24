"""Report layer (M5): metrics table, Chinese text report, matplotlib charts.

Charts use Microsoft JhengHei (same convention as backtest/chart.py).
"""
from __future__ import annotations

import copy
import os

import numpy as np
import pandas as pd

from . import metrics as M
from .backtest_layer import run_backtests
from .data_layer import load_cnn_fear_greed


def compute_metrics(results: dict) -> dict:
    out = {}
    for k, r in results.items():
        if k == "_meta":
            continue
        vals = r["value"]
        units = r["units"]
        out[k] = {
            "label": r["label"],
            "invested": r["invested"],
            "final_value": r["final_value"],
            "final_cash": r["final_cash"],
            "total_return": r["final_value"] / r["invested"] - 1.0 if r["invested"] else 0.0,
            "units": units,
            "avg_cost": r["basis"] / units if units > 0 else float("nan"),
            "xirr": M.xirr(r["cf_dates"], r["cf_amounts"]),
            "max_drawdown": M.max_drawdown(vals),
            "dd_duration_days": M.max_drawdown_duration(vals),
            "worst_1y": M.worst_rolling_return(vals, 252),
        }
    return out


def forward_return_check(results: dict, frame: pd.DataFrame, windows) -> dict:
    """Post-deploy forward returns of 00631L at each Strategy deploy point."""
    dp = results["_meta"]["deploy_points"]
    dates = frame.index
    target = frame["target"].to_numpy(dtype=np.float64)
    pos = {d: i for i, d in enumerate(dates)}
    rows = {w: [] for w in windows}
    for d, px, score, amt, kind in dp:
        i = pos.get(d)
        if i is None:
            continue
        for w in windows:
            j = i + w
            if j < len(target):
                rows[w].append(target[j] / target[i] - 1.0)
    summary = {}
    for w, rs in rows.items():
        if rs:
            arr = np.array(rs)
            summary[w] = {"n": len(arr), "mean": float(arr.mean()),
                          "win": float((arr > 0).mean()), "median": float(np.median(arr))}
        else:
            summary[w] = {"n": 0, "mean": float("nan"), "win": float("nan"), "median": float("nan")}
    return summary


def _pct(x):
    return "n/a" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x*100:.2f}%"


def build_text_report(results, mets, fwd, decay_gap, cfg, frame) -> str:
    meta = results["_meta"]
    n_dp = len(meta["deploy_points"])
    sig_dp = sum(1 for *_, kind in meta["deploy_points"] if kind == "signal")
    L = []
    L.append("=" * 72)
    L.append("00631L 長期被動買進模型 — 回測報告")
    L.append("=" * 72)
    L.append(f"期間: {frame.index[0].date()} ~ {frame.index[-1].date()}  "
             f"交易日 {len(frame)}  投入月數 {meta['n_months']}")
    L.append(f"每月投入 M={cfg['capital']['monthly_input']:.0f}  "
             f"base_ratio={cfg['capital']['base_ratio']}  "
             f"觸發門檻={cfg['signal']['trigger_threshold']}  "
             f"維度權重 技{cfg['signal']['dim_weights']['technical']}/市{cfg['signal']['dim_weights']['market']}")
    L.append(f"加碼次數: {n_dp}（訊號 {sig_dp} / 強制部署 {n_dp - sig_dp}）")
    L.append("")
    L.append("【各組對照績效】（策略須勝 A 純DCA）")
    hdr = f"{'組別':<26}{'總投入':>10}{'期末市值':>12}{'總報酬':>9}{'XIRR':>9}{'平均成本':>10}{'最大回撤':>9}{'最差單年':>9}"
    L.append(hdr)
    L.append("-" * len(hdr))
    order = ["A", "B", "Cp", "Ct", "S"]
    for k in order:
        m = mets[k]
        L.append(f"{m['label']:<26}{m['invested']:>10.0f}{m['final_value']:>12.0f}"
                 f"{_pct(m['total_return']):>9}{_pct(m['xirr']):>9}"
                 f"{m['avg_cost']:>10.2f}{_pct(m['max_drawdown']):>9}{_pct(m['worst_1y']):>9}")
    L.append("")

    # core comparison: Strategy vs A
    s, a = mets["S"], mets["A"]
    L.append("【核心命題：Strategy vs A 純DCA(00631L)】")
    L.append(f"  期末市值差: {s['final_value']-a['final_value']:+.0f}  "
             f"({_pct(s['final_value']/a['final_value']-1)})")
    L.append(f"  平均成本: 策略 {s['avg_cost']:.2f} vs A {a['avg_cost']:.2f}  "
             f"({_pct(s['avg_cost']/a['avg_cost']-1)})")
    L.append(f"  XIRR: 策略 {_pct(s['xirr'])} vs A {_pct(a['xirr'])}")
    L.append(f"  未部署現金池餘額: {s['final_cash']:.0f}")
    L.append("")
    L.append("【持倉風險（槓桿放大）】")
    L.append(f"  Strategy 帳面最大未實現回撤 {_pct(s['max_drawdown'])}  "
             f"水下最長 {s['dd_duration_days']} 交易日  最差單年 {_pct(s['worst_1y'])}")
    L.append(f"  對照 C 純DCA 0050(未槓桿) 最大回撤 {_pct(mets['Cp']['max_drawdown'])}")
    L.append("")
    L.append("【槓桿衰減量測（診斷，非策略報酬）】")
    L.append(f"  0050(1x)×2 累乘理論值 vs 00631L 實際 期末差距: {_pct(decay_gap)}")
    L.append("  （負值＝實際落後理論，即衰減＋費用侵蝕；基準用匹配標的 0050，非大盤）")
    L.append("")
    L.append("【加碼點事後 00631L 報酬】")
    for w, st in fwd.items():
        L.append(f"  +{w}日: n={st['n']}  平均 {_pct(st['mean'])}  "
                 f"中位 {_pct(st['median'])}  勝率 {_pct(st['win'])}")
    L.append("=" * 72)
    return "\n".join(L)


# ── M4 robustness ─────────────────────────────────────────────────────────────

def build_m4_report(frame, signals, cfg) -> str:
    """Parameter sensitivity (reusing precomputed signals) + bear sub-periods."""
    L = ["", "=" * 72, "M4 穩健性檢視", "=" * 72]

    # 1) sensitivity grid — base_ratio × trigger_threshold (signals unaffected)
    L.append("【參數敏感度：Strategy 期末市值 / XIRR（A 純DCA 為標竿）】")
    base_a = compute_metrics(run_backtests(frame, signals, cfg))["A"]
    L.append(f"  標竿 A 純DCA: 期末 {base_a['final_value']:.0f}  XIRR {_pct(base_a['xirr'])}")
    L.append(f"  {'base_ratio v thr':<16}" + "".join(f"{t:>16}" for t in [0.60, 0.70, 0.80]))
    for br in [0.4, 0.6, 0.8]:
        cells = []
        for thr in [0.60, 0.70, 0.80]:
            c = copy.deepcopy(cfg)
            c["capital"]["base_ratio"] = br
            c["signal"]["trigger_threshold"] = thr
            s = compute_metrics(run_backtests(frame, signals, c))["S"]
            vs_a = s["final_value"] / base_a["final_value"] - 1.0
            cells.append(f"{_pct(vs_a):>16}")
        L.append(f"  {br:<16}" + "".join(cells) + "   (vs A 期末)")
    L.append("  解讀：base_ratio 越低＝池越大＝越多現金等加碼；強多頭中留現金普遍拖累。")

    # 2) bear / crisis sub-periods (signals already calibrated on full history)
    L.append("")
    L.append("【空頭/危機子區間：帳面回撤與策略 vs A】")
    for label, s0, s1 in [("2020 COVID", "2020-01-01", "2020-12-31"),
                          ("2022 空頭", "2021-12-01", "2023-06-30"),
                          ("2025 關稅急跌", "2025-01-01", "2025-12-31")]:
        mask = (frame.index >= s0) & (frame.index <= s1)
        if mask.sum() < 20:
            continue
        sub_f = frame.loc[mask]
        sub_s = signals.loc[mask]
        m = compute_metrics(run_backtests(sub_f, sub_s, cfg))
        L.append(f"  {label} ({s0}~{s1}): "
                 f"A 回撤 {_pct(m['A']['max_drawdown'])} / 報酬 {_pct(m['A']['total_return'])}  |  "
                 f"S 回撤 {_pct(m['S']['max_drawdown'])} / 報酬 {_pct(m['S']['total_return'])}  |  "
                 f"C(0050) 回撤 {_pct(m['Cp']['max_drawdown'])}")
    L.append("  解讀：S 與 A 回撤接近（資金多在 00631L），現金池僅小幅緩衝槓桿帳面波動。")
    L.append("=" * 72)
    return "\n".join(L)


BEAR_WINDOWS = [
    ("2020 COVID", "2020-01-01", "2020-12-31"),
    ("2022 空頭", "2021-12-01", "2023-06-30"),
    ("2025 關稅急跌", "2025-01-01", "2025-12-31"),
]


def build_fullrun_bear_report(results, frame) -> str:
    """Slice the FULL-RUN daily equity curve (already accumulated for years) and
    measure what a long-term holder's book actually went through each bear:
    peak→trough drawdown, time to trough, and trading days back to the prior peak.
    Unlike M4's sub-period re-run, capital is NOT reset — this is the on-the-way
    experience of someone who has been accumulating since 2016.
    """
    dates = frame.index
    L = ["", "=" * 72, "全程持倉穿越空頭（切 equity 曲線，本金不重置）", "=" * 72]
    for name, s0, s1 in BEAR_WINDOWS:
        mask = (dates >= s0) & (dates <= s1)
        if mask.sum() < 5:
            continue
        i0 = int(np.argmax(mask))
        i1 = int(len(mask) - 1 - np.argmax(mask[::-1]))
        L.append(f"\n● {name} ({dates[i0].date()} ~ {dates[i1].date()})")
        for k in ["A", "S"]:
            v = results[k]["value"]
            peak = np.maximum.accumulate(v)
            dd = v[i0:i1 + 1] / np.where(peak[i0:i1 + 1] > 0, peak[i0:i1 + 1], np.nan) - 1.0
            trough_rel = int(np.nanargmin(dd))
            trough_i = i0 + trough_rel
            max_dd = float(dd[trough_rel])
            peak_val = peak[trough_i]
            # peak day before trough
            peak_i = int(np.where(v[:trough_i + 1] >= peak_val)[0][-1])
            fall_days = trough_i - peak_i
            # recovery to prior peak (search to end of full series)
            after = np.where(v[trough_i:] >= peak_val)[0]
            if len(after):
                rec = f"{int(after[0])} 交易日後回前高"
            else:
                rec = f"未回前高（截至期末仍水下 {len(v) - 1 - trough_i} 日）"
            L.append(f"   {results[k]['label']:<22} 進場時市值 {v[i0]:>10.0f}  "
                     f"峰→谷回撤 {_pct(max_dd):>9}（{fall_days}日下跌）  谷底 {v[trough_i]:>10.0f}  {rec}")
    L.append("\n  解讀：滿倉穿越時 00631L 帳面回撤遠深於子區間重跑版，且回前高耗時長——")
    L.append("        這才是長期持有者大跌時要承受的真實帳面痛感與槓桿風險。")
    L.append("=" * 72)
    return "\n".join(L)


# ── charts ───────────────────────────────────────────────────────────────────

def _setup_mpl(font):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams["font.sans-serif"] = [font, "SimHei", "Arial"]
    plt.rcParams["axes.unicode_minus"] = False
    return plt


def make_charts(results, signals, frame, cfg, outdir):
    plt = _setup_mpl(cfg["report"]["font"])
    dates = frame.index
    meta = results["_meta"]

    # 1) value curves
    fig, ax = plt.subplots(figsize=(12, 6))
    for k, c in [("A", "tab:gray"), ("B", "tab:olive"), ("Cp", "tab:green"), ("S", "tab:red")]:
        ax.plot(dates, results[k]["value"], label=results[k]["label"], color=c, lw=1.3)
    ax.set_title("累積帳面市值對照（策略 vs 對照組）")
    ax.set_ylabel("帳面市值 (NTD)")
    ax.legend(); ax.grid(alpha=0.3)
    fig.tight_layout(); fig.savefig(os.path.join(outdir, "value_curves.png"), dpi=110); plt.close(fig)

    # 2) deploy points on 00631L + cheapness
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 7), sharex=True,
                                   gridspec_kw={"height_ratios": [2, 1]})
    ax1.plot(dates, frame["target"], color="black", lw=0.9, label="00631L 收盤")
    sdp = [(d, p) for d, p, sc, amt, kind in meta["deploy_points"] if kind == "signal"]
    stp = [(d, p) for d, p, sc, amt, kind in meta["deploy_points"] if kind == "stale"]
    if sdp:
        ax1.scatter([d for d, _ in sdp], [p for _, p in sdp], marker="^", color="red", s=40, label="訊號加碼", zorder=5)
    if stp:
        ax1.scatter([d for d, _ in stp], [p for _, p in stp], marker="v", color="orange", s=30, label="強制部署", zorder=5)
    ax1.set_title("加碼點標記（訊號母體 TAIEX，買進標的 00631L）")
    ax1.set_ylabel("00631L"); ax1.legend(); ax1.grid(alpha=0.3)
    ax2.plot(dates, signals["cheapness"], color="tab:blue", lw=0.9, label="cheapness_score")
    ax2.axhline(cfg["signal"]["trigger_threshold"], color="red", ls="--", lw=0.8, label="觸發門檻")
    ax2.fill_between(dates, 0, 1, where=signals["is_choppy"].to_numpy(), color="gray", alpha=0.2, label="is_choppy")
    ax2.set_ylim(0, 1); ax2.set_ylabel("便宜分數"); ax2.legend(); ax2.grid(alpha=0.3)
    fig.tight_layout(); fig.savefig(os.path.join(outdir, "deploy_points.png"), dpi=110); plt.close(fig)

    # 3) pool level
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.fill_between(dates, 0, meta["pool_series"], color="tab:cyan", alpha=0.6)
    ax.plot(dates, meta["pool_series"], color="tab:blue", lw=0.8)
    ax.set_title("現金池水位"); ax.set_ylabel("池內現金 (NTD)"); ax.grid(alpha=0.3)
    fig.tight_layout(); fig.savefig(os.path.join(outdir, "pool_level.png"), dpi=110); plt.close(fig)

    # 4) leverage decay + book drawdown
    theo, actual, _ = M.leverage_decay_gap(
        frame["c_close"].to_numpy(dtype=np.float64), frame["target"].to_numpy(dtype=np.float64))
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 7), sharex=True,
                                   gridspec_kw={"height_ratios": [2, 1]})
    ax1.plot(dates, actual, color="black", lw=1.0, label="00631L 實際(歸一)")
    ax1.plot(dates, theo, color="tab:red", lw=1.0, ls="--", label="指數×2 累乘理論值")
    ax1.set_title("槓桿衰減：0050×2 理論 vs 00631L 實際（診斷用）")
    ax1.set_ylabel("歸一化"); ax1.legend(); ax1.grid(alpha=0.3)
    sv = results["S"]["value"]
    peak = np.maximum.accumulate(sv)
    dd = sv / np.where(peak > 0, peak, np.nan) - 1.0
    ax2.fill_between(dates, dd, 0, color="tab:red", alpha=0.4)
    ax2.set_title("Strategy 帳面回撤"); ax2.set_ylabel("回撤"); ax2.grid(alpha=0.3)
    fig.tight_layout(); fig.savefig(os.path.join(outdir, "leverage_decay.png"), dpi=110); plt.close(fig)

    # 5) self-built TW fear/greed (CNN convention: low=fear) + CNN overlay
    greed = (1.0 - signals["market"].to_numpy()) * 100.0
    fig, ax = plt.subplots(figsize=(12, 4.5))
    ax.plot(dates, greed, color="tab:purple", lw=0.9, label="自製台股恐懼貪婪(低=恐懼)")
    if cfg["report"].get("cnn_overlay", True):
        cnn = load_cnn_fear_greed(str(dates[0].date()), str(dates[-1].date()))
        if not cnn.empty:
            ax.plot(cnn.index, cnn.values, color="tab:orange", lw=1.1, label="CNN F&G(近1年,美股)")
    ax.axhline(25, color="green", ls=":", lw=0.8); ax.axhline(75, color="red", ls=":", lw=0.8)
    ax.set_ylim(0, 100); ax.set_title("市場情緒：自製台股 vs CNN 對照")
    ax.set_ylabel("0=極度恐懼 100=極度貪婪"); ax.legend(); ax.grid(alpha=0.3)
    fig.tight_layout(); fig.savefig(os.path.join(outdir, "sentiment.png"), dpi=110); plt.close(fig)

    # 6) full-run equity sliced through the 2022 bear (capital NOT reset)
    s0, s1 = "2021-12-01", "2023-06-30"
    mask = (dates >= s0) & (dates <= s1)
    if mask.sum() >= 5:
        sub_d = dates[mask]
        fig, ax = plt.subplots(figsize=(12, 5))
        for k, c in [("A", "tab:gray"), ("S", "tab:red")]:
            v = results[k]["value"]
            ax.plot(sub_d, v[mask], color=c, lw=1.3, label=results[k]["label"])
            peak = np.maximum.accumulate(v)[mask]
            ax.plot(sub_d, peak, color=c, lw=0.7, ls=":", alpha=0.7)
        ax.set_title("全程持倉穿越 2022 空頭（本金不重置；虛線=波段前高）")
        ax.set_ylabel("帳面市值 (NTD)"); ax.legend(); ax.grid(alpha=0.3)
        fig.tight_layout(); fig.savefig(os.path.join(outdir, "bear_2022_fullrun.png"), dpi=110); plt.close(fig)
