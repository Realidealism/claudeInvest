"""Generate the figures for the micro-Taiex strategy rulebook (Word export).

Five figures, all drawn white-background / print-friendly at 200 dpi:
  fig1_dataflow      system data flow, tick -> broker
  fig2_onbar         per-5m-bar decision order (engine.on_bar)
  fig3_patterns      the three live entry patterns as candlestick sketches
  fig4_stops         Chandelier ratchet vs per-trade cap, tighter one wins
  fig5_sessions      trading sessions and force-close points

Taiwan convention: red = up bar, green = down bar.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

plt.rcParams["font.sans-serif"] = ["Microsoft JhengHei", "Microsoft YaHei", "SimHei"]
plt.rcParams["axes.unicode_minus"] = False

OUT = Path(__file__).resolve().parent / "doc_figs"
OUT.mkdir(exist_ok=True)

UP, DOWN = "#d62728", "#2ca02c"      # 紅漲 綠跌
BOX_MAIN, BOX_SIDE, BOX_STOP = "#dbeafe", "#f1f5f9", "#fee2e2"
EDGE = "#334155"


def _box(ax, x, y, w, h, text, fc=BOX_MAIN, fs=10, bold=False):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.08",
                                fc=fc, ec=EDGE, lw=1.2))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=fs,
            fontweight="bold" if bold else "normal", linespacing=1.5)


def _arrow(ax, p1, p2, text=None, fs=9, color=EDGE, style="-|>"):
    ax.add_patch(FancyArrowPatch(p1, p2, arrowstyle=style, mutation_scale=14,
                                 lw=1.2, color=color, shrinkA=2, shrinkB=2))
    if text:
        ax.text((p1[0] + p2[0]) / 2 + 0.08, (p1[1] + p2[1]) / 2, text, fontsize=fs,
                color=color, ha="left", va="center")


# ---------------------------------------------------------------- fig 1
def fig_dataflow():
    fig, ax = plt.subplots(figsize=(11.6, 3.6))
    ax.set_xlim(0, 11.6); ax.set_ylim(0, 3.6); ax.axis("off")
    y, h, w = 1.9, 1.0, 1.68
    xs = [0.15, 2.08, 4.01, 5.94, 7.87, 9.77]
    labels = ["群益 SKCOM\n即時 tick", "BarAggregator\n聚合 1m / 5m",
              "CompositeStrategy\n三訊號評估", "RiskManager\n停損 / 部位閘",
              "部位狀態機\n單口 PositionSM", "SimBroker\n紙上成交"]
    fcs = [BOX_SIDE, BOX_SIDE, BOX_MAIN, BOX_STOP, BOX_MAIN, BOX_SIDE]
    for x, lb, fc in zip(xs, labels, fcs):
        _box(ax, x, y, w, h, lb, fc=fc, fs=9.2)
    for i in range(5):
        _arrow(ax, (xs[i] + w, y + h / 2), (xs[i + 1], y + h / 2))
    _box(ax, 2.08, 0.35, 1.68, 0.8, "1m 明細棒\n只供強制平倉", fc="#fef9c3", fs=8.8)
    _arrow(ax, (2.92, y), (2.92, 1.15))
    _box(ax, 5.94, 0.35, 3.61, 0.8, "成交回報 → RoundTrip\n→ CSV / Telegram 推播",
          fc=BOX_SIDE, fs=9.2)
    _arrow(ax, (10.61, y), (10.61, 0.75))
    _arrow(ax, (10.61, 0.75), (9.55, 0.75))
    ax.text(5.8, 3.35, "圖 1　系統資料流（每根 5m 收盤驅動一次完整決策）",
            ha="center", fontsize=12, fontweight="bold")
    fig.savefig(OUT / "fig1_dataflow.png", dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)


# ---------------------------------------------------------------- fig 2
def fig_onbar():
    fig, ax = plt.subplots(figsize=(8.4, 11.1))
    ax.set_xlim(0, 8.4); ax.set_ylim(0, 11.1); ax.axis("off")
    cx, w, h = 1.5, 4.3, 0.72
    steps = [
        (9.55, "收到一根 K 棒", BOX_SIDE),
        (8.45, "① 是 5m 棒嗎？", BOX_MAIN),
        (7.35, "② 成交量 > 0 嗎？", BOX_MAIN),
        (6.25, "③ 計算 ATR(21)　Wilder RMA", BOX_SIDE),
        (5.15, "④ 停損檢查 check_stop\n(cap 線 / Chandelier 線)", BOX_STOP),
        (4.05, "⑤ 收盤前 1 分鐘？(強制平倉)", BOX_STOP),
        (2.95, "⑥ 三訊號評估 → 多方訊號？", BOX_MAIN),
        (1.85, "⑦ 部位閘 evaluate_signal", BOX_MAIN),
        (0.75, "⑧ 送出進場單（1 口）", "#dcfce7"),
    ]
    for y, txt, fc in steps:
        _box(ax, cx, y, w, h, txt, fc=fc, fs=10)
    for i in range(len(steps) - 1):
        _arrow(ax, (cx + w / 2, steps[i][0]), (cx + w / 2, steps[i + 1][0] + h))
    outs = [(8.45, "否 → 只檢查強平後結束"), (7.35, "否 → 只檢查強平後結束"),
            (5.15, "觸發 → 平倉，本根結束"), (4.05, "是 → 平倉，不再看訊號"),
            (2.95, "無訊號 → 結束"), (1.85, "已持多單 → 不動作")]
    for y, txt in outs:
        _arrow(ax, (cx + w, y + h / 2), (cx + w + 0.45, y + h / 2), color="#94a3b8")
        ax.text(cx + w + 0.55, y + h / 2, txt, fontsize=8.6, va="center", color="#475569")
    ax.text(4.2, 10.75, "圖 2　每根 K 棒的決策順序（engine.on_bar）",
            ha="center", fontsize=12, fontweight="bold")
    ax.text(4.2, 0.35, "順序不可調換：停損永遠先於訊號，強平永遠先於進場",
            ha="center", fontsize=9.5, style="italic", color="#b91c1c")
    fig.savefig(OUT / "fig2_onbar.png", dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)


# ---------------------------------------------------------------- fig 3
def _candles(ax, data, width=0.6):
    for i, (o, hi, lo, c) in enumerate(data):
        col = UP if c >= o else DOWN
        ax.vlines(i, lo, hi, color=col, lw=1.4)
        ax.add_patch(plt.Rectangle((i - width / 2, min(o, c)), width, abs(c - o) or 0.06,
                                   fc=col, ec=col))


def fig_patterns():
    fig, axes = plt.subplots(1, 3, figsize=(13.5, 4.5))

    # --- pick: bullish engulfing that breaks the 20-bar low ---
    ax = axes[0]
    base = [(10, 10.6, 9.5, 9.8), (9.8, 10.0, 9.0, 9.2), (9.2, 9.5, 8.6, 8.8),
            (8.8, 9.0, 8.2, 8.4), (8.4, 8.6, 7.9, 8.1)]
    prev = (8.1, 8.2, 7.0, 7.2)          # 黑K
    curr = (7.1, 8.6, 6.9, 8.45)         # 紅K，吞噬且破底
    _candles(ax, base + [prev, curr])
    ax.axhline(7.9, ls="--", lw=1.1, color="#64748b")
    ax.text(0.05, 7.95, "前 20 根最低", fontsize=8.5, color="#64748b")
    ax.annotate("", xy=(6, 8.45), xytext=(6, 7.1),
                arrowprops=dict(arrowstyle="<->", color="#b91c1c", lw=1.3))
    ax.text(6.25, 7.75, "紅K完全\n吞噬黑K", fontsize=8.5, color="#b91c1c")
    ax.set_title("pick　反轉做多（吞噬 + 破底）", fontsize=11, fontweight="bold")

    # --- buy: donchian breakout with ATR margin ---
    ax = axes[1]
    d = [(8.0, 8.5, 7.8, 8.3), (8.3, 8.8, 8.1, 8.4), (8.4, 8.9, 8.2, 8.6),
         (8.6, 8.95, 8.3, 8.5), (8.5, 8.9, 8.2, 8.7), (8.7, 9.0, 8.5, 8.8),
         (8.8, 9.85, 8.7, 9.75)]
    _candles(ax, d)
    ax.axhline(9.0, ls="--", lw=1.1, color="#64748b")
    ax.text(0.05, 9.05, "前 20 根最高", fontsize=8.5, color="#64748b")
    ax.axhline(9.45, ls="-.", lw=1.2, color="#b91c1c")
    ax.text(0.05, 9.5, "前高 + 0.75 × ATR(21)", fontsize=8.5, color="#b91c1c")
    ax.annotate("", xy=(6.55, 9.75), xytext=(6.55, 9.0),
                arrowprops=dict(arrowstyle="<->", color="#b91c1c", lw=1.3))
    ax.text(6.7, 9.3, "須站上\n這條才算", fontsize=8.5, color="#b91c1c")
    ax.set_xlim(-0.7, 8.2)
    ax.set_title("buy　順勢做多（突破 + 幅度確認）", fontsize=11, fontweight="bold")

    # --- sell_flee: bear trap ---
    ax = axes[2]
    d = [(9.4, 9.6, 9.1, 9.2), (9.2, 9.3, 8.8, 8.9), (8.9, 9.0, 8.5, 8.6),
         (8.6, 8.8, 8.3, 8.5), (8.5, 8.7, 8.2, 8.35), (8.35, 8.5, 8.1, 8.2),
         (8.2, 8.75, 7.5, 8.62)]
    _candles(ax, d)
    ax.axhline(8.1, ls="--", lw=1.1, color="#64748b")
    ax.text(0.05, 7.98, "前 10 根最低", fontsize=8.5, color="#64748b")
    ax.axhline(8.2, ls=":", lw=1.3, color="#0369a1")
    ax.text(3.4, 8.28, "前一根收盤（收盤須高於它）", fontsize=8.5, color="#0369a1")
    ax.add_patch(plt.Rectangle((5.6, 8.25), 0.8, 0.5, fc="#fde68a", alpha=.55, ec="none"))
    ax.text(6.55, 8.58, "收盤須落在\n當根區間上 40%", fontsize=8.5, color="#92400e")
    ax.annotate("假跌破", xy=(6, 7.5), xytext=(4.6, 7.35), fontsize=9, color="#b91c1c",
                arrowprops=dict(arrowstyle="->", color="#b91c1c", lw=1.2))
    ax.set_xlim(-0.7, 8.6)
    ax.set_title("sell_flee　空頭陷阱反轉做多", fontsize=11, fontweight="bold")

    for ax in axes:
        ax.set_xticks([]); ax.set_yticks([])
        for s in ax.spines.values():
            s.set_color("#cbd5e1")
    fig.suptitle("圖 3　現行三個進場型態（紅 = 收漲，綠 = 收跌）", fontsize=12.5,
                 fontweight="bold", y=1.02)
    fig.savefig(OUT / "fig3_patterns.png", dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)


# ---------------------------------------------------------------- fig 4
def fig_stops():
    """Realistic magnitudes: entry 46000, ATR21=110.
    cap  = clamp(0.5*ATR, 12, 0.087%*46000) = clamp(55, 12, 40) = 40 pts
    chand= 3.0*ATR = 330 pts  -> the cap is the binding stop almost all the time,
    and Chandelier only takes over after a ~290 pt run.
    """
    fig, ax = plt.subplots(figsize=(11.5, 5.6))
    entry, atr = 46000.0, 110.0
    cap, dist = 40.0, 3.0 * atr
    high = [46000, 46060, 46120, 46090, 46180, 46260, 46230, 46340,
            46420, 46380, 46450, 46390, 46200, 46020]
    close = [h - 25 for h in high]
    x = list(range(len(high)))
    chand, run, out = [], -1e9, []
    for h_ in high:
        run = max(run, h_)
        raw = run - dist
        out.append(raw if not out else max(out[-1], raw))
    chand = out
    cap_line = [entry - cap] * len(x)
    eff = [max(a, b) for a, b in zip(chand, cap_line)]

    ax.plot(x, close, "-o", color="#0f172a", lw=2.0, ms=4.5, label="5m 收盤價", zorder=4)
    ax.plot(x, cap_line, "--", color="#b91c1c", lw=1.9,
            label="per-trade cap 線 = 進場價 - 40（錨定進場價，全程不動）")
    ax.plot(x, chand, "-", color="#2563eb", lw=1.9,
            label="Chandelier = 波段最高 - 3.0×ATR（只緊不鬆）")
    ax.plot(x, eff, ":", color="#f59e0b", lw=3.4,
            label="實際生效防守 = 兩者取較緊（較高）者")
    ax.scatter([0], [entry], marker="^", s=170, color="#16a34a", zorder=6,
               label="進場（做多 1 口）")
    cross = next(i for i in range(len(x)) if chand[i] > cap_line[i])
    ax.axvline(cross, color="#94a3b8", ls="-.", lw=1.2)
    ax.annotate("這段由 cap 守（只容忍 40 點）\nChandelier 在 330 點外，根本碰不到",
                xy=(2, cap_line[0]), xytext=(0.15, 45700), fontsize=9.5, color="#b91c1c",
                arrowprops=dict(arrowstyle="->", color="#b91c1c"))
    ax.annotate("漲約 290 點後 Chandelier 反超\n→ 換它守，且只會往上棘輪",
                xy=(cross + 1.2, chand[cross + 1]), xytext=(5.0, 45690), fontsize=9.5,
                color="#2563eb", arrowprops=dict(arrowstyle="->", color="#2563eb"))
    ax.scatter([13], [close[13]], marker="v", s=170, color="#b91c1c", zorder=6)
    ax.annotate("收盤跌破生效線 → 出場", xy=(13, close[13]), xytext=(10.1, 45880),
                fontsize=9.5, color="#b91c1c",
                arrowprops=dict(arrowstyle="->", color="#b91c1c"))
    ax.set_xlabel("5m K 棒序"); ax.set_ylabel("指數點位")
    ax.set_ylim(45620, 46520)
    ax.set_title("圖 4　兩道停損並行，實際生效的是較緊的那條（entry 46000、ATR21 約 110）",
                 fontsize=12.5, fontweight="bold")
    ax.legend(loc="upper left", fontsize=8.8, framealpha=.95)
    ax.grid(alpha=.25)
    fig.savefig(OUT / "fig4_stops.png", dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)


# ---------------------------------------------------------------- fig 5
def fig_sessions():
    fig, ax = plt.subplots(figsize=(12, 3.2))
    ax.set_xlim(0, 24); ax.set_ylim(0, 3.2); ax.axis("off")
    ax.add_patch(plt.Rectangle((8.75, 1.5), 5.0, 0.62, fc="#bfdbfe", ec=EDGE))
    ax.text(11.25, 1.81, "日盤　08:45 – 13:45", ha="center", va="center", fontsize=10.5, fontweight="bold")
    ax.add_patch(plt.Rectangle((15.0, 1.5), 9.0, 0.62, fc="#c7d2fe", ec=EDGE))
    ax.text(19.5, 1.81, "夜盤　15:00 – 次日 05:00", ha="center", va="center", fontsize=10.5, fontweight="bold")
    ax.add_patch(plt.Rectangle((0, 1.5), 5.0, 0.62, fc="#c7d2fe", ec=EDGE, hatch="//"))
    ax.text(2.5, 1.81, "（前一日夜盤延續）", ha="center", va="center", fontsize=9.5)
    for xpos, lab in [(13.75, "13:44\n強制平倉"), (4.99, "04:59\n強制平倉")]:
        ax.plot([xpos, xpos], [1.35, 2.3], color="#b91c1c", lw=2.2)
        ax.text(xpos, 2.42, lab, ha="center", fontsize=9, color="#b91c1c", fontweight="bold")
    for h in range(0, 25, 2):
        ax.plot([h, h], [1.38, 1.5], color="#94a3b8", lw=1)
        ax.text(h, 1.16, f"{h:02d}:00", ha="center", fontsize=8, color="#475569")
    ax.text(6.9, 1.81, "無交易", ha="center", va="center", fontsize=9.5, color="#64748b")
    ax.text(12, 2.95, "圖 5　交易時段與強制平倉點（部位絕不跨越 session）",
            ha="center", fontsize=12.5, fontweight="bold")
    ax.text(12, 0.62, "無交易時段：05:00–08:45、13:45–15:00　│　週六、週日不交易",
            ha="center", fontsize=9.5, color="#475569")
    ax.text(12, 0.24, "夜盤 00:00–05:00 尾段屬於前一個平日晚上",
            ha="center", fontsize=9.5, style="italic", color="#475569")
    fig.savefig(OUT / "fig5_sessions.png", dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)


if __name__ == "__main__":
    fig_dataflow(); fig_onbar(); fig_patterns(); fig_stops(); fig_sessions()
    for p in sorted(OUT.glob("*.png")):
        print(p.name, f"{p.stat().st_size / 1024:.0f} KB")
