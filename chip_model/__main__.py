"""python -m chip_model run — M2->M3->M4 串接：產 picks CSV + 回測報告。"""
import sys

from chip_model.backtest import run_backtest
from chip_model.db_access import load_common_universe, shareholder_dates
from chip_model.metrics import compute_metrics
from chip_model.strategy import Rule, generate_signals, pick_for_date


def _fmt_pct(v):
    return f"{'n/a':>10}" if v is None else f"{v * 100:+9.2f}%"


def _print_report(rule, latest, picks, signals, bt, csv_path):
    s = bt["summary"]
    print("=" * 64)
    print("集保大戶選股模型 (chip_model)")
    print("=" * 64)
    print(f"規則：每週取「籌碼集中度」前 {rule.top_n} 名"
          f"（大戶增+散戶減+千張人數增，4週共識排名，橫斷面普通股）")
    print(f"最新快照週：{latest}　選出 {len(picks)} 檔　→ {csv_path}")
    if not picks.empty:
        head = picks.head(rule.top_n)
        for r in head.itertuples(index=False):
            print(f"  {r.stock_id:<6} 大戶增={r.d_big:+5.2f}  散戶減={r.d_retail:+5.2f}  "
                  f"千張人數增={int(r.d_holders):+5d}  比例={r.ratio:5.2f}%")
    print("-" * 64)
    print(f"回測：訊號週 {s['n_signal_weeks']} 週　訊號數 {s['n_signals']}　"
          f"成交筆數 {s['n_trades']}")
    cols = ["區間", "筆數", "勝率", "平均", "中位",
            "超額", "最大虧損", "最大獲利", "最大回撤"]
    print(f"{cols[0]:<6}{cols[1]:>6}{cols[2]:>9}" + "".join(f"{c:>10}" for c in cols[3:]))
    for h, d in s["horizons"].items():
        if d["n"] == 0:
            print(f"{f'{h}週':<6}{0:>6}      （無可評估樣本）")
            continue
        print(f"{f'{h}週':<6}{d['n']:>6}{d['win_rate'] * 100:>8.1f}%"
              f"{_fmt_pct(d['avg_ret'])}{_fmt_pct(d['median_ret'])}"
              f"{_fmt_pct(d['avg_excess'])}{_fmt_pct(d['min_ret'])}"
              f"{_fmt_pct(d['max_ret'])}{_fmt_pct(d['max_drawdown'])}")
    print("-" * 64)
    print("[注意] 集保歷史僅約 14 個月（~59 週），扣前視窗後可評估樣本偏小，"
          "回測結論信心度低，僅供方向參考。")
    print("=" * 64)


def run():
    rule = Rule()
    metrics = compute_metrics()
    universe = load_common_universe()
    signals = generate_signals(metrics, rule, universe)

    dates = shareholder_dates()
    latest = dates[-1]
    picks = pick_for_date(signals, latest)
    csv_path = f"data/picks_{latest:%Y%m%d}.csv"
    picks.to_csv(csv_path, index=False, encoding="utf-8-sig")

    bt = run_backtest(signals)
    _print_report(rule, latest, picks, signals, bt, csv_path)


def main(argv):
    if len(argv) >= 1 and argv[0] == "run":
        run()
        return 0
    print("usage: python -m chip_model run")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
