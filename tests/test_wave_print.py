"""
Wave test for 2330 — prints wave detection results for manual comparison.

Outputs:
  1. Wave list (all detected waves with tip, direction, length, waterfall/ditch)
  2. Day-indexed results (last 20 days of direction, tips, prices, signals)
"""

import sys
import os

# Ensure project root is on path for exe
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
from db.connection import get_cursor
from analysis.close import calculate_close
from analysis.candle import calculate_candle
from analysis.wave import calculate_wave

STOCK_ID = "2330"


def fetch_data() -> dict:
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT trade_date, open_price, high_price, low_price,
                   close_price, volume, turnover
            FROM tw.daily_prices
            WHERE stock_id = %s
              AND close_price IS NOT NULL
            ORDER BY trade_date ASC
            """,
            (STOCK_ID,),
        )
        rows = cur.fetchall()

    dates = [r["trade_date"] for r in rows]
    close = np.array([float(r["close_price"]) for r in rows], dtype=np.float32)
    high = np.array([float(r["high_price"]) for r in rows], dtype=np.float32)
    low = np.array([float(r["low_price"]) for r in rows], dtype=np.float32)
    open_ = np.array([float(r["open_price"]) for r in rows], dtype=np.float32)
    volume = np.array([float(r["volume"]) for r in rows], dtype=np.float32)

    return {
        "dates": dates,
        "close": close,
        "high": high,
        "low": low,
        "open": open_,
        "volume": volume,
    }


def print_header(title: str):
    print()
    print("=" * 90)
    print(f"  {title}")
    print("=" * 90)


def main():
    data = fetch_data()
    n = len(data["dates"])

    print(f"Stock: {STOCK_ID}")
    print(f"Data:  {data['dates'][0]} ~ {data['dates'][-1]} ({n} days)")
    print(f"Close: {data['close'].min():.2f} ~ {data['close'].max():.2f}")

    # Calculate dependencies
    close_result = calculate_close(data["close"])
    candle_result = calculate_candle(data["open"], data["high"], data["low"], data["close"])
    wave_result = calculate_wave(
        data["open"], data["high"], data["low"], data["close"],
        candle_result, close_result.bs,
        volume=data["volume"],
    )

    W = wave_result.waves
    wc = W.count()

    # ── 1. Wave List ────────────────────────────────────────────────────
    print_header(f"Wave List ({wc} waves)")
    print(f"{'#':>4s}  {'Dir':>4s}  {'Tip':>10s}  "
          f"{'Length':>10s}  {'UpPrice':>10s}  {'MidPrice':>10s}  {'DnPrice':>10s}  "
          f"{'WF':>3s}  {'WD':>3s}  {'Day':>5s}  {'DayIdx':>6s}  "
          f"{'AvgVol':>12s}")
    print("-" * 140)

    for i in range(wc):
        d = "UP" if W.wave[i] else "DN"
        wf = "Y" if W.waterfall[i] else ""
        wd = "Y" if W.water_ditch[i] else ""
        day_str = f"{W.day_idx[i]}"
        date_str = str(data["dates"][W.day_idx[i]]) if W.day_idx[i] < n else "?"

        print(f"{i:4d}  {d:>4s}  {W.tip[i]:10.2f}  "
              f"{W.length[i]:10.2f}  {W.up_price[i]:10.2f}  {W.mid_price[i]:10.2f}  "
              f"{W.down_price[i]:10.2f}  {wf:>3s}  {wd:>3s}  {W.day[i]:5d}  "
              f"{date_str:>10s}  "
              f"{W.avg_volume[i]:12.0f}")

    # ── 2. Day-indexed results (last 20 days) ──────────────────────────
    show_days = min(20, n)
    start = n - show_days

    print_header(f"Day Results (last {show_days} days)")
    print(f"{'Date':>12s}  {'Close':>8s}  {'Dir':>4s}  "
          f"{'Tip0':>10s}  {'Tip1':>10s}  {'Tip2':>10s}  "
          f"{'UpPx0':>10s}  {'MidPx0':>10s}  {'DnPx0':>10s}  "
          f"{'C>D2MA':>6s}  {'Sink':>5s}")
    print("-" * 120)

    for i in range(start, n):
        d = "UP" if wave_result.direction[i] else "DN"
        cross = "Y" if wave_result.close_cross_wave_d2ma[i] else ""
        sk = "Y" if wave_result.sink[i] else ""

        print(f"{str(data['dates'][i]):>12s}  {data['close'][i]:8.2f}  {d:>4s}  "
              f"{wave_result.tip0[i]:10.2f}  {wave_result.tip1[i]:10.2f}  "
              f"{wave_result.tip2[i]:10.2f}  "
              f"{wave_result.up_price0[i]:10.2f}  {wave_result.mid_price0[i]:10.2f}  "
              f"{wave_result.down_price0[i]:10.2f}  "
              f"{cross:>6s}  {sk:>5s}")

    # ── 3. Waterfall / Ditch signals (last 20 days) ────────────────────
    print_header(f"Waterfall / Ditch Signals (last {show_days} days)")
    print(f"{'Date':>12s}  {'Close':>8s}  "
          f"{'RWF0':>5s}  {'BWF0':>5s}  {'RWF1':>5s}  {'BWF1':>5s}  "
          f"{'RWD1':>5s}  {'BWD1':>5s}  "
          f"{'BWF_Up':>10s}  {'RWF_Dn':>10s}  "
          f"{'BrkBWF':>6s}  {'BrkRWF':>6s}")
    print("-" * 120)

    for i in range(start, n):
        def flag(b):
            return "Y" if b else ""

        print(f"{str(data['dates'][i]):>12s}  {data['close'][i]:8.2f}  "
              f"{flag(wave_result.red_waterfall0[i]):>5s}  "
              f"{flag(wave_result.black_waterfall0[i]):>5s}  "
              f"{flag(wave_result.red_waterfall1[i]):>5s}  "
              f"{flag(wave_result.black_waterfall1[i]):>5s}  "
              f"{flag(wave_result.red_sizable_wave1[i]):>5s}  "
              f"{flag(wave_result.black_sizable_wave1[i]):>5s}  "
              f"{wave_result.black_wf_up_price[i]:10.2f}  "
              f"{wave_result.red_wf_down_price[i]:10.2f}  "
              f"{flag(wave_result.close_break_black_wf_up[i]):>6s}  "
              f"{flag(wave_result.close_break_red_wf_down[i]):>6s}")

    # ── 4. Convex / Concave conditions (last 20 days) ──────────────────
    print_header(f"Convex / Concave Conditions (last {show_days} days)")
    print(f"{'Date':>12s}  {'Close':>8s}  "
          f"{'CxI':>4s}  {'CxII':>5s}  {'CxIII':>6s}  {'CxIV':>5s}  "
          f"{'CvI':>4s}  {'CvII':>5s}  {'CvIII':>6s}  {'CvIV':>5s}")
    print("-" * 90)

    for i in range(start, n):
        def flag(b):
            return "Y" if b else ""

        print(f"{str(data['dates'][i]):>12s}  {data['close'][i]:8.2f}  "
              f"{flag(wave_result.convex_i[i]):>4s}  "
              f"{flag(wave_result.convex_ii[i]):>5s}  "
              f"{flag(wave_result.convex_iii[i]):>6s}  "
              f"{flag(wave_result.convex_iv[i]):>5s}  "
              f"{flag(wave_result.concave_i[i]):>4s}  "
              f"{flag(wave_result.concave_ii[i]):>5s}  "
              f"{flag(wave_result.concave_iii[i]):>6s}  "
              f"{flag(wave_result.concave_iv[i]):>5s}")

    # ── 5. Wave Trend (last 20 days) ─────────────────────────────────
    wt = wave_result.wave_trend
    print_header(f"Wave Trend (last {show_days} days)")
    print(f"{'Date':>12s}  {'Close':>8s}  "
          f"{'Short':>7s}  {'Medium':>7s}  {'Long':>7s}  {'Comp':>7s}")
    print("-" * 60)

    for i in range(start, n):
        print(f"{str(data['dates'][i]):>12s}  {data['close'][i]:8.2f}  "
              f"{wt.short[i]:>+7.3f}  {wt.medium[i]:>+7.3f}  "
              f"{wt.long[i]:>+7.3f}  {wt.composite[i]:>+7.3f}")

    print()
    print(f"Total waves: {wc} (UP: {sum(W.wave)}, DN: {wc - sum(W.wave)})")
    print(f"Waterfalls:  {sum(W.waterfall)}")
    print(f"Ditches:     {sum(W.water_ditch)}")

    input("\nPress Enter to exit...")


if __name__ == "__main__":
    main()
