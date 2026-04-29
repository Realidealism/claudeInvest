"""Signal-factory version history — git-like tracking via SQLite.

Each adjustment to the six condition factories should be recorded as a
new version row, so we can diff PF / win-rate / max drawdown across
iterations without bloating the markdown memory.

Storage: data/signal_versions.db
Schema:
    versions(version_id, date, summary, snapshot_path, cost_pct)
    metrics(version_id, signal, side, n_trades, win_pct, avg_raw,
            avg_net, median, cum, max_win, max_loss, win_avg, loss_avg,
            pf_raw, pf_net, mdd_raw, mdd_net, avg_hold)

Usage:
    python -m signal_backtest._versions add v1 "加洪量 DefenseRule"
    python -m signal_backtest._versions list
    python -m signal_backtest._versions show v1
    python -m signal_backtest._versions diff v0 v1
"""

from __future__ import annotations

import argparse
import io
import sqlite3
import sys
from datetime import date as date_t
from pathlib import Path

import pandas as pd

from signal_backtest._compare import compute_stats, SIGNALS, COST_PCT

# UTF-8 on Windows console
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")


DB_PATH = Path("data/signal_versions.db")
DEFAULT_SCAN_DIR = Path("tmp/sb_compare")

SCHEMA = """
CREATE TABLE IF NOT EXISTS versions (
    version_id    TEXT PRIMARY KEY,
    date          TEXT NOT NULL,
    summary       TEXT,
    snapshot_path TEXT,
    cost_pct      REAL DEFAULT 0.004
);

CREATE TABLE IF NOT EXISTS metrics (
    version_id  TEXT NOT NULL,
    signal      TEXT NOT NULL,
    side        TEXT NOT NULL,
    n_trades    INTEGER,
    win_pct     REAL,
    avg_raw     REAL,
    avg_net     REAL,
    median      REAL,
    cum         REAL,
    max_win     REAL,
    max_loss    REAL,
    win_avg     REAL,
    loss_avg    REAL,
    pf_raw      REAL,
    pf_net      REAL,
    mdd_raw     REAL,
    mdd_net     REAL,
    avg_hold    REAL,
    PRIMARY KEY (version_id, signal),
    FOREIGN KEY (version_id) REFERENCES versions(version_id) ON DELETE CASCADE
);
"""


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


# ── Commands ─────────────────────────────────────────────────────────────────


def cmd_add(args: argparse.Namespace) -> None:
    """Read trades.parquet from each signal's dir and insert one version."""
    scan_dir = Path(args.scan_dir or DEFAULT_SCAN_DIR)
    if not scan_dir.exists():
        print(f"目錄不存在：{scan_dir}", file=sys.stderr)
        sys.exit(1)

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM versions WHERE version_id = ?", (args.version_id,))
        if cur.fetchone():
            print(f"版本 {args.version_id} 已存在，請先 delete 或換 id", file=sys.stderr)
            sys.exit(1)

        cur.execute(
            "INSERT INTO versions (version_id, date, summary, snapshot_path, cost_pct) "
            "VALUES (?, ?, ?, ?, ?)",
            (args.version_id, str(date_t.today()), args.summary,
             args.snapshot or str(scan_dir), COST_PCT),
        )

        rows_inserted = 0
        for key, label, side in SIGNALS:
            path = scan_dir / key / "trades.parquet"
            if not path.exists():
                print(f"  缺檔，跳過：{path}", file=sys.stderr)
                continue
            df = pd.read_parquet(path)
            s = compute_stats(df)
            if s is None:
                print(f"  {label}：no trades，跳過", file=sys.stderr)
                continue
            cur.execute(
                """
                INSERT INTO metrics
                  (version_id, signal, side, n_trades, win_pct, avg_raw,
                   avg_net, median, cum, max_win, max_loss, win_avg, loss_avg,
                   pf_raw, pf_net, mdd_raw, mdd_net, avg_hold)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (args.version_id, key, side, s["n"], s["win_pct"], s["avg_raw"],
                 s["avg_net"], s["median"], s["cum"], s["max_win"], s["max_loss"],
                 s["win_avg"], s["loss_avg"], s["pf_raw"], s["pf_net"],
                 s["mdd"], s["mdd_net"], s["avg_hold"]),
            )
            rows_inserted += 1
        conn.commit()
        print(f"已新增版本 {args.version_id}：{rows_inserted} 個訊號")
    finally:
        conn.close()


def cmd_list(args: argparse.Namespace) -> None:
    """Chronological list with avg PF_net and delta from previous version."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT v.version_id, v.date, v.summary,
                   AVG(m.pf_net) AS pf_net_avg,
                   AVG(m.win_pct) AS win_avg,
                   SUM(m.n_trades) AS total_trades
            FROM versions v
            LEFT JOIN metrics m ON m.version_id = v.version_id
            GROUP BY v.version_id
            ORDER BY v.date, v.version_id
        """)
        rows = cur.fetchall()
        if not rows:
            print("尚無任何版本")
            return

        print(f"{'版本':<8}{'日期':<12}{'PF淨':>7}{'勝率%':>7}{'交易':>7}{'Δ PF':>7}  說明")
        print("-" * 90)
        prev_pf = None
        for r in rows:
            pf = r["pf_net_avg"]
            delta = f"{pf - prev_pf:+.2f}" if prev_pf is not None and pf is not None else ""
            pf_s = f"{pf:.2f}" if pf is not None else "—"
            win_s = f"{r['win_avg']:.1f}" if r["win_avg"] is not None else "—"
            n_s = f"{r['total_trades']:,}" if r["total_trades"] else "—"
            print(f"{r['version_id']:<8}{r['date']:<12}{pf_s:>7}{win_s:>7}"
                  f"{n_s:>7}{delta:>7}  {r['summary'] or ''}")
            if pf is not None:
                prev_pf = pf
    finally:
        conn.close()


def cmd_show(args: argparse.Namespace) -> None:
    """Full metric table for one version."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM versions WHERE version_id = ?", (args.version_id,))
        v = cur.fetchone()
        if v is None:
            print(f"版本 {args.version_id} 不存在", file=sys.stderr)
            sys.exit(1)

        print(f"版本：{v['version_id']}  日期：{v['date']}  成本假設：{v['cost_pct']*100:.1f}%")
        print(f"說明：{v['summary'] or ''}")
        print(f"快照：{v['snapshot_path'] or ''}\n")

        cur.execute("SELECT * FROM metrics WHERE version_id = ? ORDER BY signal",
                    (args.version_id,))
        rows = cur.fetchall()
        if not rows:
            print("無指標資料")
            return

        labels = {k: lab for k, lab, _ in SIGNALS}

        print("【目標 1+3】勝率 + 交易質量")
        print(f"{'訊號':<8}{'方向':<5}{'交易':>7}{'勝率%':>7}{'毛均%':>7}{'淨均%':>7}"
              f"{'PF毛':>6}{'PF淨':>6}{'持倉':>5}")
        print("-" * 65)
        for r in rows:
            mk = "(!)" if r["pf_net"] < 1.0 else "   "
            print(f"{labels[r['signal']]:<8}{r['side']:<5}{r['n_trades']:>7}"
                  f"{r['win_pct']:>7.1f}{r['avg_raw']:>+7.2f}{r['avg_net']:>+7.2f}"
                  f"{r['pf_raw']:>6.2f}{r['pf_net']:>6.2f} {mk}{r['avg_hold']:>5.0f}")

        print("\n【目標 2】左尾右尾 + 連續回撤")
        print(f"{'訊號':<8}{'最大獲利%':>10}{'最大虧損%':>10}{'贏均%':>7}{'輸均%':>7}"
              f"{'回撤毛':>10}{'回撤淨':>10}")
        print("-" * 70)
        for r in rows:
            print(f"{labels[r['signal']]:<8}{r['max_win']:>+10.1f}{r['max_loss']:>+10.1f}"
                  f"{r['win_avg']:>+7.2f}{r['loss_avg']:>+7.2f}"
                  f"{r['mdd_raw']:>10.1f}{r['mdd_net']:>10.1f}")
    finally:
        conn.close()


def cmd_diff(args: argparse.Namespace) -> None:
    """Side-by-side metric diff between two versions."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT a.signal, a.side,
                   a.win_pct AS a_win, b.win_pct AS b_win,
                   a.avg_net AS a_avg, b.avg_net AS b_avg,
                   a.pf_net  AS a_pf,  b.pf_net  AS b_pf,
                   a.max_loss AS a_ml, b.max_loss AS b_ml,
                   a.mdd_net AS a_mdd, b.mdd_net AS b_mdd,
                   a.n_trades AS a_n, b.n_trades AS b_n
            FROM metrics a
            JOIN metrics b ON a.signal = b.signal
            WHERE a.version_id = ? AND b.version_id = ?
            ORDER BY a.signal
        """, (args.version_a, args.version_b))
        rows = cur.fetchall()
        if not rows:
            print(f"找不到 {args.version_a} 或 {args.version_b} 的共同訊號", file=sys.stderr)
            sys.exit(1)

        labels = {k: lab for k, lab, _ in SIGNALS}
        a, b = args.version_a, args.version_b
        print(f"diff {a} → {b}  (Δ = b - a)\n")
        print(f"{'訊號':<8}{'方向':<5}{'交易Δ':>8}{'勝率Δ':>8}{'淨均Δ':>8}"
              f"{'PF淨Δ':>8}{'最大虧Δ':>10}{'回撤淨Δ':>10}")
        print("-" * 70)
        for r in rows:
            d_n = r["b_n"] - r["a_n"]
            d_win = r["b_win"] - r["a_win"]
            d_avg = r["b_avg"] - r["a_avg"]
            d_pf = r["b_pf"] - r["a_pf"]
            d_ml = r["b_ml"] - r["a_ml"]
            d_mdd = r["b_mdd"] - r["a_mdd"]
            print(f"{labels[r['signal']]:<8}{r['side']:<5}{d_n:>+8d}{d_win:>+8.1f}"
                  f"{d_avg:>+8.2f}{d_pf:>+8.2f}{d_ml:>+10.1f}{d_mdd:>+10.1f}")
    finally:
        conn.close()


def cmd_delete(args: argparse.Namespace) -> None:
    """Remove a version (cascades to its metrics)."""
    conn = _connect()
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        cur = conn.cursor()
        cur.execute("DELETE FROM metrics WHERE version_id = ?", (args.version_id,))
        cur.execute("DELETE FROM versions WHERE version_id = ?", (args.version_id,))
        conn.commit()
        print(f"已刪除 {args.version_id}")
    finally:
        conn.close()


# ── Entry point ──────────────────────────────────────────────────────────────


def main() -> None:
    p = argparse.ArgumentParser(description="Signal-factory version history")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_add = sub.add_parser("add", help="新增一版（從 scan_dir 讀 trades.parquet）")
    p_add.add_argument("version_id", help="版本識別 (e.g. v1)")
    p_add.add_argument("summary", help="一句話描述這版改了什麼")
    p_add.add_argument("--scan-dir", help=f"trades.parquet 來源 (default: {DEFAULT_SCAN_DIR})")
    p_add.add_argument("--snapshot", help="歸檔 snapshot 路徑 (預設用 scan_dir)")
    p_add.set_defaults(func=cmd_add)

    p_list = sub.add_parser("list", help="列出所有版本")
    p_list.set_defaults(func=cmd_list)

    p_show = sub.add_parser("show", help="顯示單一版本完整指標")
    p_show.add_argument("version_id")
    p_show.set_defaults(func=cmd_show)

    p_diff = sub.add_parser("diff", help="比對兩版差異")
    p_diff.add_argument("version_a")
    p_diff.add_argument("version_b")
    p_diff.set_defaults(func=cmd_diff)

    p_del = sub.add_parser("delete", help="刪除一版")
    p_del.add_argument("version_id")
    p_del.set_defaults(func=cmd_delete)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
