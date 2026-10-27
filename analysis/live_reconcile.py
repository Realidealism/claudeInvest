"""Monthly reconciliation of the nightly snapshot against the backtest.

The nightly snapshot (tw.open_positions) reruns the unified backtest every
night, so what it recorded over a month should be what the backtest of the
deployed build says for the same nights. Two questions, one report:

  (a) consistency -- each unified position the snapshot recorded (exits in
      the month, positions held on its last night) paired by (stock, side,
      entry date) with the backtest's trades. Every record lands in exactly
      one class; records on one side only are attributed to a cause.
  (b) out of sample -- trades entered on or after HOLDOUT_START, which the
      backtest and sweep outputs leave out so no tuning sees them, against
      the in-sample per-year range from the same run.

Two backtests run on the deployed commit (dist/BUILD_INFO.json), checked out
in a worktree under tmp/, so experiments in the working tree do not leak in:
(a) raw, as the snapshot records positions, and (b) on the baseline's
accounting (total returns and the short-sale constraints, the CLI defaults
from v370), as the adopted versions are measured. A build from before v370
has no such defaults; (b) then reads the raw run and says so.
Reads the database only; writes the report under logs/reconcile/ and sends a
summary through the bot. Run monthly by live_reconcile.bat.

Usage:
    python -m analysis.live_reconcile             # the previous calendar month
    python -m analysis.live_reconcile 2026-10 --no-send
"""
from __future__ import annotations

import argparse
import io
import json
import os
import shutil
import subprocess
import sys
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

if __name__ == "__main__" and sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from db.connection import get_cursor
from signal_backtest._compare import SIGNALS, compute_stats
from signal_backtest.engine import HOLDOUT_START
from signal_backtest.trade import REASON_EXIT_END

REPO = Path(__file__).resolve().parents[1]
BUILD_INFO = REPO / "dist" / "BUILD_INFO.json"
WORKTREE = REPO / "tmp" / "reconcile_wt"
OUT_ROOT = REPO / "tmp" / "reconcile"
REPORT_DIR = REPO / "logs" / "reconcile"

BACKTEST_SIGNALS = "pick,touch,buy,sell,buy_flee,sell_flee,unified_long,unified_short"
BACKTEST_TIMEOUT = 1800       # seconds; a cold 8-signal run measured 350-371 s (2026-10-19)
GIT_TIMEOUT = 120
MIN_MONEY_LEVEL = 4           # the nightly universe, analysis/daily_snapshot.py
# What the positions depend on: the factories import analysis/ (scores, the
# thermometer, indicators) and backtest/ (the data loader) besides their own.
VERSION_PATHS = ("signal_backtest", "analysis", "backtest")
PNL_TOL = 1e-4                # 0.01 percentage point; the snapshot rounds to 0.001%
SIDES = {"long": ("unified_long", "做多"), "short": ("unified_short", "做空")}

# (a) classes, and the causes of everything not consistent
CLASSES = ("一致", "同筆不同結果", "只在線上", "只在回測")
CAUSES = ("已記改寫", "母體外", "版本不同", "版本未知", "未解釋")

# (b)'s accounting
BASIS_BASELINE = "總報酬＋做空限制"
BASIS_RAW = "原始口徑：未計股利、無做空限制"


class ReconcileError(Exception):
    pass


# ── month and code ───────────────────────────────────────────────────────────


def previous_month(today: date) -> str:
    last = today.replace(day=1) - timedelta(days=1)
    return f"{last.year:04d}-{last.month:02d}"


def month_bounds(month: str) -> tuple[date, date]:
    first = date.fromisoformat(f"{month}-01")
    nxt = (first.replace(day=28) + timedelta(days=4)).replace(day=1)
    return first, nxt - timedelta(days=1)


def _git(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    r = subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=GIT_TIMEOUT)
    if check and r.returncode != 0:
        raise ReconcileError(f"git {' '.join(args)} failed: {r.stderr.strip()}")
    return r


def deployed_build() -> dict:
    if not BUILD_INFO.exists():
        raise ReconcileError(f"{BUILD_INFO} not found: no deployed build to reconcile against")
    info = json.loads(BUILD_INFO.read_text(encoding="utf-8"))
    if not info.get("commit"):
        raise ReconcileError(f"{BUILD_INFO} records no commit")
    return info


def remove_worktree() -> None:
    # Only this worktree: a repo-wide prune would also touch the others.
    _git("worktree", "remove", "--force", str(WORKTREE), check=False)
    if WORKTREE.exists():
        shutil.rmtree(WORKTREE, ignore_errors=True)


def prepare_worktree(commit: str) -> Path:
    remove_worktree()
    # -f: the path may still be registered if its folder went without git.
    _git("worktree", "add", "-f", "--detach", str(WORKTREE), commit)
    return WORKTREE


def supports_holdout(src: Path) -> bool:
    return "--include-holdout" in (src / "signal_backtest" / "__main__.py").read_text(
        encoding="utf-8")


def has_baseline_defaults(src: Path) -> bool:
    """From v370 the CLI defaults to total returns and the short-sale
    constraints, which the snapshot has neither of; such a build is told to
    run raw. An older build runs raw by default and has no such flag."""
    return "--raw-return" in (src / "signal_backtest" / "__main__.py").read_text(encoding="utf-8")


def run_backtest(src: Path, out: Path, today: date, *, raw: bool) -> None:
    """The 8 signals on the code in src, every trade kept: raw, or on the
    build's defaults (raw=False, only for a build that has_baseline_defaults).
    A build from before the holdout cut has no flag; its output is the same
    while no entry can yet fall on or after HOLDOUT_START, and wrong after that."""
    # No --cache: the cache would live in the worktree, cold and then deleted.
    cmd = [sys.executable, "-m", "signal_backtest", "--signals", BACKTEST_SIGNALS,
           "--output", str(out), "--workers", "16"]
    if supports_holdout(src):
        cmd.append("--include-holdout")
    elif today >= HOLDOUT_START:
        raise ReconcileError("部署版本不支援 --include-holdout，回測會漏掉 holdout 交易；需重建")
    if raw and has_baseline_defaults(src):
        cmd += ["--raw-return", "--short-constraints", "off"]
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    env.pop("PYTHONPATH", None)
    with open(out / "backtest.log", "w", encoding="utf-8") as log:
        proc = subprocess.Popen(cmd, cwd=src, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            rc = proc.wait(timeout=BACKTEST_TIMEOUT)
        except subprocess.TimeoutExpired:
            # The Pool workers are grandchildren; kill the tree, not just the parent.
            subprocess.run(["taskkill", "/T", "/F", "/PID", str(proc.pid)], capture_output=True)
            proc.wait()
            raise ReconcileError(f"backtest exceeded {BACKTEST_TIMEOUT}s; see {out / 'backtest.log'}")
    if rc != 0:
        raise ReconcileError(f"backtest exit {rc}; see {out / 'backtest.log'}")


# ── records ──────────────────────────────────────────────────────────────────


def load_window(cur, first: date, last: date) -> tuple[date | None, date]:
    """(S, D): the nightly snapshot before the month and the month's last one.
    The month covers exits in (S, D] and the positions held on D."""
    cur.execute("SELECT MAX(snapshot_date) AS d FROM tw.open_positions "
                "WHERE snapshot_date BETWEEN %s AND %s", (first, last))
    end = cur.fetchone()["d"]
    if end is None:
        raise ReconcileError(f"no nightly snapshot between {first} and {last}")
    cur.execute("SELECT MAX(snapshot_date) AS d FROM tw.open_positions WHERE snapshot_date < %s",
                (first,))
    return cur.fetchone()["d"], end


def load_live(cur, start: date | None, end: date) -> tuple[pd.DataFrame, int]:
    """The snapshot's records for the window, and how many exit rows repeat an
    earlier one: a stock with no bar on a night shows its last bar's exit
    again, so each position's exit is its first row."""
    lo = start or date.min
    cur.execute("SELECT stock_id, side, entry_date, snapshot_date AS exit_date, "
                "pnl_pct / 100.0 AS pnl FROM tw.open_positions "
                "WHERE is_exited AND snapshot_date <= %s ORDER BY snapshot_date", (end,))
    first: dict[tuple, dict] = {}
    repeats = 0
    for r in cur.fetchall():
        k = (r["stock_id"], r["side"], r["entry_date"])
        if k in first:
            repeats += r["exit_date"] > lo
            continue
        # pnl_pct is NUMERIC: psycopg2 returns Decimal, the backtest float.
        first[k] = dict(r, pnl=float(r["pnl"]), state="exited")
    # Real exits no row showed (a night with no snapshot, or hidden on the row).
    # Recorded by the month's last night: a later night may date an exit back
    # onto D (a gap exit), which D itself showed held.
    cur.execute("SELECT stock_id, side, entry_date, exit_date FROM tw.position_revisions "
                "WHERE cause IN ('exited_unseen', 'exited_hidden') AND exit_date <= %s "
                "AND snapshot_date <= %s", (end, end))
    for r in cur.fetchall():
        k = (r["stock_id"], r["side"], r["entry_date"])
        if k not in first or r["exit_date"] < first[k]["exit_date"]:
            first[k] = dict(r, pnl=None, state="exited")
    exits = [v for v in first.values() if v["exit_date"] > lo]
    cur.execute("SELECT stock_id, side, entry_date FROM tw.open_positions "
                "WHERE snapshot_date = %s AND NOT is_exited", (end,))
    held = [dict(r, exit_date=None, pnl=None, state="held") for r in cur.fetchall()]
    return _records(exits + held), repeats


def load_backtest(bt_dir: Path, start: date | None, end: date) -> pd.DataFrame:
    lo = start or date.min
    rows = []
    for side, (key, _) in SIDES.items():
        df = pd.read_parquet(bt_dir / key / "trades.parquet")
        for t in df.itertuples(index=False):
            entry, exit_ = _day(t.進場日期), _day(t.出場日期)
            if entry > end:
                continue
            if t.出場原因 == REASON_EXIT_END or exit_ > end:
                rows.append(dict(stock_id=t.股票代號, side=side, entry_date=entry,
                                 exit_date=None, pnl=None, state="held"))
            elif exit_ > lo:
                rows.append(dict(stock_id=t.股票代號, side=side, entry_date=entry,
                                 exit_date=exit_, pnl=float(t.報酬率), state="exited"))
    return _records(rows)


def _day(v) -> date:
    return v if type(v) is date else pd.Timestamp(v).date()


def _records(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["stock_id", "side", "entry_date", "exit_date", "pnl", "state"])
    dup = df.duplicated(["stock_id", "side", "entry_date"])
    if dup.any():
        raise ReconcileError(f"duplicate keys: {df[dup].head().to_dict('records')}")
    return df


# ── (a) classification ───────────────────────────────────────────────────────


def _same(a, b) -> bool:
    if a.state != b.state:
        return False
    if a.state == "held":
        return True
    if a.exit_date != b.exit_date:
        return False
    if pd.isna(a.pnl) or pd.isna(b.pnl):      # an unseen exit has no price
        return True
    return abs(a.pnl - b.pnl) <= PNL_TOL


def classify(live: pd.DataFrame, bt: pd.DataFrame, *, revised: set, low_level: set,
             version_of) -> pd.DataFrame:
    """One row per key on either side, with its class and, unless consistent,
    its cause. revised: keys (stock, side, entry) the snapshot recorded as
    rewritten; low_level: keys below the nightly money level on every night
    held; version_of(stock, side, entry, end) -> "same" | "diff" | "unknown"."""
    key = ["stock_id", "side", "entry_date"]
    m = live.merge(bt, on=key, how="outer", suffixes=("_live", "_bt"), indicator="where")
    out = []
    for r in m.itertuples(index=False):
        k = (r.stock_id, r.side, r.entry_date)
        if r.where == "both":
            a = pd.Series(dict(state=r.state_live, exit_date=r.exit_date_live, pnl=r.pnl_live))
            b = pd.Series(dict(state=r.state_bt, exit_date=r.exit_date_bt, pnl=r.pnl_bt))
            cls = "一致" if _same(a, b) else "同筆不同結果"
        else:
            cls = "只在線上" if r.where == "left_only" else "只在回測"
        cause = None
        if cls != "一致":
            # The nights that held it: from the entry to its exit, or to the
            # month's last night while either side still holds it.
            states = {s for s in (r.state_live, r.state_bt) if isinstance(s, str)}
            exits = [d for d in (r.exit_date_live, r.exit_date_bt) if isinstance(d, date)]
            stop = None if "held" in states else max(exits)
            if k in revised:
                cause = "已記改寫"
            elif cls == "只在回測" and k in low_level:
                cause = "母體外"
            else:
                v = version_of(r.stock_id, r.side, r.entry_date, stop)
                cause = {"diff": "版本不同", "unknown": "版本未知"}.get(v, "未解釋")
        out.append(dict(stock_id=r.stock_id, side=r.side, entry_date=r.entry_date,
                        live_state=r.state_live, live_exit=r.exit_date_live, live_pnl=r.pnl_live,
                        bt_state=r.state_bt, bt_exit=r.exit_date_bt, bt_pnl=r.pnl_bt,
                        cls=cls, cause=cause))
    res = pd.DataFrame(out, columns=["stock_id", "side", "entry_date", "live_state", "live_exit",
                                     "live_pnl", "bt_state", "bt_exit", "bt_pnl", "cls", "cause"])
    # Closure (rules/02 Rubric 8): every record on each side is in exactly one row.
    n_live = int(res["cls"].isin(["一致", "同筆不同結果", "只在線上"]).sum())
    n_bt = int(res["cls"].isin(["一致", "同筆不同結果", "只在回測"]).sum())
    assert n_live == len(live) and n_bt == len(bt), (n_live, len(live), n_bt, len(bt))
    assert res["cls"].isin(CLASSES).all()
    assert ((res["cls"] == "一致") == res["cause"].isna()).all()
    return res


def load_revised(cur, start: date | None, end: date) -> set:
    cur.execute("SELECT stock_id, side, entry_date, new_entry_date FROM tw.position_revisions "
                "WHERE cause IN ('redated', 'gone', 'not_run') "
                "AND snapshot_date > %s AND snapshot_date <= %s", (start or date.min, end))
    keys = set()
    for r in cur.fetchall():
        keys.add((r["stock_id"], r["side"], r["entry_date"]))
        if r["new_entry_date"] is not None:
            keys.add((r["stock_id"], r["side"], r["new_entry_date"]))
    return keys


def load_low_level(cur, spans: list[tuple[str, str, date, date]]) -> set:
    """(stock, side, entry) of the spans (stock, side, entry, last day held)
    the nightly run evaluated on no night: below the money level, or no
    liquidity row, on every day of it. One night at the level would have
    shown the position."""
    if not spans:
        return set()
    sids, sides, starts, stops = ([s[i] for s in spans] for i in range(4))
    cur.execute("SELECT k.sid, k.side, k.d0 FROM unnest(%s::text[], %s::text[], %s::date[], "
                "%s::date[]) AS k(sid, side, d0, d1) WHERE NOT EXISTS (SELECT 1 FROM "
                "tw.stock_liquidity_daily l WHERE l.stock_id = k.sid "
                "AND l.trade_date BETWEEN k.d0 AND k.d1 AND l.money_level >= %s)",
                (sids, sides, starts, stops, MIN_MONEY_LEVEL))
    return {(r["sid"], r["side"], r["d0"]) for r in cur.fetchall()}


def _relevant(paths) -> list[str]:
    return [p for p in paths or [] if p.replace("\\", "/").startswith(VERSION_PATHS)]


def make_version_of(cur, info: dict, end: date):
    """version_of(...) for classify: did any night from the entry to the end
    run code (VERSION_PATHS) other than the deployed build's? A night with no
    tw.snapshot_runs row is unknown. So is a "+dirty" night, unless it is the
    deployed build's own stamp, run after that build (an earlier dirty build
    of the same commit carries the same stamp), and the build's uncommitted
    files are outside VERSION_PATHS: uncommitted code cannot be compared."""
    cur.execute("SELECT DISTINCT snapshot_date FROM tw.open_positions WHERE snapshot_date <= %s",
                (end,))
    nights = sorted(r["snapshot_date"] for r in cur.fetchall())
    cur.execute("SELECT snapshot_date, build_commit, created_at FROM tw.snapshot_runs "
                "WHERE kind = 'daily'")
    run_of = {r["snapshot_date"]: (r["build_commit"], r["created_at"]) for r in cur.fetchall()}
    base = info["commit"]
    deployed = base + ("+dirty" if info.get("uncommitted_sources") else "")
    clean_dirt = not _relevant(info.get("uncommitted_sources"))
    # built_at is the build machine's local time, written without a zone.
    built_at = (datetime.fromisoformat(info["built_at"]).astimezone()
                if info.get("built_at") else None)
    differs: dict[str, str] = {}

    def code_differs(stamp: str, ran_at) -> str:
        if stamp == deployed and clean_dirt and (
                "+" not in stamp or (built_at is not None and ran_at >= built_at)):
            return "same"
        commit, dirty = stamp.split("+")[0], "+" in stamp
        if commit not in differs:
            if commit == base:
                differs[commit] = "same"
            else:
                r = _git("diff", "--quiet", base, commit, "--", *VERSION_PATHS, check=False)
                differs[commit] = {0: "same", 1: "diff"}.get(r.returncode, "unknown")
        if differs[commit] == "same" and (dirty or not clean_dirt):
            return "unknown"
        return differs[commit]

    def version_of(_sid, _side, entry: date, stop: date | None) -> str:
        span = [n for n in nights if entry <= n <= (stop or end)]
        states = {code_differs(*run_of[n]) if n in run_of else "unknown" for n in span}
        if "diff" in states:
            return "diff"
        return "unknown" if "unknown" in states or not span else "same"

    return version_of


# ── (b) out of sample ────────────────────────────────────────────────────────


def _stats(df: pd.DataFrame) -> dict | None:
    s = compute_stats(df) if len(df) else None
    if s is None:
        return None
    payoff = s["win_avg"] / abs(s["loss_avg"]) if s["loss_avg"] else float("inf")
    return dict(n=s["n"], pf=s["pf_net"], win=s["win_pct"], payoff=payoff)


def holdout_section(bt_dir: Path, end: date) -> dict:
    groups = {"合池": [k for k, _, _ in SIGNALS], "unified 多": ["unified_long"],
              "unified 空": ["unified_short"]}
    out = {}
    for name, keys in groups.items():
        df = pd.concat([pd.read_parquet(bt_dir / k / "trades.parquet") for k in keys],
                       ignore_index=True)
        entry = df["進場日期"].map(_day)
        exit_ = df["出場日期"].map(_day)
        closed = (df["出場原因"] != REASON_EXIT_END) & (exit_ <= end)
        oos = entry >= HOLDOUT_START
        ins = df[~oos & (df["出場原因"] != REASON_EXIT_END)]
        years = {y: _stats(g) for y, g in ins.groupby(entry[ins.index].map(lambda d: d.year))}
        out[name] = dict(
            holdout=_stats(df[oos & closed]),
            holdout_open=int((oos & (entry <= end) & ~closed).sum()),
            years={y: s for y, s in years.items() if s is not None},
        )
    return out


# ── output ───────────────────────────────────────────────────────────────────


def _pf(v: float) -> str:
    return "inf" if v == float("inf") else f"{v:.2f}"


def _year_range(years: dict) -> str:
    pfs = sorted(s["pf"] for s in years.values())
    if not pfs:
        return "無"
    mid = pfs[len(pfs) // 2] if len(pfs) % 2 else (pfs[len(pfs) // 2 - 1] + pfs[len(pfs) // 2]) / 2
    return f"{_pf(pfs[0])}～{_pf(pfs[-1])}，中位 {_pf(mid)}"


def summary_text(month: str, start, end, commit: str, flags: list[str], res: pd.DataFrame,
                 n_live: int, n_bt: int, hold: dict, report: Path, *, basis: str) -> str:
    cls = res["cls"].value_counts()
    cause = res["cause"].value_counts()
    lines = [f"【月對帳】{month}（夜間快照 {start or '起'} 之後～{end}，部署 {commit[:8]}）"]
    lines += [f"⚠ {f}" for f in flags]
    lines.append(f"逐筆一致性（unified 多／空）：線上 {n_live} 筆、回測 {n_bt} 筆")
    lines.append("  " + "｜".join(f"{c} {int(cls.get(c, 0))}" for c in CLASSES))
    if cause.sum():
        lines.append("  成因：" + "、".join(f"{c} {int(cause[c])}" for c in CAUSES if c in cause))
    if end < HOLDOUT_START:
        lines.append(f"樣本外（{HOLDOUT_START} 起進場；{basis}）：尚未開始")
    else:
        lines.append(f"樣本外（{HOLDOUT_START} 起進場，累計至 {end}；{basis}）：")
        for name, g in hold.items():
            h = g["holdout"]
            got = (f"{h['n']} 筆 PF {_pf(h['pf'])} 勝率 {h['win']:.1f}% 賺賠比 {_pf(h['payoff'])}"
                   if h else "0 筆已出場")
            lines.append(f"  {name}：{got}，持有中 {g['holdout_open']}"
                         f"（樣本內逐年 PF {_year_range(g['years'])}）")
    lines.append(f"報告：{report.relative_to(REPO)}")
    return "\n".join(lines)


def report_text(summary: str, res: pd.DataFrame, hold: dict, *, basis: str) -> str:
    lines = ["# 月對帳", "", "```", summary, "```", "", "## (a) 逐筆一致性", "",
             "成因的意思：",
             "- 已記改寫：快照自己在 tw.position_revisions 記下了這筆被重算改掉。",
             "- 母體外：持有期間每一晚的 money_level 都 < 4（或沒有流動性列），夜間快照不會跑到。",
             "- 版本不同：持有期間有一晚跑的程式（signal_backtest/、analysis/、backtest/）和部署版不同。",
             "- 版本未知：持有期間有一晚沒有 tw.snapshot_runs 紀錄（10-19 以前），"
             "或跑的是含未提交改動的程式，無法比對。",
             "- 未解釋：以上都不是；資料事後被修正、補跑較晚時 D+1 跳空把出場回填到 D，都會落在這裡。",
             ""]
    for side in SIDES:
        sub = res[res["side"] == side]
        lines.append(f"- {SIDES[side][1]}：" + "，".join(
            f"{c} {int((sub['cls'] == c).sum())}" for c in CLASSES))
    lines += ["", "### 不一致明細", "",
              "| 股票 | 方向 | 進場日 | 類別 | 成因 | 線上 | 回測 |", "|---|---|---|---|---|---|---|"]

    def side_txt(state, exit_, pnl):
        if not isinstance(state, str):
            return "—"
        if state == "held":
            return "持有"
        p = "無價" if pnl is None or pd.isna(pnl) else f"{pnl * 100:+.2f}%"
        return f"{exit_} 出場 {p}"

    for r in res[res["cls"] != "一致"].sort_values(["cls", "cause", "entry_date"]).itertuples():
        lines.append(f"| {r.stock_id} | {SIDES[r.side][1]} | {r.entry_date} | {r.cls} | {r.cause} | "
                     f"{side_txt(r.live_state, r.live_exit, r.live_pnl)} | "
                     f"{side_txt(r.bt_state, r.bt_exit, r.bt_pnl)} |")
    why = ("(b) 要和版本採用時的數字同一把尺，所以另跑一次部署版的預設設定。"
           if basis == BASIS_BASELINE else "部署版早於 v370、沒有這組預設，(b) 只能沿用 (a) 的回測。")
    lines += ["", "## (b) 樣本外", "",
              f"口徑：{basis}。(a) 用原始口徑，因為夜間快照的持倉與損益不計股利、也沒有做空限制；{why}", ""]
    for name, g in hold.items():
        h = g["holdout"]
        lines.append(f"### {name}")
        lines.append(f"- holdout（{HOLDOUT_START} 起進場、已出場）：" + (
            f"n {h['n']}，PF {_pf(h['pf'])}，勝率 {h['win']:.1f}%，賺賠比 {_pf(h['payoff'])}"
            if h else "0 筆") + f"；持有中 {g['holdout_open']}")
        lines += ["", "| 進場年 | n | PF 淨 | 勝率% | 賺賠比 |", "|---|---|---|---|---|"]
        for y, s in sorted(g["years"].items()):
            lines.append(f"| {y} | {s['n']} | {_pf(s['pf'])} | {s['win']:.1f} | {_pf(s['payoff'])} |")
        lines.append("")
    return "\n".join(lines) + "\n"


def send(text: str) -> bool:
    from telegram_bot.notify import send_sync
    return send_sync(text)


# ── main ─────────────────────────────────────────────────────────────────────


def reconcile(month: str, *, today: date, do_send: bool) -> int:
    first, last = month_bounds(month)
    info = deployed_build()
    commit = info["commit"]
    flags = [f"部署含未提交改動：{p}" for p in _relevant(info.get("uncommitted_sources"))]
    bt_dir = OUT_ROOT / month / "bt"
    base_dir = OUT_ROOT / month / "bt_baseline"
    key = ["stock_id", "side", "entry_date"]
    try:
        try:
            src = prepare_worktree(commit)
            run_backtest(src, bt_dir, today, raw=True)
            if has_baseline_defaults(src):
                run_backtest(src, base_dir, today, raw=False)
                hold_dir, basis = base_dir, BASIS_BASELINE
            else:
                hold_dir, basis = bt_dir, BASIS_RAW
                flags.append(f"部署版早於 v370，樣本外為{BASIS_RAW}")
        finally:
            remove_worktree()

        with get_cursor(commit=False) as cur:
            start, end = load_window(cur, first, last)
            live, repeats = load_live(cur, start, end)
            bt = load_backtest(bt_dir, start, end)
            revised = load_revised(cur, start, end)
            only_bt = bt.merge(live[key], on=key, how="left", indicator="where")
            only_bt = only_bt[only_bt["where"] == "left_only"]
            low = load_low_level(cur, [
                (r.stock_id, r.side, r.entry_date,
                 r.exit_date if isinstance(r.exit_date, date) else end)
                for r in only_bt.itertuples(index=False)])
            version_of = make_version_of(cur, info, end)
            res = classify(live, bt, revised=revised, low_level=low, version_of=version_of)
        hold = holdout_section(hold_dir, end)
    finally:
        # The outputs hold the holdout's trades; nothing else may read them.
        shutil.rmtree(bt_dir, ignore_errors=True)
        shutil.rmtree(base_dir, ignore_errors=True)
    if repeats:
        flags.append(f"重複出場列 {repeats} 筆（無 K 棒的夜晚重寫了前一根的出場，只取第一列）")

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report = REPORT_DIR / f"{month}.md"
    summary = summary_text(month, start, end, commit, flags, res, len(live), len(bt), hold,
                           report, basis=basis)
    report.write_text(report_text(summary, res, hold, basis=basis), encoding="utf-8")
    print(summary)
    if do_send and not send(summary):
        print("summary not delivered", file=sys.stderr)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("month", nargs="?", help="YYYY-MM (default: the previous calendar month)")
    p.add_argument("--no-send", action="store_true", help="print the summary, do not send it")
    args = p.parse_args(argv)
    today = date.today()
    month = args.month or previous_month(today)
    try:
        return reconcile(month, today=today, do_send=not args.no_send)
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
