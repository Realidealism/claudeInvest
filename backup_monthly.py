"""Monthly PostgreSQL backup -> D:\\invest_backup\\invest_YYYYMMDD.dump

Target of the Task Scheduler entry Invest\\BackupMonthly (1st of the month
03:00, only while the user is logged on, missed runs are not caught up) through
backup_monthly.bat. The database lives in the Docker container `invest-db`, so
pg_dump runs inside the container and the custom-format archive is streamed to
the host.

A dump only counts once it has been read back: it is written to a .partial
file, listed with pg_restore and checked for the core table, and only then
renamed. Older dumps are pruned after that, so a failed run can never cost an
existing backup. The size comparison with the previous dump is a tripwire for
gross truncation, not evidence of completeness -- that evidence is an actual
restore, see ops/RESTORE.md.

Any failure exits non-zero AND sends a Telegram alert. The exit code is what
Task Scheduler records; the alert is what a person sees. Neither replaces the
other.

Reconstructed 2026-10-06. The original script was never committed and was lost
on 2026-07-02 together with its wrapper; the task stayed scheduled but its
target no longer existed, so no dump was produced on 2026-08-01, 09-01 or
10-01, unnoticed because the failure alert lived in the lost script. Those months
cannot be recovered: this script protects the database from the date of its
first archive onwards. The exclusion list below was recovered from the last
archive the original produced (2026-07-01).

CLI:
    python backup_monthly.py
    python backup_monthly.py --container NAME --out-dir DIR --keep N
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import traceback
from datetime import date
from pathlib import Path

CONTAINER = "invest-db"
DB_NAME = "invest"
DB_USER = "postgres"
OUT_DIR = Path(r"D:\invest_backup")
KEEP = 6

# Schema is dumped, rows are not: intraday history and per-session scratch
# tables that the live pipeline rebuilds, and that would triple the archive
# (tw.score_snapshot_intraday alone is 2.2 GB).
#
# tw.intraday_value_profile is deliberately NOT here although the original
# script excluded it: it is under 1 MB, cannot be re-collected, and both the
# h(t) turnover curve and the opening-session brief need weeks of it, so
# leaving it out would cost about 21 trading days of warm-up after a restore.
EXCLUDE_DATA = (
    "tw.intraday_opening_range",
    "tw.intraday_orb_signals",
    "tw.intraday_quotes",
    "tw.open_positions_intraday",
    "tw.orb_backtest_trades",
    "tw.score_snapshot_intraday",
    "tw.signal_snapshot_intraday",
    "tw.stock_halts_today",
)

# An archive without this table's rows is not a backup of this database.
CORE_TABLE = ("tw", "daily_prices")
# Tripwire only: a new dump under half the previous one was almost certainly
# truncated. Passing it proves nothing about completeness.
MIN_SIZE_RATIO = 0.5
DUMP_TIMEOUT_S = 2 * 60 * 60
LIST_TIMEOUT_S = 10 * 60

_DUMP_NAME = re.compile(r"^invest_\d{8}\.dump$")


class BackupError(RuntimeError):
    pass


def _existing_dumps(out_dir: Path) -> list[Path]:
    """Completed dumps, newest first. The date in the name is the sort key."""
    return sorted((p for p in out_dir.iterdir() if _DUMP_NAME.match(p.name)),
                  key=lambda p: p.name, reverse=True)


def _dump(container: str, partial: Path) -> None:
    cmd = ["docker", "exec", container, "pg_dump", "-U", DB_USER, "-d", DB_NAME, "-Fc"]
    cmd += [f"--exclude-table-data={t}" for t in EXCLUDE_DATA]
    with open(partial, "wb") as fh:
        proc = subprocess.run(cmd, stdout=fh, stderr=subprocess.PIPE, timeout=DUMP_TIMEOUT_S)
    if proc.returncode != 0:
        tail = proc.stderr.decode("utf-8", "replace").strip()[-300:]
        raise BackupError(f"pg_dump exit {proc.returncode}: {tail}")


def _verify(container: str, partial: Path, previous: Path | None) -> int:
    """Prove the archive is readable and complete enough. Returns its size."""
    with open(partial, "rb") as fh:
        proc = subprocess.run(["docker", "exec", "-i", container, "pg_restore", "-l"],
                              stdin=fh, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              timeout=LIST_TIMEOUT_S)
    if proc.returncode != 0:
        tail = proc.stderr.decode("utf-8", "replace").strip()[-300:]
        raise BackupError(f"archive cannot be listed (pg_restore -l exit {proc.returncode}): {tail}")
    toc = proc.stdout.decode("utf-8", "replace")
    if f"TABLE DATA {CORE_TABLE[0]} {CORE_TABLE[1]} " not in toc:
        raise BackupError(f"archive has no rows for {CORE_TABLE[0]}.{CORE_TABLE[1]}")
    size = partial.stat().st_size
    if previous is not None and size < MIN_SIZE_RATIO * previous.stat().st_size:
        raise BackupError(
            f"archive is {size / 1e6:.0f} MB, less than {MIN_SIZE_RATIO:.0%} of "
            f"{previous.name} ({previous.stat().st_size / 1e6:.0f} MB)")
    return size


def _alert(reason: str, out_dir: Path) -> None:
    """Tell a person. A failed alert is logged too -- it must not vanish."""
    try:
        latest = _existing_dumps(out_dir)
        last = latest[0].name if latest else "none"
    except OSError:
        last = "unknown"
    try:
        from telegram_bot.notify import send_sync
        delivered = send_sync(f"[備份] 月備失敗：{reason}\n最後一份成功的備份：{last}")
        if not delivered:
            print("[ERROR] backup alert was not delivered")
    except Exception:
        print("[ERROR] backup alert raised:")
        traceback.print_exc(file=sys.stdout)


def run(container: str, out_dir: Path, keep: int) -> Path:
    if not out_dir.is_dir():
        raise BackupError(f"output directory does not exist: {out_dir}")
    final = out_dir / f"invest_{date.today():%Y%m%d}.dump"
    partial = final.with_name(final.name + ".partial")
    previous = next((p for p in _existing_dumps(out_dir) if p != final), None)
    try:
        _dump(container, partial)
        size = _verify(container, partial, previous)
        partial.replace(final)
    finally:
        if partial.exists():
            partial.unlink()
    # Prune only after the new dump is in place and verified.
    for old in _existing_dumps(out_dir)[keep:]:
        old.unlink()
        print(f"pruned: {old.name}")
    print(f"backup ok: {final} ({size / 1e6:.0f} MB)")
    return final


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Dump the invest database to a dated archive.")
    p.add_argument("--container", default=CONTAINER)
    p.add_argument("--out-dir", type=Path, default=OUT_DIR)
    p.add_argument("--keep", type=int, default=KEEP)
    args = p.parse_args(argv)
    try:
        run(args.container, args.out_dir, args.keep)
        return 0
    except subprocess.TimeoutExpired as exc:
        reason = f"timed out after {exc.timeout:.0f}s"
    except BackupError as exc:
        reason = str(exc)
    except Exception as exc:
        traceback.print_exc(file=sys.stdout)
        reason = f"{type(exc).__name__}: {exc}"
    print(f"[ERROR] backup failed: {reason}")
    _alert(reason, args.out_dir)
    return 1


if __name__ == "__main__":
    sys.exit(main())
