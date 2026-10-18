"""What rebuild_and_restart.bat is about to build, and what it built.

  check  -- before a build. Refuses (exit 5) when the commit recorded for the
            exes now in dist\\ is not in HEAD's history: building would drop
            whatever that deployment carried. On 2026-10-15 four rebuilds
            from this tree replaced, unnoticed, a deployment built from two
            branches the tree did not have. Also refuses when exes exist but
            no record does (deleted, or a build that stopped half way), and
            when a migration is uncommitted: the exes bundle db/migrations
            and init_db applies whatever is new to the production database.
            --allow-older overrides the first two; --allow-migrations the
            last. Uncommitted .py/.spec files are listed as a warning only:
            other lines of work often have some in progress.
  record -- after a successful build, writes dist\\BUILD_INFO.json (commit,
            branch, exes covered, the uncommitted source files that went in,
            time). The .bat deletes the record before building, so a build
            that fails half way leaves none.
  stamp  -- before the build, writes _build_stamp.py (gitignored) holding
            the commit, "+dirty" when uncommitted sources go in. The exes
            bundle it, and each snapshot run records it (tw.snapshot_runs).
  unstamp -- after the build, or when it fails, removes _build_stamp.py, so
            programs run from source read git instead of a stale stamp.

Output is ASCII: the .bat appends it to an ASCII log.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
INFO = REPO / "dist" / "BUILD_INFO.json"
STAMP = REPO / "_build_stamp.py"
EXES = ("daily_update.exe", "intraday_snapshot.exe")   # what rebuild_and_restart.bat builds
REFUSED = 5


def _git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True,
                          encoding="utf-8", errors="replace")


def _head() -> str:
    """HEAD's commit, or "" when git cannot say."""
    r = _git("rev-parse", "HEAD")
    if r.returncode != 0:
        print(f"[build_info] git rev-parse HEAD failed: {r.stderr.strip()}")
        return ""
    return r.stdout.strip()


def _uncommitted(*pathspecs: str) -> list[str]:
    out = _git("status", "--porcelain", "--", *pathspecs).stdout
    return sorted(line[3:] for line in out.splitlines() if line.strip())


def check(allow_older: bool = False, allow_migrations: bool = False) -> int:
    for path in _uncommitted("*.py", "*.spec"):
        print(f"[build_info] WARNING uncommitted source goes into this build: {path}")
    migrations = _uncommitted("db/migrations/*.sql")
    for path in migrations:
        print(f"[build_info] uncommitted migration would be bundled and applied: {path}")
    if migrations and not allow_migrations:
        print("[build_info] REFUSED: commit or remove the migration(s) first, "
              "or rerun with --allow-migrations.")
        return REFUSED

    if not INFO.exists():
        built = [exe for exe in EXES if (INFO.parent / exe).exists()]
        if not built:
            print("[build_info] no dist\\BUILD_INFO.json and no exes yet; nothing to compare")
            return 0
        why = (f"dist\\ has {', '.join(built)} but no BUILD_INFO.json (deleted, or a "
               f"build that stopped half way), so what they were built from is unknown")
        return _refuse_or_allow(why, allow_older)

    deployed = json.loads(INFO.read_text(encoding="utf-8")).get("commit") or ""
    head = _head()
    r = _git("merge-base", "--is-ancestor", deployed, head) if deployed and head else None
    if r is not None and r.returncode == 0:
        print(f"[build_info] ok: the deployed build {deployed[:10]} is in HEAD {head[:10]}")
        return 0
    if r is not None and r.returncode == 1:
        why = (f"the deployed build {deployed[:10]} is not in the history of HEAD "
               f"{head[:10]}; building now would drop what it carried")
    else:
        detail = r.stderr.strip() if r is not None else "no commit recorded or no HEAD"
        why = (f"git could not compare the deployed build {deployed[:10] or '?'} with "
               f"HEAD {head[:10] or '?'} ({detail})")
    return _refuse_or_allow(why, allow_older)


def _refuse_or_allow(why: str, allow_older: bool) -> int:
    if allow_older:
        print(f"[build_info] WARNING {why} (--allow-older given)")
        return 0
    print(f"[build_info] REFUSED: {why}. Merge the deployed build into this tree "
          f"first, or rerun with --allow-older.")
    return REFUSED


def record() -> int:
    head = _head()
    if not head:
        print("[build_info] nothing recorded: no HEAD commit")
        return 1
    info = {
        "commit": head,
        "branch": _git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip(),
        "exes": list(EXES),
        "uncommitted_sources": _uncommitted("*.py", "*.spec"),
        "built_at": datetime.now().isoformat(timespec="seconds"),
    }
    INFO.write_text(json.dumps(info, indent=2), encoding="utf-8")
    print(f"[build_info] recorded {head[:10]} on {info['branch']} for {', '.join(EXES)}, "
          f"{len(info['uncommitted_sources'])} uncommitted source file(s)")
    return 0


def stamp() -> int:
    head = _head()
    if not head:
        print("[build_info] nothing stamped: no HEAD commit")
        return 1
    commit = head + ("+dirty" if _uncommitted("*.py", "*.spec") else "")
    STAMP.write_text("# Written by tools/build_info.py stamp for the build in progress;\n"
                     "# removed by unstamp after it.\n"
                     f'COMMIT = "{commit}"\n', encoding="ascii")
    print(f"[build_info] stamped {commit}")
    return 0


def unstamp() -> int:
    STAMP.unlink(missing_ok=True)
    print("[build_info] stamp removed")
    return 0


def main(argv: list[str]) -> int:
    if argv[:1] == ["check"]:
        return check(allow_older="--allow-older" in argv,
                     allow_migrations="--allow-migrations" in argv)
    if argv[:1] == ["record"]:
        return record()
    if argv[:1] == ["stamp"]:
        return stamp()
    if argv[:1] == ["unstamp"]:
        return unstamp()
    print("usage: build_info.py check [--allow-older] [--allow-migrations] | record"
          " | stamp | unstamp")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
