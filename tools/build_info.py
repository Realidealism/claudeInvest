"""What rebuild_and_restart.bat is about to build, and what it built.

  check  -- before a build. Refuses (exit 5) when the commit recorded for the
            exes now in dist\\ is not in HEAD's history: building would drop
            whatever that deployment carried. On 2026-10-15 four rebuilds
            from this tree replaced, unnoticed, a deployment built from two
            branches the tree did not have. --allow-older overrides.
            Uncommitted .py/.spec files are listed as a warning only: other
            lines of work often have some in progress.
  record -- after a successful build, writes dist\\BUILD_INFO.json (commit,
            branch, the uncommitted source files that went in, time).

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
REFUSED = 5


def _git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True,
                          encoding="utf-8", errors="replace")


def _head() -> str:
    return _git("rev-parse", "HEAD").stdout.strip()


def _uncommitted_sources() -> list[str]:
    out = _git("status", "--porcelain", "--", "*.py", "*.spec").stdout
    return sorted(line[3:] for line in out.splitlines() if line.strip())


def check(allow_older: bool = False) -> int:
    for path in _uncommitted_sources():
        print(f"[build_info] WARNING uncommitted source goes into this build: {path}")
    if not INFO.exists():
        print("[build_info] no dist\\BUILD_INFO.json yet; nothing to compare against")
        return 0
    deployed = json.loads(INFO.read_text(encoding="utf-8")).get("commit") or ""
    head = _head()
    if _git("merge-base", "--is-ancestor", deployed, head).returncode == 0:
        print(f"[build_info] ok: the deployed build {deployed[:10]} is in HEAD {head[:10]}")
        return 0
    why = (f"the deployed build {deployed[:10] or '?'} is not in the history of "
           f"HEAD {head[:10]}; building now would drop what it carried")
    if allow_older:
        print(f"[build_info] WARNING {why} (--allow-older given)")
        return 0
    print(f"[build_info] REFUSED: {why}. Merge it into this tree first, "
          f"or rerun with --allow-older.")
    return REFUSED


def record() -> int:
    info = {
        "commit": _head(),
        "branch": _git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip(),
        "uncommitted_sources": _uncommitted_sources(),
        "built_at": datetime.now().isoformat(timespec="seconds"),
    }
    INFO.write_text(json.dumps(info, indent=2), encoding="utf-8")
    print(f"[build_info] recorded {info['commit'][:10]} on {info['branch']}, "
          f"{len(info['uncommitted_sources'])} uncommitted source file(s)")
    return 0


def main(argv: list[str]) -> int:
    if argv[:1] == ["check"]:
        return check(allow_older="--allow-older" in argv)
    if argv[:1] == ["record"]:
        return record()
    print("usage: build_info.py check [--allow-older] | record")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
