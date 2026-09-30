# -*- coding: utf-8 -*-
"""Run the classification pipeline as one gate: a failed step stops the run.

The reason this exists is a specific failure. On 2026-09-08 a syntax error in
db/industry_data.py made db.seed_industry abort, and the three commands after it
-- fill_weight, check_industry, pytest -- all reported success, because they read
the database rather than the file. Every number was unchanged, which looked like
"the edit was safe" and actually meant "nothing was written". A green light at
the end of a chain says the last command succeeded; it says nothing about
whether the change under test ever reached the database.

So: each step runs only if the previous one exited 0, and the summary names the
step that stopped it. Nothing here is clever -- the point is that it is not
possible to read the tail of the output and miss the failure.

    python -m db.verify              # seed -> fill -> check -> pytest
    python -m db.verify --no-tests   # skip pytest when iterating
    python -m db.verify --check-only # read-only: check_industry and pytest

Exit code is 0 only if every step ran and passed.
"""
import argparse
import subprocess
import sys
import time

PY = sys.executable


def _steps(args):
    yield ("seed_skeleton --check", [PY, "-m", "db.seed_skeleton", "--check"], False)
    if not args.check_only:
        yield ("seed_skeleton", [PY, "-m", "db.seed_skeleton"], False)
        yield ("seed_industry", [PY, "-m", "db.seed_industry"], True)
        yield ("fill_weight", [PY, "-m", "db.fill_weight"], True)
    yield ("check_industry", [PY, "-m", "db.check_industry"], True)
    if not args.no_tests:
        yield ("pytest", [PY, "-m", "pytest", "-q"], True)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-tests", action="store_true", help="skip the test suite")
    ap.add_argument("--check-only", action="store_true",
                    help="write nothing: run only check_industry and the tests")
    args = ap.parse_args()

    done = []
    for name, cmd, keep_tail in _steps(args):
        started = time.time()
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              encoding="utf-8", errors="replace")
        took = time.time() - started
        out = (proc.stdout or "") + (proc.stderr or "")
        tail = [ln for ln in out.splitlines() if ln.strip()][-3:] if keep_tail else []

        if proc.returncode != 0:
            print("FAILED at step %d/%d: %s (exit %d)"
                  % (len(done) + 1, len(list(_steps(args))), name, proc.returncode))
            print("-" * 70)
            print("\n".join(out.splitlines()[-25:]))
            print("-" * 70)
            if done:
                print("steps that had already run: " + ", ".join(done))
            # The whole point: say plainly that later steps prove nothing here.
            print("\nThe steps after this one were NOT run. If you run them by hand "
                  "they may well pass\nagainst the database as it stands -- that "
                  "would say nothing about this change,\nbecause the change never "
                  "reached the database.")
            raise SystemExit(1)

        done.append(name)
        print("ok  %-22s %5.1fs" % (name, took))
        for ln in tail:
            print("      %s" % ln)

    print("\nall %d steps passed" % len(done))


if __name__ == "__main__":
    main()
