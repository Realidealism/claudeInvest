# -*- coding: utf-8 -*-
"""Two assertions about the migration directory, in both directions.

A migration file is the only written record of a change that has already been
made to the production database. init_db() reads the directory from disk and
keys on the filename against public.schema_migrations, so git has no say in what
runs -- which is exactly why nothing noticed when files stopped being committed.

    registered -> on disk    every filename in schema_migrations still has a file
    on disk    -> tracked    every .sql in the directory is under version control

The first direction found three files that are gone for good:
050_add_social_subscriptions.sql, 051_intraday_push_state.sql and
057_add_social_pushed.sql were re-executed by the 2026-06-11 baseline run and
were already absent from the 2026-08-10 build. They are not on disk, not
anywhere in git history, and not in any surviving exe -- the bundled TOCs run
001-049, 052-056, 058-113 unbroken. What those DDLs said is unrecoverable; the
live schema is the only trace. They are listed in ACKNOWLEDGED_LOST so that a
settled loss does not fail this check forever, because a gate that is always red
hides the next real finding. A fourth missing file is a finding.

The second direction is the one that would have caught the cause rather than the
symptom. The gap was not made by whatever deleted those three: 7b08bcd6 added
052 on 2026-06-10 while 050 and 051 sat untracked beside it, and 085 and 102-113
repeated that for months. This assertion fails the moment a migration exists
without being tracked, which is the day it happens rather than the day someone
goes looking.

Deliberately not asserted: a file on disk that is NOT registered. That is what a
new migration looks like before it runs, so failing on it would fail every commit
that adds one. init_db() is what decides to apply it.

Exit codes: 0 clean, 1 findings, 2 a precondition is missing and the check could
not run. A zero is only reported when both directions actually ran -- a check
that cannot reach git has not passed, it has abstained.

    python -m db.check_migrations
"""
import io
import os
import subprocess
import sys

# Registered migrations whose files are gone and have been ruled unrecoverable.
# Each one must be documented in LOST_DOC, and that is asserted, so this list
# cannot quietly grow into an excuse.
ACKNOWLEDGED_LOST = (
    "050_add_social_subscriptions.sql",
    "051_intraday_push_state.sql",
    "057_add_social_pushed.sql",
)

MIGRATION_DIR = os.path.join(os.path.dirname(__file__), "migrations")
LOST_DOC = os.path.join(MIGRATION_DIR, "LOST.md")


def _disk():
    return sorted(f for f in os.listdir(MIGRATION_DIR) if f.endswith(".sql"))


def _registered():
    from db.connection import get_cursor
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT filename FROM public.schema_migrations")
        return {r["filename"] for r in cur.fetchall()}


def _tracked():
    """Filenames git has under version control, or None if git could not answer.

    None is not an empty set. Treating an unavailable git as "nothing is tracked"
    would report every migration as a finding; treating it as "everything is
    tracked" would report a clean run over a check that never happened.
    """
    try:
        proc = subprocess.run(["git", "ls-files", "db/migrations"],
                              cwd=os.path.dirname(os.path.dirname(__file__)),
                              capture_output=True, text=True, encoding="utf-8")
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    return {os.path.basename(p) for p in proc.stdout.split("\n") if p.strip()}


def check_registered_on_disk(disk, registered):
    """Registered migrations whose file no longer exists, minus the settled ones."""
    missing = sorted(registered - set(disk))
    return ([f for f in missing if f not in ACKNOWLEDGED_LOST],
            [f for f in missing if f in ACKNOWLEDGED_LOST])


def check_disk_tracked(disk, tracked):
    """Migration files that exist but are not under version control."""
    return sorted(f for f in disk if f not in tracked)


def check_lost_documented():
    """Every acknowledged loss has to be named in LOST.md.

    The allowlist above is what stops a settled loss failing the run. Requiring
    the document to name each entry is what stops the allowlist being the whole
    of the record.
    """
    if not os.path.exists(LOST_DOC):
        return ["LOST.md is missing"] if ACKNOWLEDGED_LOST else []
    body = io.open(LOST_DOC, encoding="utf-8").read()
    return ["%s is allowed for but not documented in LOST.md" % f
            for f in ACKNOWLEDGED_LOST if f not in body]


def main():
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    disk = _disk()
    registered = _registered()
    tracked = _tracked()

    gone, settled = check_registered_on_disk(disk, registered)
    undocumented = check_lost_documented()

    print("%d migration files on disk, %d registered in schema_migrations"
          % (len(disk), len(registered)))

    print("\n[registered -> on disk] %d" % len(gone))
    for f in gone:
        print("    registered but the file is gone:", f)
    for f in settled:
        print("  . settled permanent loss (see db/migrations/LOST.md):", f)
    for problem in undocumented:
        print("    ", problem)

    if tracked is None:
        print("\n[on disk -> tracked] UNVERIFIED -- git could not be reached, so "
              "nothing is known\n    about whether these files are under version "
              "control. This is not a pass.")
        untracked = []
    else:
        untracked = check_disk_tracked(disk, tracked)
        print("\n[on disk -> tracked] %d" % len(untracked))
        for f in untracked:
            print("    exists but is not tracked by git:", f)

    findings = len(gone) + len(untracked) + len(undocumented)
    if tracked is None:
        print("\nUNVERIFIED: one direction could not run.")
        raise SystemExit(2)
    if findings:
        print("\n%d finding(s). A migration that has run against the database and "
              "is not in git\nhas no written record outside this working tree."
              % findings)
        raise SystemExit(1)
    print("\nboth directions clean (%d settled loss(es) on record)" % len(settled))


if __name__ == "__main__":
    main()
