"""Check the byte budgets and layout of the instruction files.

Usage:  python c:/Claude/Invest/.claude/ref/check_budget.py
Exit code 0 when every line is PASS, 1 when any line is FAIL. Read-only.
Budgets are defined in .claude/rules/04_maintenance.md; change them there first.
"""
import glob
import os
import re
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MEMORY = os.path.join(
    os.path.expanduser("~"), ".claude", "projects", "c--Claude-Invest", "memory", "MEMORY.md"
)

FILE_BUDGETS = [
    ("CLAUDE.md", 10000),
    (".claude/rules/01_dispatch.md", 10000),
    (".claude/rules/02_judgment.md", 16000),
    (".claude/rules/04_maintenance.md", 9000),
    (".claude/rules/06_service_safety.md", 9000),
]
REF_BUDGET = 60000
MEMORY_BUDGET = 23000
MEMORY_LINE_BUDGET = 450
MEMORY_MAX_LINES = 180  # the harness loads only the first 200 lines
LOG_ENTRY_BUDGET = 600  # applies to pitfall-log entries dated after LOG_ENTRY_SINCE
LOG_ENTRY_SINCE = "2026-11-11"
# Rule files allowed in .claude/rules/ -> must it be path-scoped?
ALLOWED_RULES = {
    "01_dispatch.md": False,
    "02_judgment.md": False,
    "04_maintenance.md": True,
    "06_service_safety.md": True,
}

failed = False


def report(ok, label, detail):
    global failed
    if not ok:
        failed = True
    print(f"{'PASS' if ok else 'FAIL'}  {label}  {detail}")


def read_paths(text):
    """Return the paths: list from YAML frontmatter, or None when the file has no frontmatter."""
    if not text.startswith("---"):
        return None
    parts = text.split("\n---", 1)
    if len(parts) < 2:
        return None
    patterns, in_paths = [], False
    for line in parts[0].split("\n")[1:]:
        if line.strip() == "paths:":
            in_paths = True
        elif in_paths and line.strip().startswith("- "):
            patterns.append(line.strip()[2:].strip().strip("\"'"))
        elif line.strip():
            in_paths = False
    return patterns


def glob_to_regex(pattern):
    """Translate a gitignore-style glob; a pattern without '/' matches at any depth."""
    anywhere = "/" not in pattern
    out, i = "", 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out += "(?:.*/)?"
            i += 3
        elif pattern.startswith("**", i):
            out += ".*"
            i += 2
        elif pattern[i] == "*":
            out += "[^/]*"
            i += 1
        else:
            out += re.escape(pattern[i])
            i += 1
    return re.compile(("(?:.*/)?" if anywhere else "") + out + "$")


def repo_files():
    """Tracked and untracked files, plus the instruction files that .gitignore hides."""
    try:
        res = subprocess.run(
            ["git", "-C", REPO, "ls-files", "--cached", "--others", "--exclude-standard"],
            capture_output=True, text=True, encoding="utf-8", errors="replace", check=True,
        )
    except Exception as exc:  # git missing or not a repository
        print(f"INFO  git ls-files unavailable ({exc.__class__.__name__}); path patterns not checked")
        return None
    files = [f for f in res.stdout.split("\n") if f]
    for sub in ("rules", "ref"):
        for path in glob.glob(os.path.join(REPO, ".claude", sub, "**", "*"), recursive=True):
            if os.path.isfile(path):
                files.append(os.path.relpath(path, REPO).replace(os.sep, "/"))
    return files


for rel, budget in FILE_BUDGETS:
    path = os.path.join(REPO, rel)
    if not os.path.isfile(path):
        report(False, rel, "missing")
        continue
    n = os.path.getsize(path)
    report(n <= budget, rel, f"{n} / {budget} bytes")

# Every .md under .claude/rules/ loads at session start unless it has paths: frontmatter.
all_files = repo_files()
rules_dir = os.path.join(REPO, ".claude", "rules")
for path in sorted(glob.glob(os.path.join(rules_dir, "**", "*.md"), recursive=True)):
    name = os.path.relpath(path, rules_dir).replace(os.sep, "/")
    label = f".claude/rules/{name}"
    with open(path, encoding="utf-8") as fh:
        patterns = read_paths(fh.read(4000))
    if name not in ALLOWED_RULES:
        report(False, label, "unexpected file in rules/ (see 04_maintenance.md before adding one)")
    elif ALLOWED_RULES[name] and not patterns:
        report(False, label, "must have a non-empty paths: frontmatter")
    elif not ALLOWED_RULES[name] and patterns is not None:
        report(False, label, "must not have frontmatter (always-loaded file)")
    elif not patterns:
        report(True, label, "always loaded")
    elif all_files is None:
        report(True, label, f"path-scoped, {len(patterns)} pattern(s), not checked against files")
    else:
        dead = [p for p in patterns if not any(glob_to_regex(p).match(f) for f in all_files)]
        report(not dead, label, f"path-scoped, {len(patterns)} pattern(s)"
               + ("" if not dead else "; matches no file: " + ", ".join(dead)))

for path in sorted(glob.glob(os.path.join(REPO, ".claude", "ref", "*.md"))):
    n = os.path.getsize(path)
    report(n <= REF_BUDGET, ".claude/ref/" + os.path.basename(path), f"{n} / {REF_BUDGET} bytes")

# Pitfall-log entries written after the v2 restructure must stay short.
cases = os.path.join(REPO, ".claude", "ref", "02_cases.md")
if os.path.isfile(cases):
    with open(cases, encoding="utf-8") as fh:
        entries = [ln for ln in fh.read().split("\n") if re.match(r"- \d{4}-\d{2}-\d{2} \|", ln)]
    long_new = [ln[2:12] for ln in entries
                if ln[2:12] > LOG_ENTRY_SINCE and len(ln.encode("utf-8")) > LOG_ENTRY_BUDGET]
    report(not long_new, "pitfall log", f"{len(entries)} entries; over {LOG_ENTRY_BUDGET} bytes "
           f"since {LOG_ENTRY_SINCE}: {len(long_new)}" + ("" if not long_new else " (" + ", ".join(long_new) + ")"))

if not os.path.isfile(MEMORY):
    report(False, "MEMORY.md", f"missing at {MEMORY}")
else:
    n = os.path.getsize(MEMORY)
    report(n <= MEMORY_BUDGET, "MEMORY.md", f"{n} / {MEMORY_BUDGET} bytes")
    with open(MEMORY, encoding="utf-8") as fh:
        lines = fh.read().split("\n")
    report(len(lines) <= MEMORY_MAX_LINES, "MEMORY.md line count", f"{len(lines)} / {MEMORY_MAX_LINES}")
    long_lines = [
        (i + 1, len(ln.encode("utf-8")))
        for i, ln in enumerate(lines)
        if len(ln.encode("utf-8")) > MEMORY_LINE_BUDGET
    ]
    report(
        not long_lines,
        "MEMORY.md lines",
        f"{len(long_lines)} line(s) over {MEMORY_LINE_BUDGET} bytes"
        + ("" if not long_lines else ": " + ", ".join(f"L{i}={b}" for i, b in long_lines[:20])),
    )
    # The live baseline line carries the bold marker; other lines may quote the phrase unbolded.
    current = [i + 1 for i, ln in enumerate(lines) if "**(當前)**" in ln]
    report(len(current) == 1, "MEMORY.md baseline",
           f"{len(current)} line(s) carry the bold current-baseline marker, expected 1")

always = sum(
    os.path.getsize(os.path.join(REPO, rel))
    for rel in ("CLAUDE.md", ".claude/rules/01_dispatch.md", ".claude/rules/02_judgment.md")
    if os.path.isfile(os.path.join(REPO, rel))
)
print(f"INFO  always-loaded rules total: {always} bytes (MEMORY.md not included)")
sys.exit(1 if failed else 0)
