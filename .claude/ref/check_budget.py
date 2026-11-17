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
    (".claude/rules/07_signal_memory.md", 3000),
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
    "07_signal_memory.md": True,
}
# Signal-factory topic index: entries moved out of MEMORY.md live here.
TOPIC_INDEX = os.path.join(os.path.dirname(MEMORY), "INDEX_signal_factory.md")
TOPIC_POINTER = "(INDEX_signal_factory.md)"
# Section titles of the signal-factory topic index, in order. Changing a title in the
# index means changing it here too; that is deliberate.
INDEX_SECTIONS = [
    "失敗封存",
    "已採用的結論、不對稱性、架構",
    "上游相依、評估方法、資料修正",
    "規則參考",
    "歷史結論（v94 時期）",
    "跨條目衝突（來源之間沒有調和；讀到相關條目時一併讀）",
    "待查與證據限制",
]
# The reading rule states the entry and section counts on exactly one line of this form.
INDEX_STATEMENT_KEY = "讀完全文的判準："
INDEX_STATEMENT_RE = (
    r"- 讀完全文的判準：本索引共 (\d+) 條、(\d+) 節，節標題依序是「([^」]*)」。"
    r"最後一行是以「=== 索引結束：」開頭的結尾標記，它寫的條數、節數與這裡相同。"
)

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
    # MEMORY.md must point at the topic index, and the index must exist and be non-empty.
    pointers = sum(1 for ln in lines if TOPIC_POINTER in ln)
    report(pointers == 1, "MEMORY.md topic pointer",
           f"{pointers} line(s) link INDEX_signal_factory.md, expected 1")
    index_size = os.path.getsize(TOPIC_INDEX) if os.path.isfile(TOPIC_INDEX) else 0
    report(index_size > 0, "INDEX_signal_factory.md",
           f"{index_size} bytes" if index_size else f"missing or empty at {TOPIC_INDEX}")
    # The topic index must be verifiably complete. Three places have to agree: the
    # statement in the reading rule, the closing marker, and the entries and sections present.
    if index_size:
        with open(TOPIC_INDEX, encoding="utf-8") as fh:
            index_text = fh.read().replace("\r\n", "\n")
        # The marker is the last line: at most one terminating newline may follow it.
        body = index_text[:-1] if index_text.endswith("\n") else index_text
        index_lines = body.split("\n")
        end = re.fullmatch(r"=== 索引結束：共 (\d+) 條、(\d+) 節 ===", index_lines[-1])
        report(bool(end), "INDEX end marker",
               "closing marker is the last line" if end else "closing marker missing or not the last line")
        entries = [int(m.group(1)) for m in (re.match(r"\*\*(\d+)\. \[", ln) for ln in index_lines) if m]
        heads = [m for m in (re.fullmatch(r"## (\d+)\. (.+)", ln) for ln in index_lines) if m]
        sections_ok = ([int(m.group(1)) for m in heads] == list(range(1, len(INDEX_SECTIONS) + 1))
                       and [m.group(2) for m in heads] == INDEX_SECTIONS)
        report(sections_ok, "INDEX sections",
               f"{len(heads)} section heading(s); numbers, titles and order "
               + ("match" if sections_ok else "differ from") + " INDEX_SECTIONS")
        # Exactly one line may carry the statement, and that line must parse in full.
        candidates = [ln for ln in index_lines if INDEX_STATEMENT_KEY in ln]
        report(len(candidates) == 1, "INDEX header statement count",
               f"{len(candidates)} line(s) contain the reading-rule statement, expected 1")
        stated = re.fullmatch(INDEX_STATEMENT_RE, candidates[0]) if len(candidates) == 1 else None
        if len(candidates) == 1:
            report(bool(stated), "INDEX header statement format",
                   "statement line parses in full" if stated else "statement line does not match the fixed form")
        if end and stated:
            n_entries, n_sections = int(end.group(1)), int(end.group(2))
            report(entries == list(range(1, n_entries + 1)), "INDEX entries",
                   f"{len(entries)} numbered entries, marker says {n_entries}")
            counts_ok = ((int(stated.group(1)), int(stated.group(2))) == (n_entries, n_sections)
                         and n_sections == len(INDEX_SECTIONS))
            report(counts_ok, "INDEX counts",
                   "header statement, marker and INDEX_SECTIONS " + ("agree" if counts_ok else "disagree"))
            report(stated.group(3).split("｜") == INDEX_SECTIONS, "INDEX declared section titles",
                   "titles listed in the reading rule compared with INDEX_SECTIONS")

always = sum(
    os.path.getsize(os.path.join(REPO, rel))
    for rel in ("CLAUDE.md", ".claude/rules/01_dispatch.md", ".claude/rules/02_judgment.md")
    if os.path.isfile(os.path.join(REPO, rel))
)
print(f"INFO  always-loaded rules total: {always} bytes (MEMORY.md not included)")
sys.exit(1 if failed else 0)
