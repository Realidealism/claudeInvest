"""PreToolUse hook: enforce signal_backtest protocol.

Blocks Bash commands invoking `signal_backtest --signals` that:
- Don't include all 8 required signals, OR
- Use --workers < 16

Escape hatch: include `--ad-hoc` token in the command to bypass.
"""
import json
import sys
import re

REQUIRED = {"pick", "touch", "buy", "sell", "buy_flee", "sell_flee",
            "unified_long", "unified_short"}
MIN_WORKERS = 16


def main():
    try:
        data = json.load(sys.stdin)
    except Exception:
        sys.exit(0)

    if data.get("tool_name") != "Bash":
        sys.exit(0)

    cmd = (data.get("tool_input") or {}).get("command", "") or ""

    if "signal_backtest" not in cmd or "--signals" not in cmd:
        sys.exit(0)

    if "--ad-hoc" in cmd:
        sys.exit(0)

    sig_m = re.search(r'--signals\s+([\w_,]+)', cmd)
    signals = set(sig_m.group(1).split(",")) if sig_m else set()
    missing = REQUIRED - signals

    workers_m = re.search(r'--workers\s+(\d+)', cmd)
    workers = int(workers_m.group(1)) if workers_m else 1

    errors = []
    if missing:
        errors.append(
            f"signal_backtest 必須含 8 訊號，缺：{','.join(sorted(missing))}"
        )
    if workers < MIN_WORKERS:
        errors.append(
            f"signal_backtest --workers 必須 >= {MIN_WORKERS}（當前 {workers}）"
        )

    if errors:
        msg = "\n".join(errors) + "\n（要繞過此檢查，在指令中加 --ad-hoc token）"
        print(msg, file=sys.stderr)
        sys.exit(2)

    sys.exit(0)


if __name__ == "__main__":
    main()
