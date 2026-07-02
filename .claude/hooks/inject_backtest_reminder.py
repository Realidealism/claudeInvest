"""UserPromptSubmit hook: inject signal_backtest protocol reminder when
user message contains signal-factory skill trigger phrases.

Triggers: 回測 / 試看看 / 跑一下 / 看效果

Output: JSON with hookSpecificOutput.additionalContext (injected into model
context for the upcoming turn).
"""
import json
import sys

TRIGGERS = ["回測", "試看看", "跑一下", "看效果"]

REMINDER = """[signal-factory protocol reminder]
用戶訊息含「回測/試看看/跑一下/看效果」之一。若下一步要跑 signal_backtest 全套回測：
  MUST: --signals pick,touch,buy,sell,buy_flee,sell_flee,unified_long,unified_short (8 訊號)
  MUST: --workers 16 (或更高)
  Ad-hoc 測試例外: 在指令中加 --ad-hoc token 才能繞過 PreToolUse 檢查
  決策依據: unified_long/unified_short PF（單訊號 PF 不可單獨採用）
參考: feedback_signal_version_workflow.md / .claude/skills/signal-factory/SKILL.md"""


def main():
    try:
        data = json.load(sys.stdin)
    except Exception:
        sys.exit(0)

    # field name may be "prompt" or "user_prompt" depending on harness version
    prompt = data.get("prompt") or data.get("user_prompt") or ""

    if not any(t in prompt for t in TRIGGERS):
        sys.exit(0)

    output = {
        "hookSpecificOutput": {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": REMINDER,
        }
    }
    print(json.dumps(output, ensure_ascii=False))
    sys.exit(0)


if __name__ == "__main__":
    main()
