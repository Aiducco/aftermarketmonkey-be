#!/usr/bin/env bash
# PostToolUse (Edit|Write): record the edited file for the Stop hook, and black-format
# Python files in repos that have a venv (be). fe is deliberately not auto-formatted:
# the repo is not prettier-clean and prettier/eslint --fix would rewrite whole files.
input=$(cat)
f=$(echo "$input" | jq -r '.tool_input.file_path // empty')
sid=$(echo "$input" | jq -r '.session_id // "x"')
[ -n "$f" ] && [ -f "$f" ] || exit 0
repo=$(git -C "$(dirname "$f")" rev-parse --show-toplevel 2>/dev/null) || exit 0
echo "$f" >> "${TMPDIR:-/tmp}/asw-edited-$sid.txt"
case "$f" in
  */src/constants.py) ;;   # known not black-clean: leave alone
  *.py)
    [ -x "$repo/venv/bin/black" ] && "$repo/venv/bin/black" -q --line-length=120 "$f" ;;
esac >/dev/null 2>&1
exit 0
