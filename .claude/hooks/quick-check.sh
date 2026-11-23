#!/usr/bin/env bash
# Stop: check only the files edited in this session (recorded by format.sh), so pre-existing
# lint/type errors elsewhere and other sessions' uncommitted work never block anything.
# Exit 2 sends the errors back to Claude, which keeps working.
input=$(cat)
[ "$(echo "$input" | jq -r '.stop_hook_active')" = "true" ] && exit 0   # avoid infinite loops
sid=$(echo "$input" | jq -r '.session_id // "x"')
list="${TMPDIR:-/tmp}/asw-edited-$sid.txt"
[ -s "$list" ] || exit 0
files=$(sort -u "$list")
out=""
tsc_repos=()
while IFS= read -r f; do
  [ -f "$f" ] || continue
  repo=$(git -C "$(dirname "$f")" rev-parse --show-toplevel 2>/dev/null) || continue
  case "$f" in
    */src/constants.py) ;;
    *.py)
      [ -x "$repo/venv/bin/python" ] || continue
      r=$("$repo/venv/bin/python" -m py_compile "$f" 2>&1) || out+="== $f\n$r\n"
      r=$("$repo/venv/bin/black" --check -q --line-length=120 "$f" 2>&1) || out+="== $f (black)\n$r\n" ;;
    *.ts|*.vue)
      [ -d "$repo/node_modules" ] || continue
      r=$(cd "$repo" && npx eslint --max-warnings=0 "$f" 2>&1) || out+="== $f (eslint)\n$r\n"
      tsc_repos+=("$repo") ;;
  esac
done <<< "$files"
# fe type check once per repo; report only lines that mention a file edited this session
if [ "${#tsc_repos[@]}" -gt 0 ]; then
  for repo in $(printf '%s\n' "${tsc_repos[@]}" | sort -u); do
    r=$(cd "$repo" && npx vue-tsc --build 2>&1)
    while IFS= read -r f; do
      case "$f" in "$repo"/*) ;; *) continue ;; esac
      rel="${f#$repo/}"
      m=$(printf '%s\n' "$r" | grep -F "$rel(") && out+="== $rel (vue-tsc)\n$m\n"
    done <<< "$files"
  done
fi
if [ -n "$out" ]; then
  printf '%b' "Checks failed on files edited this session. Fix before finishing:\n$out" | tail -c 4000 >&2
  exit 2
fi
rm -f "$list"
exit 0
