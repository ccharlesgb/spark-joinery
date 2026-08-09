#!/usr/bin/env bash
# Runs `just check` at the end of each agent turn; blocks completion on failure.
set -uo pipefail

output=$(just check 2>&1)
status=$?

if [ "$status" -ne 0 ]; then
  # Keep the payload bounded in case of noisy output.
  output=$(printf '%s' "$output" | tail -c 8000)
  python3 - "$output" <<'PYEOF'
import json
import sys

output = sys.argv[1]
print(json.dumps({
    "decision": "block",
    "reason": (
        "`just check` failed. Fix the reported lint/type/format/test "
        "failures before finishing.\n\n" + output
    ),
}))
PYEOF
fi
