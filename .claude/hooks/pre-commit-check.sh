#!/bin/bash
# Auto-run cargo fmt before any git commit command
set -e

INPUT=$(cat)
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // empty')

# Only intercept git commit commands
if [[ ! "$COMMAND" =~ ^git\ commit ]] && [[ ! "$COMMAND" =~ git\ add.*\&\&.*git\ commit ]]; then
  exit 0
fi

cd /Users/danhlephuoc/code/cqels-rs

# Auto-format before commit
~/.cargo/bin/cargo fmt --all 2>/dev/null || true

# Stage any formatting changes
git add -u 2>/dev/null || true

exit 0
