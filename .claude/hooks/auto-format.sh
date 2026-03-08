#!/bin/bash
# Auto-format Rust files after edit/write operations
set -e

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

# Only format Rust files
if [[ ! "$FILE_PATH" =~ \.rs$ ]]; then
  exit 0
fi

# Run cargo fmt on the workspace
cd /Users/danhlephuoc/code/cqels-rs
~/.cargo/bin/cargo fmt --all 2>/dev/null || true

exit 0
