#!/usr/bin/env bash
# scrub_check.sh — fail if any tracked file leaks internal infrastructure details.
# Exit 1 if a match is found; exit 0 if clean.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PATTERNS=(
    'redacted-host'
    '100\.64\.'
    'ts\.net'
    'redacted-mesh'
    'redacted-key'
    'sk-[A-Za-z0-9]{20,}'
)

FOUND=0

for pattern in "${PATTERNS[@]}"; do
    # Search tracked files only (git ls-files), skipping binary files.
    while IFS= read -r match; do
        echo "SCRUB FAIL [$pattern]: $match"
        FOUND=1
    done < <(git ls-files | xargs grep -rn --include="*" -E "$pattern" 2>/dev/null || true)
done

if [[ "$FOUND" -ne 0 ]]; then
    echo ""
    echo "scrub_check: FAILED — remove the matches above before committing."
    exit 1
fi

echo "scrub_check: clean."
exit 0
