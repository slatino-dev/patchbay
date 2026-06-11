#!/usr/bin/env bash
# scrub_check.sh - fail (exit 1) if any tracked text file contains a likely
# secret (private keys, cloud / API tokens) or a carrier-grade-NAT address.
# Generic CI / pre-commit guard; ships no project- or host-specific values.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SELF="scripts/scrub_check.sh"
PATTERNS=(
  '-----BEGIN (RSA |EC |OPENSSH |DSA |PGP )?PRIVATE KEY-----'
  '\bAKIA[0-9A-Z]{16}\b'
  '\bgh[pousr]_[A-Za-z0-9]{36}\b'
  '\bsk-(ant-|proj-|or-v1-|live-)?[A-Za-z0-9]{32,}\b'
  '\bxox[baprs]-[0-9A-Za-z-]{12,}\b'
  '\b100\.(6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\.[0-9]{1,3}\.[0-9]{1,3}\b'
)
if git -C "$ROOT" rev-parse --is-inside-work-tree &>/dev/null; then
  mapfile -t FILES < <(git -C "$ROOT" ls-files)
else
  mapfile -t FILES < <(cd "$ROOT" && find . -type f -not -path '*/.git/*' | sed 's|^\./||')
fi
FOUND=0
for file in "${FILES[@]}"; do
  [ "$file" = "$SELF" ] && continue
  case "$file" in
    */dist/*|dist/*|*/node_modules/*|*/target/*|*/build/*|*.lock|*.png|*.jpg|*.jpeg|*.gif|*.ico|*.svg|*.pdf|*.woff|*.woff2|*.ttf|*.otf|*.min.*|*.whl|*.so|*.dll|*.exe|*.gz|*.zip|*.tar) continue;;
  esac
  abs="$ROOT/$file"; [ -f "$abs" ] || continue
  for pat in "${PATTERNS[@]}"; do
    if grep -Eq "$pat" "$abs" 2>/dev/null; then
      echo "SCRUB FAIL [$file]:"; grep -nE "$pat" "$abs" | head -3; FOUND=1
    fi
  done
done
[ "$FOUND" -ne 0 ] && { echo "scrub_check: FAILED - remove secrets before committing."; exit 1; }
echo "scrub_check: OK - no secrets detected."
