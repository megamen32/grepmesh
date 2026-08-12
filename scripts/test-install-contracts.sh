#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

bash -n "$repo_root/install.sh"
for path in \
  "$repo_root/install.ps1" \
  "$repo_root/grepmesh-user.service" \
  "$repo_root/grepmesh-user.plist" \
  "$repo_root/scripts/package-release.py" \
  "$repo_root/scripts/smoke-release.py" \
  "$repo_root/.github/workflows/release.yml"; do
  test -f "$path"
done

grep -Fq 'grepmesh-macos-aarch64.tar.gz' "$repo_root/install.sh"
grep -Fq 'grepmesh-macos-x86_64.tar.gz' "$repo_root/install.sh"
grep -Fq 'launchctl bootstrap' "$repo_root/install.sh"
grep -Fq 'grepmesh-windows-x86_64.zip' "$repo_root/install.ps1"
grep -Fq 'schtasks.exe' "$repo_root/install.ps1"
grep -Fq 'x86_64-apple-darwin' "$repo_root/.github/workflows/release.yml"
grep -Fq 'aarch64-apple-darwin' "$repo_root/.github/workflows/release.yml"
grep -Fq 'x86_64-pc-windows-msvc' "$repo_root/.github/workflows/release.yml"
grep -Fq 'Smoke packaged MCP and search' "$repo_root/.github/workflows/release.yml"

echo 'installer contracts passed'
