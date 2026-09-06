#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
archive="${1:-$repo_root/.tmp/dist/grepmesh-linux-x86_64.tar.gz}"
mkdir -p "$repo_root/.tmp"
test_root="$(mktemp -d "$repo_root/.tmp/install-canary.XXXXXX")"
trap 'rm -rf "$test_root"' EXIT
for pass in first upgrade; do
  GREPMESH_ARCHIVE="$archive" GREPMESH_PREFIX="$test_root/prefix" \
    GREPMESH_TMP_DIR="$test_root/temp" GREPMESH_START_SERVICE=0 \
    XDG_CONFIG_HOME="$test_root/config" bash "$repo_root/install.sh"
  env -u LD_LIBRARY_PATH "$test_root/prefix/bin/grepmesh-mcp" --help >/dev/null
  test -f "$test_root/prefix/share/grepmesh/licenses/onnxruntime/LICENSE"
  test -f "$test_root/prefix/share/grepmesh/onnxruntime-manifest.txt"
  test ! -d "$test_root/config/systemd"
  echo "$pass install: native binary loads with bundled runtime; service not started"
done
