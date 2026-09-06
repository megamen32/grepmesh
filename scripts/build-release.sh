#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="${1:-$repo_root/.tmp/dist}"
rg_bin="${RG_BIN:-$(command -v rg)}"

case "$(uname -s):$(uname -m)" in
  Linux:x86_64) target="linux-x86_64" ;;
  Darwin:arm64) target="macos-aarch64" ;;
  Darwin:x86_64) target="macos-x86_64" ;;
  *)
    echo "Local release builder supports Linux x86_64, macOS arm64, and macOS x86_64." >&2
    exit 1
    ;;
esac

runtime_args=()
if [[ "$target" == linux-x86_64 ]]; then
  "$repo_root/scripts/build-linux-compatible.sh"
  binary="$repo_root/.tmp/linux-compatible-package/grepmesh-mcp"
  runtime_args=(--runtime-dir "$repo_root/.tmp/linux-compatible-package")
else
  export CARGO_TARGET_DIR="$repo_root/.tmp/release-target"
  cargo build --release --locked --manifest-path "$repo_root/Cargo.toml"
  binary="$CARGO_TARGET_DIR/release/grepmesh-mcp"
fi
python3 "$repo_root/scripts/package-release.py" \
  --target "$target" \
  --binary "$binary" \
  --rg "$rg_bin" \
  --output "$out" "${runtime_args[@]}"
