#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="${1:-$repo_root/dist}"
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

cargo build --release --locked --manifest-path "$repo_root/Cargo.toml"
python3 "$repo_root/scripts/package-release.py" \
  --target "$target" \
  --binary "$repo_root/target/release/grepmesh-mcp" \
  --rg "$rg_bin" \
  --output "$out"
