#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="${1:-$repo_root/.tmp/dist}"
rg_bin="${RG_BIN:-$(command -v rg || true)}"
if [[ -z "$rg_bin" || ! -x "$rg_bin" ]]; then
  echo 'ripgrep is required to package a GrepMesh release; install rg or set RG_BIN.' >&2
  exit 1
fi

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
  if [[ "$target" == macos-aarch64 ]]; then
    cargo build --release --locked --features ocr-coreml --manifest-path "$repo_root/Cargo.toml"
  else
    bash "$repo_root/scripts/prepare-macos-intel-runtime.sh"
    export ORT_LIB_PATH="$repo_root/.tmp/macos-intel-runtime/lib"
    export ORT_PREFER_DYNAMIC_LINK=1 ORT_SKIP_DOWNLOAD=1
    export RUSTFLAGS="${RUSTFLAGS:-} -C link-arg=-Wl,-rpath,@executable_path/lib"
    cargo build --release --locked --no-default-features --features ocr --manifest-path "$repo_root/Cargo.toml"
    runtime_args=(--runtime-dir "$repo_root/.tmp/macos-intel-runtime")
  fi
  binary="$CARGO_TARGET_DIR/release/grepmesh-mcp"
fi
if [[ "$target" == macos-aarch64 ]]; then
  # macOS ships Bash 3.2, where an empty array expansion fails under `set -u`.
  python3 "$repo_root/scripts/package-release.py" \
    --target "$target" \
    --binary "$binary" \
    --rg "$rg_bin" \
    --output "$out"
else
  python3 "$repo_root/scripts/package-release.py" \
    --target "$target" \
    --binary "$binary" \
    --rg "$rg_bin" \
    --output "$out" "${runtime_args[@]}"
fi
