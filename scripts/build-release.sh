#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="${1:-$repo_root/dist}"
rg_bin="${RG_BIN:-$(command -v rg)}"

[[ "$(uname -s)" == "Linux" && "$(uname -m)" == "x86_64" ]] || {
  echo "Release builder currently supports Linux x86_64." >&2
  exit 1
}
mkdir -p "$out/stage"
cargo build --release --locked --manifest-path "$repo_root/Cargo.toml"
install -m 0755 "$repo_root/target/release/grepmesh-mcp" "$out/stage/grepmesh-mcp"
install -m 0755 "$rg_bin" "$out/stage/rg"
install -m 0644 "$repo_root/config.example.json" "$out/stage/config.example.json"
install -m 0644 "$repo_root/grepmesh-user.service" "$out/stage/grepmesh-user.service"
tar -C "$out/stage" -czf "$out/grepmesh-linux-x86_64.tar.gz" \
  grepmesh-mcp rg config.example.json grepmesh-user.service
sha256sum "$out/grepmesh-linux-x86_64.tar.gz" > "$out/SHA256SUMS"
