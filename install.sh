#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
prefix="${GREPMESH_PREFIX:-$HOME/.local}"
config_dir="${XDG_CONFIG_HOME:-$HOME/.config}/grepmesh"

cargo build --release --manifest-path "$repo_root/Cargo.toml"
install -d "$prefix/bin" "$config_dir"
install -m 0755 "$repo_root/target/release/grepmesh-mcp" "$prefix/bin/grepmesh-mcp"
if [[ ! -e "$config_dir/config.json" ]]; then
  install -m 0644 "$repo_root/config.example.json" "$config_dir/config.json"
fi

printf 'Installed %s\n' "$prefix/bin/grepmesh-mcp"
printf 'Edit %s, then run:\n' "$config_dir/config.json"
printf '  %s --config %s\n' "$prefix/bin/grepmesh-mcp" "$config_dir/config.json"
