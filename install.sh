#!/usr/bin/env bash
set -euo pipefail

repo="${GREPMESH_REPO:-megamen32/grepmesh}"
version="${GREPMESH_VERSION:-latest}"
prefix="${GREPMESH_PREFIX:-$HOME/.local}"
config_dir="${XDG_CONFIG_HOME:-$HOME/.config}/grepmesh"
service_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
asset="grepmesh-linux-x86_64.tar.gz"

[[ "$(uname -s)" == "Linux" && "$(uname -m)" == "x86_64" ]] || {
  echo "GrepMesh binary release currently supports Linux x86_64." >&2
  exit 1
}
for command in curl tar; do
  command -v "$command" >/dev/null || { echo "Missing required command: $command" >&2; exit 1; }
done

if [[ "$version" == "latest" ]]; then
  url="https://github.com/$repo/releases/latest/download/$asset"
else
  url="https://github.com/$repo/releases/download/$version/$asset"
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
curl -fsSL "$url" -o "$tmp/$asset"
tar -xzf "$tmp/$asset" -C "$tmp"

install -d "$prefix/bin" "$config_dir" "$service_dir"
install -m 0755 "$tmp/grepmesh-mcp" "$prefix/bin/grepmesh-mcp"
install -m 0755 "$tmp/rg" "$prefix/bin/rg"
if [[ ! -e "$config_dir/config.json" ]]; then
  sed "s|/home/user/projects|$HOME|g" "$tmp/config.example.json" > "$config_dir/config.json"
fi
sed \
  -e "s|@GREPMESH_BIN@|$prefix/bin/grepmesh-mcp|g" \
  -e "s|@GREPMESH_CONFIG@|$config_dir/config.json|g" \
  -e "s|@GREPMESH_PATH@|$prefix/bin:/usr/local/bin:/usr/bin:/bin|g" \
  "$tmp/grepmesh-user.service" > "$service_dir/grepmesh.service"

if command -v systemctl >/dev/null && systemctl --user show-environment >/dev/null 2>&1; then
  systemctl --user daemon-reload
  systemctl --user enable --now grepmesh.service
  state="$(systemctl --user is-active grepmesh.service)"
else
  nohup env PATH="$prefix/bin:$PATH" "$prefix/bin/grepmesh-mcp" \
    --config "$config_dir/config.json" > "$config_dir/grepmesh.log" 2>&1 &
  state="started"
fi

printf 'GrepMesh installed and %s at http://127.0.0.1:9419/mcp\n' "$state"
