#!/usr/bin/env bash
set -euo pipefail

repo="${GREPMESH_REPO:-megamen32/grepmesh}"
version="${GREPMESH_VERSION:-latest}"
prefix="${GREPMESH_PREFIX:-$HOME/.local}"
config_dir="${XDG_CONFIG_HOME:-$HOME/.config}/grepmesh"
os="$(uname -s)"
arch="$(uname -m)"

case "$os:$arch" in
  Linux:x86_64) asset="grepmesh-linux-x86_64.tar.gz" ;;
  Darwin:arm64) asset="grepmesh-macos-aarch64.tar.gz" ;;
  Darwin:x86_64) asset="grepmesh-macos-x86_64.tar.gz" ;;
  *)
    echo "GrepMesh binary releases support Linux x86_64, macOS arm64, and macOS x86_64." >&2
    exit 1
    ;;
esac

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

install -d "$prefix/bin" "$config_dir"
install -m 0755 "$tmp/grepmesh-mcp" "$prefix/bin/grepmesh-mcp"
install -m 0755 "$tmp/rg" "$prefix/bin/rg"
if [[ ! -e "$config_dir/config.json" ]]; then
  sed "s|/home/user/projects|$HOME|g" "$tmp/config.example.json" > "$config_dir/config.json"
fi

if [[ "$os" == "Linux" ]]; then
  service_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
  install -d "$service_dir"
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
else
  launch_agents="$HOME/Library/LaunchAgents"
  plist="$launch_agents/com.grepmesh.mcp.plist"
  install -d "$launch_agents"
  sed \
    -e "s|@GREPMESH_BIN@|$prefix/bin/grepmesh-mcp|g" \
    -e "s|@GREPMESH_CONFIG@|$config_dir/config.json|g" \
    -e "s|@GREPMESH_PATH@|$prefix/bin:/usr/local/bin:/usr/bin:/bin|g" \
    -e "s|@GREPMESH_LOG@|$config_dir/grepmesh.log|g" \
    "$tmp/grepmesh-user.plist" > "$plist"
  launchctl bootout "gui/$(id -u)" "$plist" >/dev/null 2>&1 || true
  launchctl bootstrap "gui/$(id -u)" "$plist"
  launchctl kickstart -k "gui/$(id -u)/com.grepmesh.mcp"
  state="started"
fi

printf 'GrepMesh installed and %s at http://127.0.0.1:9419/mcp\n' "$state"
