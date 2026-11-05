#!/usr/bin/env bash
# Resume only after the current builder has stopped; private checkpoint only.
set -euo pipefail
case "${GREPMESH_BOUNDED_RELEASE:-baseline}" in
  baseline) base=/home/roomhacker/.local/lib/grepmesh-native100-bounded-ad91dd1; run=/run/user/1000/grepmesh-native100-bounded-l04-v2 ;;
  optimized) base=/home/roomhacker/.local/lib/grepmesh-native100-bounded-632d940; run=/run/user/1000/grepmesh-native100-bounded-l04-v3 ;;
  *) echo 'Unknown bounded builder release' >&2; exit 1 ;;
esac
if systemctl --user list-units --type=service --state=active --no-legend 'grepmesh-native100-bounded-l04-*.service' | grep -q .; then
  echo 'The existing builder is still active; refusing duplicate.' >&2
  exit 1
fi
(cd "$base" && sha256sum -c SHA256SUMS >/dev/null)
umask 077
mkdir -p "$run/code" "$run/work/verified-source-cache"
for src in "$base"/code/*.py; do
  dest="$run/code/${src##*/}"
  if [ -e "$dest" ]; then cmp -s "$src" "$dest" || { echo 'Conflicting runtime source' >&2; exit 1; }
  else cp -- "$src" "$dest"; fi
done
for src in "$base"/verified-source-cache/*.jsonl.gz; do
  dest="$run/work/verified-source-cache/${src##*/}"
  if [ -e "$dest" ]; then cmp -s "$src" "$dest" || { echo 'Conflicting cached source' >&2; exit 1; }
  else cp -- "$src" "$dest"; fi
done
for src in "$base"/grepmesh-native100-rolling-*.json; do
  dest="$run/${src##*/}"
  if [ -e "$dest" ]; then cmp -s "$src" "$dest" || { echo 'Conflicting audit receipt' >&2; exit 1; }
  else cp -- "$src" "$dest"; fi
done
exec systemd-run --user --unit="grepmesh-native100-bounded-l04-resume-$(date -u +%Y%m%d%H%M%S)-$$" --property=Type=exec --property=MemoryMax=3G --property=MemorySwapMax=0 --property=CPUQuota=200% --property=RuntimeMaxSec=36h --property=Nice=15 --setenv=PYTHONDONTWRITEBYTECODE=1 --setenv=GREPMESH_CLOUD_GATEWAY=roomhacker@192.168.2.100 --setenv=GREPMESH_CLOUD_RCLONE=/home/roomhacker/.local/bin/rclone --setenv=GREPMESH_REUSE_VERIFIED_SHARDS=1 --setenv=GREPMESH_BOUNDED_RESUME_MODE=resume --setenv=GREPMESH_REQUIRE_CACHED_SOURCE_PART=000424 /usr/bin/python3 -u "$run/code/run_l04.py"
