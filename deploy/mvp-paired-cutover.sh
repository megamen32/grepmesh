#!/usr/bin/env bash
# Coordinated server-44 receiver + server-100 source cutover.
# Default is read-only preflight. --apply deploys 44 first, then 100; if the
# second node fails, both node-level receipts are used to restore the old pair.
set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
DEPLOY=$ROOT/deploy/mvp-deploy.sh
BIN=
CFG100=$ROOT/deploy/mvp-config-server-100.json
CFG44=$ROOT/deploy/mvp-config-server-44.json
REMOTE44=${GREPMESH_SERVER44_SSH:-roomhacker@192.168.2.5}
APPLY=0
MIN_FREE_KIB=$((15 * 1024 * 1024))

log() { printf '[paired-cutover] %s\n' "$*" >&2; }
die() { log "FAIL: $*"; exit 1; }
usage() {
  cat <<'EOF'
Usage: mvp-paired-cutover.sh --binary PATH [--config-100 PATH] [--config-44 PATH] [--remote-44 SSH_TARGET] [--apply]

The default run performs read-only preflight and prints the exact deployment
pair. --apply performs the coordinated restart using mvp-deploy.sh receipts.
EOF
  exit 2
}

while (($#)); do
  case "$1" in
    --binary) BIN=$2; shift 2 ;;
    --config-100) CFG100=$2; shift 2 ;;
    --config-44) CFG44=$2; shift 2 ;;
    --remote-44) REMOTE44=$2; shift 2 ;;
    --apply) APPLY=1; shift ;;
    -h|--help) usage ;;
    *) die "unknown argument: $1" ;;
  esac
done

[[ -n "$BIN" && -f "$BIN" ]] || die "pass an existing --binary"
[[ -f "$CFG100" && -f "$CFG44" && -x "$DEPLOY" ]] || die "deployment inputs are missing"
command -v python3 >/dev/null || die "python3 is required"
command -v ssh >/dev/null || die "ssh is required"

BIN_SHA=$(sha256sum "$BIN" | awk '{print $1}')
CFG100_SHA=$(sha256sum "$CFG100" | awk '{print $1}')
CFG44_SHA=$(sha256sum "$CFG44" | awk '{print $1}')
python3 - "$CFG100" "$CFG44" <<'PY'
import json, sys

c100, c44 = (json.load(open(path, encoding="utf-8")) for path in sys.argv[1:])
assert c100["host_id"] == "server-100", "config 100 host_id mismatch"
assert c44["host_id"] == "server-44", "config 44 host_id mismatch"
peer44 = [p for p in c100.get("peers", []) if p.get("host_id") == "server-44"]
peer100 = [p for p in c44.get("peers", []) if p.get("host_id") == "server-100"]
assert len(peer44) == len(peer100) == 1, "both reciprocal peer routes are required"
source = c100.get("remote_index_source") or {}
store = c44.get("remote_index_store") or {}
assert source.get("source_host") == "server-100", "source host must remain server-100"
assert source.get("index_host") == "server-44", "source must target receiver 44"
assert "server-100" in store.get("allowed_source_hosts", []), "receiver must retain authorized source"
source_roots = source.get("roots") or {}
store_roots = (store.get("allowed_roots") or {}).get("server-100") or {}
assert source_roots and set(source_roots) == set(store_roots), "source and receiver root names must match"
for name, roots in source_roots.items():
    allowed = store_roots[name]
    allowed = [allowed] if isinstance(allowed, str) else allowed
    assert roots and set(roots).issubset(set(allowed)), f"receiver authorization missing source root: {name}"
endpoint = source.get("endpoint", "").rstrip("/") + "/mcp"
assert endpoint in {peer44[0].get("local_url"), peer44[0].get("routable_url")}, "source endpoint must be a configured receiver peer"
PY

local_preflight() {
  systemctl is-active --quiet grepmesh-mcp || die "server-100 service is not active"
  curl -fsS --max-time 5 -o /dev/null http://127.0.0.1:9419/health || die "server-100 health probe failed"
  local available
  available=$(df -Pk /var/lib/grepmesh-mcp | awk 'NR==2 {print $4}')
  [[ "$available" =~ ^[0-9]+$ ]] && ((available >= MIN_FREE_KIB)) || die "server-100 has less than 15 GiB free for a rollback-safe deployment"
}

remote_preflight() {
  ssh -o BatchMode=yes -o ConnectTimeout=5 "$REMOTE44" bash -s -- "$MIN_FREE_KIB" <<'REMOTE'
set -Eeuo pipefail
minimum=$1
systemctl is-active --quiet grepmesh-mcp
curl -fsS --max-time 5 -o /dev/null http://127.0.0.1:9419/health
available=$(df -Pk /var/lib/grepmesh-mcp | awk 'NR==2 {print $4}')
[[ "$available" =~ ^[0-9]+$ ]] && ((available >= minimum))
REMOTE
}

local_preflight
remote_preflight || die "server-44 health/free-space preflight failed"
log "preflight PASS: both nodes healthy; reciprocal peer and source-root authorization preserved"
log "binary sha256=$BIN_SHA config100=$CFG100_SHA config44=$CFG44_SHA"
if ((APPLY == 0)); then
  log "dry-run only: no files changed and no services restarted"
  exit 0
fi

receipt_from() {
  awk '/^NODE PASS / { for (i=1;i<=NF;i++) if ($i ~ /^receipt=/) {sub(/^receipt=/, "", $i); print $i} }'
}

deploy44_output=$("$DEPLOY" --binary "$BIN" --config "$CFG44" --remote "$REMOTE44" 2>&1) || {
  printf '%s\n' "$deploy44_output" >&2
  die "receiver deploy failed; node-level script handled its rollback"
}
printf '%s\n' "$deploy44_output" >&2
receipt44=$(printf '%s\n' "$deploy44_output" | receipt_from | tail -n1)
[[ -n "$receipt44" ]] || die "receiver deployment passed but returned no rollback receipt: $deploy44_output"

if ! deploy100_output=$("$DEPLOY" --binary "$BIN" --config "$CFG100" 2>&1); then
  printf '%s\n' "$deploy100_output" >&2
  log "source deploy failed; restoring receiver 44 from its recorded receipt"
  "$DEPLOY" --rollback "$receipt44" --remote "$REMOTE44" || die "100 rolled back locally, but receiver rollback on 44 failed; receipt=$receipt44"
  die "100 deployment failed; 100 node rollback and 44 pair rollback were requested"
fi
printf '%s\n' "$deploy100_output" >&2
log "paired node deploy PASS; verify the authenticated external MCP consumer and CRUD before retiring either receipt"
