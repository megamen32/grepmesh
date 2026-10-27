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
GUARD_EVIDENCE=
MIN_FREE_KIB=$((15 * 1024 * 1024))
RESOURCE_GUARD=$ROOT/deploy/paired_resource_guard.py

log() { printf '[paired-cutover] %s\n' "$*" >&2; }
die() { log "FAIL: $*"; exit 1; }
usage() {
  cat <<'EOF'
Usage: mvp-paired-cutover.sh --binary PATH [--config-100 PATH] [--config-44 PATH] [--remote-44 SSH_TARGET] [--apply --guard-evidence STATUS.json]

The default run performs read-only preflight and prints the exact deployment
pair. --apply performs the coordinated restart using mvp-deploy.sh receipts,
but requires fresh authenticated MCP status proving the configured source root
is not hot or metadata-only on server-100. If the installed runtime reports
only a coarse ancestor bucket, the gate also requires a bounded ctime walk to
prove that exact source root has been quiet for the configured cooldown. Apply
also requires at least 8 GiB MemAvailable and low memory PSI on both hosts.
I/O PSI and swap are recorded for diagnostics but do not block a bounded
restart. Memory and exact source-root quiet are checked again between
the receiver and source deploys. A held second phase leaves the 100 source
unchanged and retains the 44 rollback receipt; it will not restart either host
while the resource or activity guard is closed.
EOF
  exit 2
}

while (($#)); do
  case "$1" in
    --binary) BIN=$2; shift 2 ;;
    --config-100) CFG100=$2; shift 2 ;;
    --config-44) CFG44=$2; shift 2 ;;
    --remote-44) REMOTE44=$2; shift 2 ;;
    --guard-evidence) GUARD_EVIDENCE=$2; shift 2 ;;
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

resource_guard_local() {
  python3 "$RESOURCE_GUARD" --label server-100
}

resource_guard_remote() {
  ssh -o BatchMode=yes -o ConnectTimeout=5 "$REMOTE44" \
    python3 - --label server-44 < "$RESOURCE_GUARD"
}

resource_guard_pair() {
  resource_guard_local && resource_guard_remote
}

validate_source_guard() {
  local phase=${1:-mcp}
  python3 - "$GUARD_EVIDENCE" "$CFG100" "$phase" <<'PY'
import json, os, sys, time

phase = sys.argv[3]
assert phase in {"mcp", "live"}, "unknown source guard phase"
if phase == "mcp":
    evidence = json.load(open(sys.argv[1], encoding="utf-8"))
    captured = evidence.get("captured_at_ms")
    now = int(time.time() * 1000)
    assert isinstance(captured, int) and 0 <= now - captured <= 60_000, "guard evidence must be <=60s old"
    status = evidence.get("status") or {}
    assert status.get("partial") is False, "MCP status must be complete"
    healthy = {row.get("host_id") for row in status.get("host_status", []) if row.get("ok") is True}
    assert {"server-100", "server-44"}.issubset(healthy), "both production hosts must be healthy in MCP status"
    nodes = {node.get("host_id"): node for node in status.get("nodes", [])}
else:
    nodes = {}
config = json.load(open(sys.argv[2], encoding="utf-8"))
source = config.get("remote_index_source") or {}
cooldown_ms = int(((config.get("limits") or {}).get("index_activity") or {}).get("hot_cooldown_ms", 3_600_000))
activity = nodes.get("server-100", {}).get("index_directory_activity") or []

def quiet_by_metadata(root, cooldown):
    """Fail closed unless bounded ctime walk proves this exact root was quiet."""
    cutoff = time.time_ns() - cooldown * 1_000_000
    try:
        root_stat = os.stat(root, follow_symlinks=False)
        if os.path.islink(root) or not os.path.isdir(root) or root_stat.st_ctime_ns >= cutoff:
            return False
    except OSError:
        return False
    pending = [root]
    visited = 0
    while pending:
        directory = pending.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    visited += 1
                    if visited > 20_000:
                        return False
                    info = entry.stat(follow_symlinks=False)
                    if info.st_ctime_ns >= cutoff:
                        return False
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(entry.path)
        except OSError:
            return False
    return True

for roots in (source.get("roots") or {}).values():
    for root in roots:
        if phase == "live":
            assert quiet_by_metadata(root, cooldown_ms), \
                f"source-root activity guard is active or exact-root quiet cannot be proven: {root}"
            continue
        if not activity:
            assert quiet_by_metadata(root, cooldown_ms), \
                f"MCP has no per-root activity rows and exact-root quiet cannot be proven: {root}"
            continue
        matches = [row for row in activity
                   if root == row.get("path")
                   or root.startswith(row.get("path", "").rstrip("/") + "/")
                   or row.get("path", "").startswith(root.rstrip("/") + "/")]
        assert matches, f"MCP status has no activity bucket covering source root {root}"
        depth = max(len(row["path"].strip("/").split("/")) for row in matches)
        exact_scope = [row for row in matches
                       if len(row["path"].strip("/").split("/")) == depth]
        if not all(row.get("hot") is False and row.get("metadata_only") is False
                   for row in exact_scope):
            scoped_record = any(row.get("path") == root
                                or row.get("path", "").startswith(root.rstrip("/") + "/")
                                for row in exact_scope)
            assert not scoped_record and quiet_by_metadata(root, cooldown_ms), \
                f"source-root activity guard is active or exact-root quiet cannot be proven: {root}"
if phase == "mcp":
    assert int(time.time() * 1000) - captured <= 60_000, "guard evidence expired during source-root check"
PY
}

local_preflight
remote_preflight || die "server-44 health/free-space preflight failed"
log "preflight PASS: both nodes healthy; reciprocal peer and source-root authorization preserved"
log "binary sha256=$BIN_SHA config100=$CFG100_SHA config44=$CFG44_SHA"
if ((APPLY == 0)); then
  log "dry-run only: no files changed and no services restarted"
  exit 0
fi

[[ -n "$GUARD_EVIDENCE" && -f "$GUARD_EVIDENCE" ]] \
  || die "--apply requires fresh authenticated MCP --guard-evidence"
validate_source_guard mcp || die "source-root activity guard is hot, stale, partial, or unproven"
resource_guard_pair || die "apply is held until both hosts pass the memory reserve guard"

receipt_from() {
  awk '/^NODE PASS / { for (i=1;i<=NF;i++) if ($i ~ /^receipt=/) {sub(/^receipt=/, "", $i); print $i} }'
}

# Upgrade the source first. The new source sends the dedicated bearer token;
# the old receiver remains backward-compatible and accepts it during the short
# interval before 44 is upgraded. Reversing this order would make the old 100
# source-sync calls fail authentication until its restart completed.
deploy100_output=$("$DEPLOY" --binary "$BIN" --config "$CFG100" 2>&1) || {
  printf '%s\n' "$deploy100_output" >&2
  die "source deploy failed; node-level script handled its rollback"
}
printf '%s\n' "$deploy100_output" >&2
receipt100=$(printf '%s\n' "$deploy100_output" | receipt_from | tail -n1)
[[ -n "$receipt100" ]] || die "source deployment passed but returned no rollback receipt: $deploy100_output"

if ! resource_guard_pair; then
  log "source 100 is bearer-capable and remains compatible with the old receiver; keep receipt for rollback: $receipt100"
  die "memory guard closed after source deployment; receiver 44 remains unchanged"
fi
if ! validate_source_guard live; then
  log "source 100 is bearer-capable and remains compatible with the old receiver; keep receipt for rollback: $receipt100"
  die "source-root quiet proof expired or changed after source deployment; receiver 44 remains unchanged"
fi

deploy44_output=$("$DEPLOY" --binary "$BIN" --config "$CFG44" --remote "$REMOTE44" 2>&1) || {
  printf '%s\n' "$deploy44_output" >&2
  log "source 100 remains compatible with the old receiver; retain source receipt: $receipt100"
  die "receiver deploy failed; node-level script handled its rollback; source receipt=$receipt100"
}
printf '%s\n' "$deploy44_output" >&2
receipt44=$(printf '%s\n' "$deploy44_output" | receipt_from | tail -n1)
[[ -n "$receipt44" ]] || die "receiver deployment passed but returned no rollback receipt: $deploy44_output"
log "paired node deploy PASS; source receipt=$receipt100 receiver receipt=$receipt44; verify authenticated MCP consumer and CRUD before retiring either receipt"
