#!/usr/bin/env bash
# mvp-deploy.sh — atomic grepmesh-mcp binary+config deploy with receipt + rollback.
#
# One invocation deploys ONE node:
#   server-100 (local):  deploy/mvp-deploy.sh --binary B --config deploy/mvp-config-server-100.json
#   server-88  (remote): deploy/mvp-deploy.sh --binary B --config deploy/mvp-config-server-88.json --remote roomhacker-server-88
# Rollback a node from its receipt (add the same --remote for server-88):
#   deploy/mvp-deploy.sh --rollback /opt/grepmesh-custom/.mvp-receipts/<timestamp>
#
# Per-target steps (executed on the target itself; remote runs via `ssh host bash -s`):
#   stage binary+config -> receipt with old binary/config copies + sha256s + commands
#   -> smoke-test new binary -> atomic mv into /opt/grepmesh-custom/grepmesh-mcp
#   -> install config -> systemctl restart grepmesh-mcp -> wait <=30s active
#   -> curl http://127.0.0.1:9419/api/catalog expecting 200
#   -> any failure after backup: restore old binary+config, restart, re-verify, exit non-zero.
#
# Requires passwordless sudo on the target (verified for roomhacker on both nodes).
# The binary needs no lib/ files of its own (ldd: system libs only); the script
# only verifies /opt/grepmesh-custom/lib/libonnxruntime.so stays present for
# STT/OCR tooling and records it in the manifest.
set -Eeuo pipefail

INSTALL_DIR=/opt/grepmesh-custom
BINARY_PATH=$INSTALL_DIR/grepmesh-mcp
CONFIG_PATH=/etc/grepmesh-mcp/config.json
SERVICE=grepmesh-mcp
RECEIPT_ROOT=$INSTALL_DIR/.mvp-receipts
HEALTH_URL=http://127.0.0.1:9419/api/catalog
HEALTH_WAIT=30

MODE=deploy
REMOTE=
BINARY=
CONFIG=
RECEIPT=

log() { printf '[mvp-deploy] %s\n' "$*" >&2; }
die() { printf '[mvp-deploy] FAIL: %s\n' "$*" >&2; exit 1; }

usage() { grep '^# ' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary) BINARY=$2; shift 2 ;;
    --config) CONFIG=$2; shift 2 ;;
    --remote) REMOTE=$2; shift 2 ;;
    --rollback) MODE=rollback; RECEIPT=$2; shift 2 ;;
    --local-mode) shift ;;
    --mode) MODE=$2; shift 2 ;;
    -h|--help) usage ;;
    *) die "unknown argument: $1 (see --help)" ;;
  esac
done

SELF=$(readlink -f "$0")

# ---- front end: run the same logic on the target ----------------------------
# Local (server-100): execute below directly.
# Remote (server-88): stage the payload over scp, then pipe this script to
# `ssh host bash -s` so the exact same server-side steps run on that node and
# its health curl also runs on that node.
if [[ -n "$REMOTE" ]]; then
  if [[ $EUID -eq 0 ]]; then
    die "run as roomhacker; the script escalates via sudo itself"
  fi
  if [[ $MODE == deploy ]]; then
    [[ -f "$BINARY" ]] || die "binary not found: $BINARY"
    [[ -f "$CONFIG" ]] || die "config not found: $CONFIG"
    STAGE=$(ssh "$REMOTE" 'mktemp -d /tmp/mvp-deploy-stage.XXXXXX')
    trap 'ssh -o BatchMode=yes "$REMOTE" "rm -rf \"$STAGE\"" >/dev/null 2>&1 || true' EXIT
    scp -q "$BINARY" "$REMOTE:$STAGE/grepmesh-mcp.new"
    scp -q "$CONFIG" "$REMOTE:$STAGE/config.json.new"
    ssh -o BatchMode=yes "$REMOTE" bash -s -- --local-mode --mode deploy \
      --binary "$STAGE/grepmesh-mcp.new" --config "$STAGE/config.json.new" < "$SELF"
  else
    [[ -n "$RECEIPT" ]] || die "--rollback requires a receipt directory path"
    ssh -o BatchMode=yes "$REMOTE" bash -s -- --local-mode --mode rollback \
      --receipt "$RECEIPT" < "$SELF"
  fi
  exit 0
fi

# ---- target-local mode (runs ON the node) -----------------------------------
if [[ ${1:-} == --local-mode ]]; then
  shift
fi
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE=$2; shift 2 ;;
    --binary) BINARY=$2; shift 2 ;;
    --config) CONFIG=$2; shift 2 ;;
    --receipt) RECEIPT=$2; shift 2 ;;
    *) die "unknown local argument: $1" ;;
  esac
done

command -v sudo >/dev/null || die "sudo not found on target"
command -v curl >/dev/null || die "curl not found on target"
command -v systemctl >/dev/null || die "systemctl not found on target"
sudo -n true 2>/dev/null || die "passwordless sudo unavailable on target"

health_ok() {
  systemctl is-active --quiet "$SERVICE" || return 1
  curl -fsS -o /dev/null -w '%{http_code}' --max-time 5 "$HEALTH_URL" 2>/dev/null | grep -qx 200
}

wait_healthy() {
  local waited=0
  while (( waited < HEALTH_WAIT )); do
    if health_ok; then return 0; fi
    sleep 1
    waited=$((waited + 1))
  done
  health_ok
}

restart_service() { sudo systemctl restart "$SERVICE"; }

sha_any() { sudo sha256sum "$1" | awk '{print $1}'; }

restore_and_verify() {
  local receipt=$1
  log "ROLLBACK: restoring previous binary and config from $receipt/old"
  sudo cp -a "$receipt/old/grepmesh-mcp" "$INSTALL_DIR/.grepmesh-mcp.restore"
  sudo cp -a "$receipt/old/config.json" "$CONFIG_PATH.restore"
  sudo mv -Tf "$INSTALL_DIR/.grepmesh-mcp.restore" "$BINARY_PATH"
  sudo mv -Tf "$CONFIG_PATH.restore" "$CONFIG_PATH"
  restart_service
  if wait_healthy; then
    log "ROLLBACK OK: service healthy again on previous release"
    return 0
  fi
  log "ROLLBACK VERIFICATION FAILED: $SERVICE is not healthy even after restore"
  return 1
}

TS=$(date -u +%Y%m%dT%H%M%SZ)
HOSTNAME=$(hostname)

if [[ $MODE == rollback ]]; then
  [[ -n "$RECEIPT" ]] || die "--rollback requires a receipt directory"
  if [[ ! -f "$RECEIPT/old/grepmesh-mcp" || ! -f "$RECEIPT/old/config.json" ]]; then
    die "receipt incomplete (need $RECEIPT/old/{grepmesh-mcp,config.json})"
  fi
  if restore_and_verify "$RECEIPT"; then
    printf 'NODE PASS rollback host=%s receipt=%s\n' "$HOSTNAME" "$RECEIPT"
    exit 0
  fi
  printf 'NODE FAIL rollback host=%s receipt=%s\n' "$HOSTNAME" "$RECEIPT" >&2
  exit 3
fi

# ---- deploy mode ----
[[ -f "$BINARY" ]] || die "binary not found: $BINARY"
[[ -f "$CONFIG" ]] || die "config not found: $CONFIG"

NEW_BIN_SHA=$(sha_any "$BINARY")
NEW_CFG_SHA=$(sha_any "$CONFIG")
python3 - "$CONFIG" <<'PY' || die "new config is not valid JSON or lacks host_id/bind/peers"
import json, sys
cfg = json.load(open(sys.argv[1]))
for key in ("host_id", "bind", "peers"):
    if key not in cfg:
        raise SystemExit(f"missing key: {key}")
PY

RECEIPT_DIR=$RECEIPT_ROOT/$TS
run() {
  local cmd="$*"
  printf '%s\n' "$cmd" | sudo tee -a "$RECEIPT_DIR/commands.log" >/dev/null
  "$@"
}
ensure_receipt_dirs() {
  sudo mkdir -p "$RECEIPT_DIR/old"
  sudo touch "$RECEIPT_DIR/commands.log"
}
ensure_receipt_dirs

# New binary must execute on this host before anything is touched.
sudo install -m 0755 "$BINARY" "$INSTALL_DIR/.grepmesh-mcp.smoke"
printf '%s\n' "sudo install -m 0755 $BINARY $INSTALL_DIR/.grepmesh-mcp.smoke (smoke test)" \
  | sudo tee -a "$RECEIPT_DIR/commands.log" >/dev/null
if ! sudo "$INSTALL_DIR/.grepmesh-mcp.smoke" --help >/dev/null 2>&1; then
  sudo rm -f "$INSTALL_DIR/.grepmesh-mcp.smoke"
  die "new binary --help smoke test failed"
fi
sudo rm -f "$INSTALL_DIR/.grepmesh-mcp.smoke"
printf '%s\n' "sudo rm -f $INSTALL_DIR/.grepmesh-mcp.smoke" \
  | sudo tee -a "$RECEIPT_DIR/commands.log" >/dev/null

# Back up the current release into the receipt before anything is modified.
run sudo cp -a "$BINARY_PATH" "$RECEIPT_DIR/old/grepmesh-mcp"
run sudo cp -a "$CONFIG_PATH" "$RECEIPT_DIR/old/config.json"
OLD_BIN_SHA=$(sha_any "$RECEIPT_DIR/old/grepmesh-mcp")
OLD_CFG_SHA=$(sha_any "$RECEIPT_DIR/old/config.json")

rollback_on_error() {
  local status=${1:-$?}
  log "deploy failed (status=$status); rolling back automatically"
  set +e
  restore_and_verify "$RECEIPT_DIR"
  rc=$?
  set -e
  printf 'NODE FAIL host=%s receipt=%s (rollback_verified=%s)\n' \
    "$HOSTNAME" "$RECEIPT_DIR" "$([[ $rc -eq 0 ]] && echo yes || echo NO)" >&2
  exit 4
}
trap rollback_on_error ERR

# lib/ is not a binary dependency (ldd: system libs only) but STT/OCR tooling
# dlopens it; require it to stay present and record its sha.
LIB_ONNX=$INSTALL_DIR/lib/libonnxruntime.so.1.25.1
[[ -e "$LIB_ONNX" ]] || die "runtime lib missing on target: $LIB_ONNX"

run sudo install -m 0755 "$BINARY" "$INSTALL_DIR/.grepmesh-mcp.mvp-new"
run sudo install -m 0644 "$CONFIG" "$CONFIG_PATH.mvp-new"

# Atomic renames within the same filesystem; verify bytes after the move.
run sudo mv -Tf "$INSTALL_DIR/.grepmesh-mcp.mvp-new" "$BINARY_PATH"
INSTALLED_SHA=$(sha_any "$BINARY_PATH")
if [[ "$INSTALLED_SHA" != "$NEW_BIN_SHA" ]]; then
  die "installed binary sha mismatch: $INSTALLED_SHA"
fi
run sudo mv -Tf "$CONFIG_PATH.mvp-new" "$CONFIG_PATH"
INSTALLED_CFG_SHA=$(sha_any "$CONFIG_PATH")
if [[ "$INSTALLED_CFG_SHA" != "$NEW_CFG_SHA" ]]; then
  die "installed config sha mismatch: $INSTALLED_CFG_SHA"
fi

run sudo systemctl restart "$SERVICE"

if ! wait_healthy; then
  sudo journalctl -u "$SERVICE" -n 20 --no-pager 2>/dev/null \
    | sudo tee "$RECEIPT_DIR/failure-journal.log" >/dev/null || true
  log "service did not become healthy within ${HEALTH_WAIT}s; health-check failure triggers rollback"
  rollback_on_error "health_check_failed"
fi

trap - ERR

sudo tee "$RECEIPT_DIR/manifest.txt" >/dev/null <<MANIFEST
mvp-deploy receipt
utc_time=$TS
host=$HOSTNAME
service=$SERVICE
binary_path=$BINARY_PATH
config_path=$CONFIG_PATH
old_binary_sha256=$OLD_BIN_SHA
new_binary_sha256=$NEW_BIN_SHA
old_config_sha256=$OLD_CFG_SHA
new_config_sha256=$NEW_CFG_SHA
installed_binary_sha256=$INSTALLED_SHA
installed_config_sha256=$INSTALLED_CFG_SHA
health_url=$HEALTH_URL
health_check=PASS (service active + HTTP 200 within ${HEALTH_WAIT}s)
libonnxruntime_path=$LIB_ONNX
libonnxruntime_sha256=$(sha_any "$LIB_ONNX")
binary_lib_dependencies=system only (libstdc++/libm/libgcc_s/libc); no staged lib needed
rollback_hint=$SELF --rollback $RECEIPT_DIR
MANIFEST
sudo chmod -R a+rX "$RECEIPT_DIR"

printf 'NODE PASS host=%s receipt=%s\n' "$HOSTNAME" "$RECEIPT_DIR"
printf '%s\n' "$RECEIPT_DIR"
