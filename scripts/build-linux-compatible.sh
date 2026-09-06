#!/usr/bin/env bash
# Build the fleet Linux x86_64 package against Microsoft's portable ONNX runtime.
set -euo pipefail
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"
[[ "$(uname -s)-$(uname -m)" == Linux-x86_64 ]] || {
  echo 'This package targets Linux x86_64.' >&2; exit 1;
}
ort_version=1.25.1
ort_sha=eb566a49cfc49ef0642f809b69340b5bb656c7c4905ba873526d226f2c005816
ort_name="onnxruntime-linux-x64-$ort_version"
ort_cache="$project_root/.tmp/onnxruntime"
archive="$ort_cache/$ort_name.tgz"
mkdir -p "$ort_cache" "$project_root/.tmp/test-temp"
if [[ ! -f "$archive" ]]; then
  curl -fsSL --retry 2 "https://github.com/microsoft/onnxruntime/releases/download/v$ort_version/$ort_name.tgz" -o "$archive.part"
  mv "$archive.part" "$archive"
fi
printf '%s  %s\n' "$ort_sha" "$archive" | sha256sum --check --status
tar -xzf "$archive" -C "$ort_cache"
export ORT_LIB_LOCATION="$ort_cache/$ort_name/lib"
export ORT_PREFER_DYNAMIC_LINK=1 ORT_SKIP_DOWNLOAD=1
export TMPDIR="$project_root/.tmp/test-temp"
# Match the current fleet's feature selection; OCR remains enabled.
export CARGO_TARGET_DIR="$project_root/.tmp/release-target"
cargo rustc --locked --release --no-default-features --bin grepmesh-mcp -- -C 'link-arg=-Wl,-rpath,$ORIGIN/lib'
package="$project_root/.tmp/linux-compatible-package"
mkdir -p "$package/lib" "$package/licenses/onnxruntime"
cp "$CARGO_TARGET_DIR/release/grepmesh-mcp" "$package/grepmesh-mcp"
cp -a "$ORT_LIB_LOCATION"/libonnxruntime.so* "$package/lib/"
cp "$ort_cache/$ort_name/LICENSE" "$package/licenses/onnxruntime/"
cp "$ort_cache/$ort_name/ThirdPartyNotices.txt" "$package/licenses/onnxruntime/"
printf 'Version: %s\nSource: https://github.com/microsoft/onnxruntime/releases/tag/v%s\nArchive-SHA256: %s\n' "$ort_version" "$ort_version" "$ort_sha" > "$package/onnxruntime-manifest.txt"
sha256sum "$package/lib/libonnxruntime.so.$ort_version" | sed "s|$package/||" >> "$package/onnxruntime-manifest.txt"
"$package/grepmesh-mcp" --help >/dev/null
python3 - "$package/lib/libonnxruntime.so" <<'PY'
import ctypes
import sys
library = ctypes.CDLL(sys.argv[1])
class ApiBase(ctypes.Structure):
    _fields_ = [("get_api", ctypes.CFUNCTYPE(ctypes.c_void_p, ctypes.c_uint32)),
                ("version", ctypes.CFUNCTYPE(ctypes.c_char_p))]
library.OrtGetApiBase.restype = ctypes.POINTER(ApiBase)
base = library.OrtGetApiBase().contents
# ort-sys rc.13 in the locked no-default-features build selects API 17.
assert base.get_api(17), "ONNX Runtime does not support required API 17"
print("ONNX Runtime:", base.version().decode(), "API 17 available")
PY
if [[ "${1:-}" == --test ]]; then
  CARGO_TARGET_DIR="$project_root/.tmp/cargo-target" \
    LD_LIBRARY_PATH="$ORT_LIB_LOCATION${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    cargo test --locked --no-default-features --lib index::tests
fi
printf 'Package: %s\n' "$package"
