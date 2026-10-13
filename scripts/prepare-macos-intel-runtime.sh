#!/usr/bin/env bash
# Intel Macs use Microsoft's last Intel CPU bundle, with the locked ORT API 17.
set -euo pipefail
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ "$(uname -s):$(uname -m)" == Darwin:x86_64 ]] || exit 1
ort_version=1.20.1
ort_sha=0f73006813af2a1a5d1723ed7dfb694fc629d15037124081bb61b7bf7d99fc78
name="onnxruntime-osx-x86_64-$ort_version"
cache="$project_root/.tmp/onnxruntime"
mkdir -p "$cache"
archive="$cache/$name.tgz"
if [[ ! -f "$archive" ]]; then
  curl -fsSL --connect-timeout 10 --max-time 120 \
    "https://github.com/microsoft/onnxruntime/releases/download/v$ort_version/$name.tgz" -o "$archive.part"
  mv "$archive.part" "$archive"
fi
printf '%s  %s\n' "$ort_sha" "$archive" | shasum -a 256 --check --status
tar -xzf "$archive" -C "$cache"
package="$project_root/.tmp/macos-intel-runtime"
mkdir -p "$package/lib" "$package/licenses/onnxruntime"
cp -a "$cache/$name/lib/"*.dylib "$package/lib/"
cp "$cache/$name/LICENSE" "$cache/$name/ThirdPartyNotices.txt" "$package/licenses/onnxruntime/"
python3 - "$package/lib/libonnxruntime.dylib" <<'PY'
import ctypes, sys
library = ctypes.CDLL(sys.argv[1])
class ApiBase(ctypes.Structure):
    _fields_ = [("get_api", ctypes.CFUNCTYPE(ctypes.c_void_p, ctypes.c_uint32)),
                ("version", ctypes.CFUNCTYPE(ctypes.c_char_p))]
library.OrtGetApiBase.restype = ctypes.POINTER(ApiBase)
base = library.OrtGetApiBase().contents
assert base.get_api(17), "Intel runtime cannot serve the locked ORT API 17"
assert base.version().decode() == "1.20.1"
PY
printf 'Version: %s\nSource: https://github.com/microsoft/onnxruntime/releases/tag/v%s\nArchive-SHA256: %s\nRequired-API: 17\n' \
  "$ort_version" "$ort_version" "$ort_sha" > "$package/onnxruntime-manifest.txt"
shasum -a 256 "$package/lib/libonnxruntime.$ort_version.dylib" | sed "s|$package/||" >> "$package/onnxruntime-manifest.txt"
