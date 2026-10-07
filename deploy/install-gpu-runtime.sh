#!/usr/bin/env bash
# Install staged GPU dependencies privately; caller supplies verified artifacts.
# No download, global CUDA install, tenant change or service restart here.
set -euo pipefail
stage=${1:?verified stage directory required}
ort="$stage/onnxruntime-linux-x64-gpu-1.25.1"
archive="$stage/onnxruntime-linux-x64-gpu-1.25.1.tgz"
expected=ddfc4ca4ccc9cd5345d3820edab710ee84e749569d052eed92c42693d3b448a8
printf '%s  %s\n' "$expected" "$archive" | sha256sum --check --status
[[ -f "$stage/cudnn/lib/libcudnn.so.9" ]]
[[ -f "$stage/nvidia/nvidia/curand/lib/libcurand.so.10" ]]
[[ -f "$stage/nvidia/nvidia/cufft/lib/libcufft.so.11" ]]
[[ -f "$ort/lib/libonnxruntime_providers_cuda.so" ]]
root=/opt/grepmesh-custom/gpu
sudo -n install -d -m 0755 "$root/lib" "$root/licenses"
sudo -n cp -a "$ort/lib/"*.so* "$stage/cudnn/lib/"*.so* \
    "$stage/nvidia/nvidia/curand/lib/"*.so* "$stage/nvidia/nvidia/cufft/lib/"*.so* "$root/lib/"
# Freeze the current compatible CUDA12 libraries instead of depending on a
# future Ollama update or altering its load paths/model cache.
sudo -n cp -a /usr/local/lib/ollama/cuda_v12/libcublas*.so* \
    /usr/local/lib/ollama/cuda_v12/libcudart*.so* "$root/lib/"
sudo -n cp "$ort/LICENSE" "$ort/ThirdPartyNotices.txt" "$root/licenses/"
sudo -n sh -c 'cd "$1" && sha256sum lib/*.so* > runtime-sha256.txt' sh "$root"
user=$(systemctl show grepmesh-mcp.service -p User --value); user=${user:-root}
group=$(systemctl show grepmesh-mcp.service -p Group --value); group=${group:-$user}
sudo -n install -d -o "$user" -g "$group" -m 0750 /var/lib/grepmesh-mcp/ocr-models /var/lib/grepmesh-mcp/.tmp/compute
sudo -n cp "$stage/pp-ocrv6_tiny_det.onnx" "$stage/eslav_pp-ocrv5_mobile_rec.onnx" "$stage/ppocrv5_eslav_dict.txt" /var/lib/grepmesh-mcp/ocr-models/
sudo -n chown -R "$user:$group" /var/lib/grepmesh-mcp/ocr-models
sudo -n install -d -m 0755 /etc/systemd/system/grepmesh-mcp.service.d
printf '%s\n' '[Service]' 'Environment="LD_LIBRARY_PATH=/opt/grepmesh-custom/gpu/lib:/opt/grepmesh-custom/lib"' \
    'Environment="OAR_HOME=/var/lib/grepmesh-mcp/ocr-models"' | \
    sudo -n tee /etc/systemd/system/grepmesh-mcp.service.d/gpu-runtime.conf >/dev/null
sudo -n systemctl daemon-reload
# Resolution proof only; real CUDA OCR is a separate required canary.
LD_LIBRARY_PATH="$root/lib" ldd "$root/lib/libonnxruntime_providers_cuda.so" | \
    awk '/not found/ {failed=1} END {exit failed}'
printf 'Private CUDA runtime installed; grepmesh service has not restarted.\n'
