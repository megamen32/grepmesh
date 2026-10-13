# Mesh GPU OCR contract (active implementation, 2026-10-07)

User outcome: server-100 indexes files and sends OCR work to the best available
GPU in the existing GrepMesh mesh. GPU capabilities come from live peer API
responses; clients do not depend on a permanently chosen host. Current GPUs
are RTX3080Ti 12GiB on server-44 and server-88; server-44 has more VRAM reserve.

Every node may advertise local compute. Workers are disabled by default.
Enabled worker serves `compute_status` and `ocr_extract` through existing
GrepMesh MCP/peer transports, and matching private HTTP API routes. Status
includes host_id, CUDA backend, live GPU UUID/name/compute capability, free
VRAM, GPU utilization, local busy count, admission status and observation time.
One inference per worker, no unbounded queue; reject busy/insufficient-memory
work so the caller can try the next candidate. Fail closed when CUDA is absent.
Backendmesh never performs local inference. GPU-required workers validate CUDA registration with error_on_failure; PP-OCR CPU shape/control operators are permitted, while neural tensor inference uses CUDA.

Client: poll at most a small known peer set, cache capabilities briefly, exclude
stale/failed/unavailable/busy peers. The shared Rust mesh-core placement module
ranks `performance * (1 - utilization/100)`, with deterministic ties. Fresh load
uses host CPU, limiting cgroup ancestors' CPU/RAM/tasks, CUDA GPU utilization
and, for Apple CoreML, native Mach/IOKit GPU counters. Known-busy admission is
temporary rather than a failed-route cooldown. Admission is rechecked on the
worker before inference. Reserve host memory for other users/services: 20GiB
on >=64GiB hosts, 4GiB on >=32GiB, 2GiB on >=8GiB, otherwise1GiB.
Hardware estimates are marked approximate; completed warm image work supplies
median megapixels/s across16 samples/30min. Cold initialization, cache hits,
PDFs and giant >64Mpixel rasters do not distort that estimator. The client
never mixes throughput units with hardware estimates; workload complexity
still makes these estimates approximate, not a universal device benchmark.
Use existing direct LAN peers,
relay fallback and protected credential env refs. On failure try another peer;
report/defer OCR when none available. Existing topology refresh supplies new
routes; retain all search routes. Send file bytes, never a local path to read
on another host. Enforce max32MiB decoded payload, input-type allowlist, finite
HTTP deadlines, bounded output and serial client indexing. Worker receives
images/PDF, renders PDF locally with existing bounded settings, returns text
and actual serving host/GPU identity. Content and credentials are not logged.

Apple-silicon builds include optional required CoreML registration with
CPUAndGPU/MLProgram. The same ONNX models/dictionary are retained; CPU shape
and unsupported operators remain possible. Real native M1 compute-plan/OCR
proof observed matmul assigned to MLGPUComputeDevice Apple M1 Max, with
236/242 detector and329/346 recognizer nodes supported byCoreML; cold5.386s
and warm0.617s on the same new2000x1000 control-image workload. This was a
finite separate candidate, not yet a production/all-host placement acceptance.
macOS x86 builds retain CPU OCR; they do not pretend to offer CUDA/CoreML.

Very tall noninterlaced PNGs stream rows into <=8192x512 RGB strips and
overlapping <=1024x512 tiles, instead of allocating the complete raster.
RGBA/GrayAlpha are composited onwhite; only exact constant-color tiles skip
inference. CRC/end-of-frame and deadlines are checked; any incomplete pass
returns an error. Identical text is deduplicated only at overlapping global
geometry; repeated text elsewhere stays. Existing decoder handles ordinary
images. The measured largest retained input has891391rows/21.1GB decoded
pixels; codec-only pass took14.03s/1664KiB, withoutsaving rawpixels. Neural
post-install completion and the pending media index still require real proof.

Runtime: NVIDIA workers use the same pinned detector/recognizer/dictionary,
ORT CUDA with limited arenas, one local Rayon worker and bounded CPU threads.
Worker service runtime stays inside the existing GrepMesh cgroup; retain at least
2.5GiB tenant VRAM plus two512MiB arenas at admission (3.5GiB free required). No changes to
Ollama/Whisper/other GPU tenants. Provision GPU runtime dependencies privately.

Acceptance: one real controlled OCR fixture from server-100 -> selected GPU ->
searchable text in server-100's real index; trace GPU PID/VRAM and selected
UUID, verify no local ORT session/CPU worker storm on100. Prove busy admission
and next-peer retry without interrupting tenants. All five search mesh routes
must remain healthy. Complete builds/deploy/restarts before final canary.


Code review fixes: GPU outage cannot make the index recv_timeout(0) spin;
expired alternate-peer capability is refreshed before retry; one large-body
intake is admitted before JSON; pending OCR completion is acknowledged only
once SQLite accepts the text. Retry is serial: after a committed result, at most one file/second;
a failed attempt restores the60s backoff, independent of hot FS activity; a thin PDF after transport failure is never treated as completed OCR.
Mesh inputs above32MiB follow the index size policy instead of retrying forever.
Local PDF rendering is bounded90s. NoGPU => defer without opening largepayload.
Failedroutes cool down60s and do not cause per-file capability/request storms.

GPU runtime archive: official onnxruntime-linux-x64-gpu-1.25.1.tgz SHA256
`ddfc4ca4ccc9cd5345d3820edab710ee84e749569d052eed92c42693d3b448a8`.
Native dependencies are private deployment assets; no proprietary GPU library
or generated policy/credential enters the Git repository. Existing Ollama and
Whisper processes, model cache and resource contracts remain their owners'.

Native candidate exposed two defects before rollout: status polling could
reject an already admitted job; only the single admitted job now waits<=3s
for its capability probe. PP-OCR includes CPU shape/control nodes, so
disabling allCPU EP nodes rejects validCUDA graphs. MandatoryCUDA provider
registration is now checked fail-closed before constructing the model sessions.
The actual CUDA worker PID/allocation plus OCR output remain required proof.

Final source guard uses a single explicitly configured ORT environment with
CUDA dispatch.error_on_failure; both actual model commits inherit it instead
of OAR's permissive local dispatch. Existing or mismatched environment is
rejected. Image/region batch sizes are1. Native44 controlled image returned
exactfixturetext in1.89s with366MiB actualGPU allocation; a CPU-ORT-only
child rejected the same valid model/input without inference. Busy admission
rejected a second request. All18 focused compute/client/OCR/SQLite regression
checks passed in one bounded85.5s run, peak819MiB, zero swap. Production
offload and finalindexedtext canary are still required.

Mac mini remains a lightweight relay with index_enabled=false. Its checked-in
config replaces the obsolete directroamingM1 peer with authenticated dynamic
GPTAdmin topology, retaining LinuxLAN routes. The restricted fleet token is
deployed only through its protected ~/.config/grepmesh/gptadmin.env, excluded
from search. No extra MCPprocess or localCPUOCR is enabled. M1 currently
travels with theuser and is intentionally not a live acceptance dependency.

Durable pending OCR is bounded65,536paths inSQLite, restored afterrestart.
FTS replacement/deletion and pending ACK commit atomically; failed enqueue
keeps the memory entry and blocks completion until durable reconciliation.
Overflow records a forced full-walk flag. Once tracked work drains, a new walk
rediscovers overflowed inputs even with a recent weekly marker; only a
successfully committed full reconciliation clears that flag. Migration from
the initial memory-only GPU release seeds this flag without resetting the
genuine full-rebuild timestamp. Confirmed undecodable image payloads return
a typed terminal error; emptyindex ACK still requiresSQLitecommit and does
not cool down the healthy GPU. IO, CUDA and transport failures remainretryable.
