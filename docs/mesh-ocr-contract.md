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
stale/failed/unavailable/busy peers. Rank available GPU performance then headroom
and utilization, recheck admission on extraction. Use existing direct LAN peers,
relay fallback and protected credential env refs. On failure try another peer;
report/defer OCR when none available. Existing topology refresh supplies new
routes; retain all search routes. Send file bytes, never a local path to read
on another host. Enforce max32MiB decoded payload, input-type allowlist, finite
HTTP deadlines, bounded output and serial client indexing. Worker receives
images/PDF, renders PDF locally with existing bounded settings, returns text
and actual serving host/GPU identity. Content and credentials are not logged.

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
once SQLite accepts the text. Retry is one file/minute independent of hot FS
activity; a thin PDF after transport failure is never treated as completed OCR.
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
