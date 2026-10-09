# GrepMesh agent instructions

## Resource budget

GrepMesh is a fleet-wide indexer on shared hosts. Do not run it, its test
suite, or a release build outside an explicit cgroup budget.

For the measured 125 GiB Linux hosts (server-100, server-88, and server-44),
the reviewed runtime envelope is:

- `MemoryHigh=4G`, `MemoryMax=6G`, `MemorySwapMax=1G`;
- `CPUQuota=200%` and `TasksMax=384`;
- `IOWeight=20` so background indexing yields to interactive and database IO;
- one GrepMesh rebuild per host at a time.

The values are an outer safety boundary, not a target. On 2026-10-03 the
resident process was about 0.6 GiB on server-88; its 25 GiB cgroup charge was
mostly inactive file cache from indexing. `MemoryHigh` must remain in place so
that cache is reclaimed before it displaces other tenants. Do not raise these
limits to accommodate file cache.

The effective service user must own and be able to write
`/var/lib/grepmesh-mcp`. A stale numeric owner previously prevented topology
cache writes on server-88 and server-44, silently collapsing them back to
static peers. Verify ownership after any unit-user or package migration.

For builds and tests use at most two Cargo jobs, `CPUQuota=200%`,
`MemoryHigh=4G`, `MemoryMax=6G`, `MemorySwapMax=1G`, and `TasksMax=512` in a
transient systemd unit. Run the narrowest relevant test first. Keep build and
temporary artifacts below 20 GiB and do not run a second heavy workload in
parallel.

The Mac LaunchAgent is not protected by systemd. Keep local indexing roots
narrow, retain the configured rebuild interval, and do not add broad roots
without measuring a complete scan first. Its reviewed operating budget is one
index/reconciliation worker, less than 2% CPU averaged over a 60-second idle
window, less than 1 GiB resident memory in steady state (2 GiB fault ceiling),
no sustained swap growth, at most 64 tasks, and at most 8 GiB for the SQLite
database plus WAL/SHM and 2 GiB of build/test temporary data. Full rebuilds stay
at least 24 hours apart on the current Mac roots. A rebuild may use one core,
but it must finish and return to the idle budget; continuous one-core use is a
defect, not an accepted background allowance.

Any budget increase requires fresh steady/peak measurements on that host, an
impact review for co-tenants, a documented change here, and a post-change
consumer canary.

OCR must keep its own inference and preprocessing pools bounded: one ORT
intra-op thread, one inter-op thread, sequential execution with spinning
disabled, and one reusable local Rayon worker. Decode and prediction both run
inside that local pool. Host-sized defaults previously created 111 threads
inside the two-CPU service and prolonged reconciliation under CPU throttling.
These application bounds do not replace or raise the existing cgroup limits.


## Mesh GPU OCR placement and budgets

Server-100 uses `ocr.backend=mesh` and must never create a local inference
session or CPU fallback. NVIDIA workers on 44/88 execute through the existing
GrepMesh API9419. Their normal GrepMesh cgroup limits remain unchanged. One
worker job per host; retained tenant VRAM reserve2560MiB plus two512MiB ORT
arenas required at admission. CUDA provider registration is mandatory; neural inference usesCUDA, while
model shape/control operators can runonCPU. CPUthreads1/1 andRayon1.
Bound32MiB input,1MiB output,90s renderer/inference deadlines; no unboundedqueue.
Queue/busy/notenoughVRAM -> selectanotherpeer or deferclientOCR for retry.

For the new server-44 GPU runtime placement, 2026-10-07 measured93GiB host
MemAvailable, memory/IO PSI0, GPU12GiB total with~7.3GiB free. A single native
candidate build may run there from an exported canonical source artifact (no
ServerAdmin checkout) with CPUQuota100%, Cargojobs1, MemoryHigh1536M,
MemoryMax2G, swap0, TasksMax128, IOWeight20,600sdeadline; build/tempgrowth<2GiB,
project build/artifacts<20GiB. This is the documented GPU worker's owninghost,
not a way to evade server-100 limits. No other tenant, Ollama, Whisper or fleet
session may be stopped or its limits changed. Always measure prior to a run;
other hosts do not inherit this new44budget without their own reserveproof.

Private GPU runtime installation on44 is a sequential deployment operation,
not an inference/build fanout: CPU100%, MemoryHigh256MiB/Max512MiB, swap0,
TasksMax32, IOWeight20, deadline180s; stagedGPU assets<4GiB, durableGPU
runtime<4GiB and wholeproject<20GiB. A file-copy process has only a small
userspace buffer; cgroup boundaries also bound newly charged filesystemcache.
Fresh44 reserve before installation must remain >=8GiB and memory/IO full
avg10<1. Do not modify the existing Ollama/Whisper libraries or their services.
The installer only adds private copies, model files and GrepMesh's dropin;
intentional GrepMesh restart belongs to the subsequent atomic rollout.

Server88 private runtime preparation was separately admitted after live
measurement on2026-10-07: MemAvailable~103GiB, hostmemory/IO fullavg10=0,
RTX3080Ti free2056MiB (inference admission remains denied). Its existing
Whisper/Ollama tenants stayrunning. Only verified files are staged/copied:
CPU100%, MemoryHigh256MiB/Max512MiB, swap0, TasksMax32, IOWeight20,
network transfer<=16MiB/s, deadline600s for preparation and180s for install.
TemporaryGPU assets<4GiB, privateinstalledGPU<4GiB, wholeproject<20GiB.
This budget follows the measured44 copyprocess peakRSS6MiB and includes
512MiB for bounded newlychargedfilesystemcache; it is not a GPU inference
reserve or authorization to alter another tenant. Recheck88 >=8GiB reserve
and hostmemory/IO fullavg10<1 before each step.

Hosted native release CI is isolated from fleet processes. Keep at most one
platform job active and Cargo jobs1. Linux/macOS have a20-minute job deadline;
Windows has a40-minute deadline after the measured cold jobs1 build exceeded
20minutes on run37692308698 while compilation still progressed. The isolated
GitHub-hosted windows-latest VM has4vCPU/16GB RAM/14GB SSD; jobs1 reserves
CPU capacity for runner/OS. No fleet capacity or runtime limit changes.
Keep the full native features and packaging/search acceptance gates; reuse already
verified native artifacts for fleet deployment rather than rebuilding on
clients. These restrictions do not authorize any fleet budget change.


## Cached energy-recovery build44 — 2026-10-09

The accepted CPU1/high1536MiB/max2GiB/swap0/tasks128/600s/IOWeight20,
Cargo jobs1 and <2GiB temporary growth/<20GiB project bounds remain unchanged.
A fresh native check found the server44 user manager delegates memory+pids,
with no cpu/io controller in its app.slice. Do not call CPUQuota/IOWeight
properties native enforcement there. The owning build command additionally
uses one allowed logical CPU via taskset (inherited by Cargo/rustc) and
ionice best-effort priority7; record affinity and actual memory/swap/pids plus
measured process/file IO. Cached-only/offline work retains >=10GiB host spare,
no new OOM/sustained pressure and bounded storage. Global delegation/source
is the R38 owner's repair, not a GrepMesh host-control mutation.
