---
status: resolved
trigger: "Owner asks fix slow indexed search, preserve OCR/PDF and foreign sessions"
created: 2026-10-13
updated: 2026-10-13
---

## Current Focus
hypothesis: confirmed global body materialization and synchronous SQLite delayed scoped interactive requests
next_action: performance/full default-root query accepted; preserve pending OCR backlog as a separate media-completeness boundary

## Symptoms
Literal exact-root/glob request on100 exceeded8s HTTP wait; same regex0.06s.
All5online;100building16GB durable index. First broad literal endedpartial.

## Evidence
- Live oldfailure preserved in GPTAdmin .tmp/grepmesh-latency-20261013.
- Production readonly newSQL candidate selection0.544s/one path/no global bodyload.
- Distinguishing scoped cachedPDF unit RED after source-input setup failure corrected.
- 30index+7backend units GREEN, bounded native44 build91s,37pureunits1.49s.
- Reviewed eff324f published: readonly single snapshot, SQL scope/pathids before body/glob gates,
  streaming stop on result cap, explicit budgets/deadlines, off-async reader; no index/schema/config changes.
- Release native relink is slow coverage600s, not ordinary unit coverage.
- Live missing-index files exposed another correctness defect; 056d368 adds bounded live recovery on an empty nontruncated page, preserving positive PDF/OCR cache hits.
- Final 30index+9backend units passed; exact source query0.2594s, cachedPDF1.146s, cachedPNG0.7252s, three-host queries1.3629/0.7493s.
- M1 startup separately blocked in SearchJobs::persistent at chmod, then optional history open under Documents. Both failed activations and unhealthy rollback receipts are retained, not accepted as ready.
- 119b93c skips only redundant already-owned private macOS chmod; nine real native units passed. 9142798 adds optional operational-state parent without changing search/read roots; six config units passed.
- M1 native build53.69s/874823680B monitored RSS; only Grep binary and one config key changed, old 2 history files retained with exact hashes. New service10518 healthy and no compute work in flight.
- Real M1 cached index query0.1823s/complete/nonpartial/nontruncated; connected native MCP same job completes with expected path (requested3 results capped/truncated explicitly).
- Fresh connected all5 status: all hosts ok, partial=false; Mini index-disabled live runtime left unchanged. Original/shared native process generations preserved across own M1 install.
- Broad default-root diagnostic finds expectedfile4.3316s but1of5 roots failed; remains partial. It is not a full inventory acceptance or attributed to absent roots.
- Follow-up root isolation proves /opt failed due2s empty-index live cap; independent native regex completes14.735s. 24aaf08 removes premature live cap without changing RAM/CPU/reader/output bounds.
- Full file enumeration5roots completes216.117s with2332264 unique file paths,102 preserved exclusions,269 permission/vanished diagnostics. This is accessible configured corpus enumeration, not proof of privileged files or full index caching.
- Temporary interactive60s produces complete broad literal20.239s, but full regex reference stillhits backend timeout. Exact seam: SearchJobs already supplies600s, LocalBackend incorrectly resets to interactive deadline.
-81f4778 transfers selected finite job deadline into local scan. Real-rg regression with interactive0/background5s failed beforefix (partialtrue), passesafter; interactive0 stillfails.6MCP+9backend+30index units pass. Native delivery/reference remains pending; old failed/partial jobs retained.

## Resolution
root_cause: reader fetched all matching bodies before caller scope/glob/limit filtering and blocked async HTTP workers; retained empty index also suppressed live files
fix: eff324f bounded reader/off-async execution; 056d368 bounded missing-index recovery; M1 operational startup repair119b93c/9142798
verification: real Linux indexed/live/PDF/OCR and M1 indexed/connected-MCP consumers passed; all5 metadata ok; broad default scope remains partial
files_changed: Cargo.toml, Cargo.lock, src/index.rs, src/backend.rs, src/jobs.rs, src/config.rs, src/server.rs, tests/mesh.rs


## Full-query closure
24aaf08 removes premature live cap;81f4778 preserves admitted job600s without interactive20s reset;29b8b93 stays on configured root filesystem;0a5a6dc aligns repository-ignore rules with configured corpus. Full regex had failed601s in foreign fuse.rclone before boundary repair; later complete query/reference agree.
9d9c5d3 repairs exact filename cache selection and OR/LIKE path lookup. Same old PDF failure indexed search interrupted2.148s retained. Final PDF.4973/OCR.4191 complete; connected native scoped1.157s, full5root literal15.63s agrees exactly with completed live regex35.996s, connected SAMEjob confirmed. All41 affected units passed; final native binaries/config/PIDs recorded, publicCI38385321184 success.
Accessible configured-file enumeration completed216.117s/2332264 paths/269 ACL-or-vanished diagnostics; no crossdevice/symlink walk, no index/manual cache mutation. Not universal privileged-file or full media extraction coverage. Server100 durable OCR pending1142/full-recovery marker remains open; retries continue without cache clearing, guard changes or old input replay.
Local verifier tuple-vs-JSON-list bug corrected by independent comparison of saved terminal results, no request replay. Initial failure retained.
Native job search/status consumers passed. Separate non-job native search_status adapter returns Unexpected response type for explicit3 hosts while same native HTTP metadata is valid; not used as fleet acceptance. Shared native client/SDK runtime is outside this Grep producer boundary; no global MCP refresh or foreign session wake performed. Evidence is retained for a scoped adapter-owner diagnosis.
