---
status: resolved
trigger: "Owner asks fix slow indexed search, preserve OCR/PDF and foreign sessions"
created: 2026-10-13
updated: 2026-10-13
---

## Current Focus
hypothesis: confirmed global body materialization and synchronous SQLite delayed scoped interactive requests
next_action: none for this repair; broad default-root partial coverage remains a separate explicit limit

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

## Resolution
root_cause: reader fetched all matching bodies before caller scope/glob/limit filtering and blocked async HTTP workers; retained empty index also suppressed live files
fix: eff324f bounded reader/off-async execution; 056d368 bounded missing-index recovery; M1 operational startup repair119b93c/9142798
verification: real Linux indexed/live/PDF/OCR and M1 indexed/connected-MCP consumers passed; all5 metadata ok; broad default scope remains partial
files_changed: Cargo.toml, Cargo.lock, src/index.rs, src/backend.rs, src/jobs.rs, src/config.rs, src/server.rs, tests/mesh.rs
