# Indexed search latency repair — 2026-10-13

The interactive indexed reader previously collected matching document bodies
across the entire database before applying the requested root, globs and match
limit. It also ran synchronously on the HTTP executor. A real scoped request
exceeded its eight-second client deadline.

The reader now uses a read-only SQLite snapshot, selects scoped paths and row
IDs, filters globs before loading bodies, streams results, and stops at the
requested result limit. SQLite and body parsing run on a blocking worker with
a two-second deadline, bounded candidate/body budgets and explicit errors.
Existing extracted PDF and image text stays in the durable cache; this repair
does not re-extract documents or change OCR placement.

Live verification found a second correctness problem: an empty retained index
page suppressed files not yet represented in that index. Empty nontruncated
pages now get a live-text scan bounded by the request deadline and two seconds.
That scan does not reuse the empty index candidates. Positive cached extraction
hits and SQL failure handling remain unchanged. A scan that cannot finish is
reported as incomplete, rather than claiming there are no matches.

## Evidence categories

- Fast unit/focused integration: 30 index tests plus 9 backend tests passed.
  The new real SQLite/ripgrep missing-file regression failed before the repair
  and passed afterwards. Cached binary PDF extraction has a separate regression.
  Final compilation took 86 seconds; tests took 1.70 seconds. Ordinary maximum:
  180 seconds. A stale build-cache attempt ran zero selected tests; it remains
  a setup failure and is excluded from acceptance.
- Native optimized Linux release build: 90 seconds, separate from the ordinary
  test gate, with an explicit 600-second maximum and the existing CPU1/Cargo1,
  1536MiB high/2GiB hard/swap0/task128 budget on server-44. Existing private CUDA
  libraries and worker configuration were retained.
- Actual MCP consumers: the same scoped source request completed in 0.259s;
  cached PDF text in 1.146s; cached image OCR text in 0.725s. The real three-host
  hostname and systemd queries returned all expected hosts in 1.363s and 0.749s,
  respectively, without partial or truncated results. A scoped absent-text
  query returned a complete empty result in 0.232s. The connected MCP consumer
  also completed with three correct host results.
- Broad default-root diagnostic: the expected source file was found in 4.332s,
  but one of five roots did not finish. This remains explicitly partial and is
  not full-inventory acceptance. The earlier incorrect empty results and all
  client/setup failures are retained separately.

Code source: `056d36842c191b599af83b031d2f6f463a46629e` (reader repair in
`eff324f5fcaa2f02551a4744f5ecba361b718516`, bounded miss recovery in `056d368`).
Native evidence is stored under `.tmp/search-latency-fix-20261013/` in the
canonical checkout. Linux installation receipts retain rollback copies and
byte-identical live configurations. Runtime limits, OCR/PDF caches, shared
controllers and foreign sessions were not changed. No model/OCR input was
submitted for these cached-search checks.

## M1 startup and final consumer

The first M1 delivery exposed an independent operational-store startup block:
the main thread waited in `SearchJobs::persistent`, first in a redundant chmod
and then in an optional telemetry-file open under Documents. Earlier failed
deliveries restored the old executable on disk but did not restore a healthy
service. Those failed readiness and unhealthy rollback receipts remain failures.
The exact operating-system cause of the blocked Documents access was not proven.

`119b93ca949c4d7e7d83bdd5bae1c04675c29921` skips redundant private-mode chmod
only on macOS when the actual owner and mode already match. Nine real native
job-store tests passed. `914279829981977e9d8299dd364eae938308a8e5` adds an
optional absolute `search_job_store_root`; its default preserves the original
state location and it does not add search/read roots. Six configuration tests
passed in 51.57 seconds. The native M1 release build completed in 53.69 seconds,
with observed peak descendant RSS 874,823,680 bytes under the existing monitored
2GiB/64-process boundary. This is a userspace observed bound, not a macOS kernel
memory quota.

Only M1 uses the new state parent, `~/.cache/grepmesh/search-runtime`. The two
old JSON history files were copied with exact hashes and private permissions;
the original files remain unchanged. Document roots, the index, OCR/PDF caches,
credentials, wrapper and shared native processes were preserved. Only the owned
Grep LaunchAgent was restarted. Actual health and CPU admission passed; the new
process remained the same generation after the consumer check.

The real M1 cached-index request found its expected original document in 0.182s,
complete and nonpartial/nontruncated. The connected native MCP separately
returned the expected M1 file through the same asynchronous job; its three-hit
requested cap was explicitly reported as truncated. Fresh connected status then
reported all five hosts healthy without partial coverage. This status is not a
claim that a full five-host content inventory was searched.

Linux keeps the verified `056d368` CUDA release; M1 runs `9142798`, whose later
changes concern optional operational state and startup only. Mini has indexing
disabled; its live-text runtime is unaffected and was left unchanged because
current reserve does not permit an unnecessary activation. There was no second
OCR submission, no model call, and no foreign session or service signal.

Final evidence: `native-m1-acceptance.json`, `m1-state-install-receipt.json`,
`m1-post-consumer-preservation.json`, `connected-native-m1-terminal.json`,
`final-fleet-native-status.json`, and the retained Linux consumer files under
`.tmp/search-latency-fix-20261013/`. Source is published on main; hosted CI for
`9142798` was queued at final native verification and is not claimed complete.
