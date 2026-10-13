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
pages now get a live-text scan bounded by the admitted request deadline.
The initial extra two-second live cap was removed after full-root verification.
That scan does not reuse the empty index candidates. Positive cached extraction
hits and SQL failure handling remain unchanged. A scan that cannot finish is
reported as incomplete, rather than claiming there are no matches.

## Initial targeted acceptance (retained)

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

Initial targeted code source: `056d36842c191b599af83b031d2f6f463a46629e` (reader repair in
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

The earlier targeted closure used Linux `056d368` and M1 `9142798`, whose later
changes concern optional operational state and startup only. Mini has indexing
disabled; its live-text runtime is unaffected and was left unchanged because
current reserve does not permit an unnecessary activation. There was no second
OCR submission, no model call, and no foreign session or service signal.

Final evidence: `native-m1-acceptance.json`, `m1-state-install-receipt.json`,
`m1-post-consumer-preservation.json`, `connected-native-m1-terminal.json`,
`final-fleet-native-status.json`, and the retained Linux consumer files under
`.tmp/search-latency-fix-20261013/`. Source is published on main; hosted CI for
`9142798` was queued at final native verification and is not claimed complete.


## Full default-root follow-up — verified

**Coverage status:** the tested broad text query completed over all five
configured server-100 roots with `partial=false` and `truncated=false`. Its
path, line and text fingerprints exactly match a separate complete live regex
pass. The final same job was also read successfully through the connected
native MCP. This closes the earlier unchecked broad-query result.

The follow-up exposed three separate defects beyond the initial reader repair:

1. An extra two-second live-miss deadline abandoned large roots. `24aaf08`
   removes that extra cap. SQLite's two-second budget remains unchanged.
2. The existing asynchronous job's finite deadline was reset to the interactive
   deadline in the local backend. `81f4778` forwards the admitted deadline;
   the normal interactive setting remains 20 seconds and the existing background
   job ceiling remains 600 seconds. A temporary 60-second interactive diagnostic
   was restored to its original value on all three Linux nodes.
3. A full live pass entered an unconfigured `fuse.rclone` mount, whereas the
   index already stays on each configured root's filesystem. `29b8b93` applies
   the same device boundary to live text/path scans. `0a5a6dc` also aligns their
   repository-ignore handling with the index: configured GrepMesh exclusions
   remain authoritative. A real-rg ignored-file regression failed before this
   change and passed afterwards; explicit excluded files remain excluded.

A subsequent real cached-PDF request reached the SQLite deadline. Read-only
production diagnostics isolated the additional path lookup: the old OR/LIKE
plan could exceed two seconds even after FTS had found the PDF. `9d9c5d3`
selects safe exact filename scopes before reading cached bodies and uses
nonoverlapping exact-root/descendant SQL branches. Complex or wildcard filename
unions retain the general reader. The full root/glob/exclusion filters,
read-only snapshot, candidate/body/result limits and deadline are preserved.
No extraction cache was cleared, and no PDF/image was submitted for reprocessing
by these acceptance requests.

### Final real consumer measurements

| Operation | Seconds | Result |
| --- | ---: | --- |
| Scoped source query through connected native MCP | 1.157 | Expected file/text, complete |
| Broad indexed query, five configured roots | 15.630 | One expected match, complete |
| Independent full live regex reference | 35.996 | Exact same path/line/text fingerprints |
| Existing cached PDF text | 0.4973 | Correct, nonpartial/nontruncated |
| Existing cached image OCR text | 0.4191 | Correct, nonpartial/nontruncated |
| M1 existing cached document | 0.0677 | Correct, nonpartial/nontruncated |

The connected-client time includes its MCP transport overhead. The final broad
query reuses the already completed independent live reference after verifying
unchanged live-backend/compiler inputs, configuration and reference-file hash;
it does not resubmit that accepted reference. An initial local verifier compared
Python tuples with JSON lists and failed despite identical results. Its failure
log remains retained; a separate corrected verifier compares the saved terminal
responses and the connected same-job readback, without another search.

### Complete accessible-file enumeration

A separate real, bounded `rg --files --hidden --no-ignore --one-file-system`
pass completed in 216.117 seconds. It preserved all 102 configured/default
exclusions and enumerated 2,332,264 unique file paths:

| Configured root | Enumerated paths | Seconds | Permission/vanished diagnostics |
| --- | ---: | ---: | ---: |
| `/etc` | 5,155 | 0.399 | 148 |
| `/home/roomhacker` | 1,178,752 | 113.014 | 93 |
| `/opt` | 1,141,153 | 100.277 | 27 |
| `/opt/mobile-browser` | 5,856 | 0.707 | 1 |
| `/var/lib/grepmesh-mcp/userio-cache` | 1,348 | 0.163 | 0 |

There were 269 explicit permission/vanished-file diagnostics; those paths are
not silently counted as searched files. Symlinks and unconfigured filesystems
were not followed. This is a point-in-time accessible-path enumeration plus
full coverage of the tested text query, not a claim that every physical file,
privileged path, binary format or newly arrived file has a complete extraction.

**Full media-index completeness remains open.** The final native snapshot
reports 799,936 retained documents on server-100 and 1,142 durable OCR inputs
awaiting a worker, with `Degraded` status. The full-recovery marker remains set;
no complete-reconciliation marker was fabricated or manually advanced. The
existing bounded OCR retry lifecycle continues. Previously extracted PDF/OCR
search is verified above; pending new extraction is not passed off as ready.

### Checks, delivery and retained evidence

- Fast unit/focused integration: 31 index tests and 10 backend tests passed on
  the final compiled input closure. This includes real SQLite VM-work checks
  for a cached binary PDF, literal percent/underscore filenames, complex-union
  fallback, result limits, snapshots and the real-rg ignore/exclusion regression.
  The ordinary run stayed below 180 seconds; its bounded invocation maximum
  is 150 seconds. M1's 31 native index tests passed in 70.62 seconds including
  compilation, with sampled peak descendant RSS 971,079,680 bytes.
- Native release builds: Linux CUDA release 88 seconds; M1 CPU release 68.36
  seconds, sampled peak RSS 863,535,104 bytes. These use the existing finite
  build budgets and a separate 600-second maximum. M1's memory monitoring is
  not a kernel memory quota.
- Slow nightly coverage: the complete file census and independent root-wide
  pass are separate from ordinary unit coverage; expected 420 seconds,
  maximum 600 seconds per admitted background operation. Their actual results
  are recorded above. No foreign work was stopped to obtain them.
- Final application source `9d9c5d3b53bf1144d875d38e427b3ed1a031c867` is installed
  on 100/44/88 and M1. Linux binary SHA256 is
  `82d4b68ed7ec5dee67a6fd07a4475c7c880db6739a54673e5baba417210e7fa3`.
  All three Linux configurations and runtime limits are byte/value preserved;
  M1's existing operational-state configuration and shared native identities
  were preserved at activation. Rollback copies remain available. Mini's
  index-disabled runtime was not changed by this latency repair.
- Hosted source CI [run 38385321184](https://github.com/megamen32/grepmesh/actions/runs/38385321184)
  completed successfully for the final application source. CI does not replace
  the actual consumer evidence above.

Evidence under `.tmp/search-latency-fix-20261013/full-coverage/`:
`final-full-verifier-corrected.json`, `connected-native-final-full.json`,
`connected-native-final-scoped.json`, `final-complete-query-comparison.json`,
`reference-reuse-binding.json`, `full-file-enumeration.json`,
`native-cached-final.json`, `native-m1-final-acceptance.json`,
`final-installed-linux.json`, `m1-pdf-install-receipt.json`,
`native-current-nodes.json`, and `index-final-metadata.json`.
All earlier failed/partial requests and failed verifier/setup attempts remain
separate historical failures. No old model, OCR, notification or business input
was replayed; no shared native/controller service was restarted.

### Media recovery and measured worker placement — subsequent checkpoint

The earlier 1,142 count is a historical snapshot. The retained, individually
identified recovery cohort contains 1,036 inputs; the difference of 106 has no
retained identity mapping and is not silently declared covered. A read-only
SQLite audit at Unix time 1791922354 found:

| Original cohort disposition | Inputs | Verification |
| --- | ---: | --- |
| Nonempty extraction and matching FTS document | 1,002 | Cache/FTS text equal; file size and modification generation still match |
| Durable pending OCR | 10 | PNG inputs remain in the application queue |
| Existing file without extraction cache | 24 | Reason is not established; absence from the queue is not proof of complete extraction |

The verified nonempty group comprises 970 PNG, 31 PDF and one JPG. Header-only
inspection of the 24 unresolved files found one zero-byte file and 23 other
inputs. No new OCR was submitted to reinterpret their previous outcomes.
The full-recovery marker remains set. **Complete media coverage is still open.**

Shared-core task placement now uses an initially unknown (`None`) performance
estimate, followed by successful native jobs per second for the same task class
and execution provider. CPU and accelerator load are separate; host memory,
CPU and task admission still apply, and one inference reservation per host is
retained. Hardware estimates do not substitute for a task measurement.

The reviewed worker source passed 139 pure library checks on native server-44:
14.74 seconds for tests, 127.095 seconds including compilation, peak RSS/cgroup
charge 795,013,120 bytes, zero swap/OOM events and verified natural cleanup.
These checks establish source behavior, not live distributed queue completion.

The corresponding M1 `ocr-coreml` artifact compiled in 232.964 seconds with
sampled peak owned RSS 836,599,808 bytes. Expected duration was 180 seconds,
with a separate 1,800-second emergency limit; crossing the estimate did not
terminate the healthy build. Its memory observation is not a kernel quota.
The controlled Grep-only activation preserved the launch plist, model hashes,
existing configuration except three captcha-directory exclusions, and rollback.
Before switching, active jobs and retained results were both zero. Its wrapper
now enables the fixed `grepmesh::compute=warn` diagnostic target.

The existing configured caller reached this worker through the registered
HTTP singleton route in 0.635 seconds. The native response contained separate
CPU and CoreML devices, initially unknown performance, CPU utilization 82.84%
and accelerator utilization 19%, with current host admission available.
This is actual protocol/device metadata evidence; ordinary queue completion
on all three workers remains a separate acceptance gate. Historical OCR
controls and queued legacy jobs were not resubmitted.

Evidence under `.tmp/ocr-queue-completion-20261013/`:
`cohort-audit-device0d-m1-installed.json`,
`terminal24-header-classification.json`,
`task-rating-source4/{input-manifest,unit-result}.json`,
`m1-device0d-build-result.json`, `m1-device0d-activation-receipt.json`, and
`original-caller-m1-device0d-status.json`.
