# Ranked-search v2 comparison

Captured 2026-10-28 against the final immutable artifact SHA-256
`a5e22cb67f48ef3f95acbe7eebd0633875f997fe1aa08c423de23091956d1b87` on
`http://127.0.0.1:19430/mcp`. The fixed 24-task corpus and gold paths were
unchanged. Candidate and baseline both reported Ready, 299 indexed files, and
the same `/home/roomhacker/ServersAdministartion/docs` root; both passed the
literal `s21-lease` gate. The candidate reported generation 0 while baseline
reported generation 310. Generation is process-local: a restarted daemon may
restore 299 persisted documents while retaining generation 0 until it performs
an in-process rebuild. Thus 0 does not imply an empty DB, and these generation
values are not comparable evidence.
The candidate process used the immutable artifact; baseline artifact SHA is
not available in the retained records.

## Ranked quality

One quality observation per task plus 30 warm repeats each for `a01`, `a02`,
and `d04` (114 rows total). There were zero tool errors and no partial
responses. Ranked search capped results at top ten; `truncated=true` on 113 of
114 rows reflects this result cap, not partial coverage.

| Metric | Baseline exact | v1 ranked candidate | v2 ranked candidate |
|---|---:|---:|---:|
| MRR | 0.2014 | 0.3477 | 0.3722 |
| nDCG@10 | 0.2029 | 0.4122 | 0.4306 |
| Recall@5 | 0.2500 | 0.5000 | 0.4750 |
| Any-hit negative rate | 0/4 | 4/4 | 4/4 |
| Strong relevant MRR | unavailable | unavailable | 0.1736 |
| Weak relevant MRR | unavailable | unavailable | 0.1986 |
| Negative strong-hit rate | unavailable | unavailable | 2/4 |
| Negative weak-only rate | unavailable | unavailable | 2/4 |

Response coverage was `strong_match` for 14 queries and `weak_match` for 10;
none returned `no_coverage`. Negative prompts still produce hits in all four
cases. Half had strong hits and half had weak-only hits, so the any-hit
negative metric remains 4/4 while the strength split distinguishes two
different failure classes. Do not treat weak-only hits as safe answers.

Natural-language strict gold-path recall was 4/7 (MRR .3571); it remains below
the 85% target. Snake-case MRR was .05 with zero recall@5. The unchanged gold
set was not adjusted to fit results. No new manual fact-equivalence review was
done for this v2 run, so only strict-path metrics are claimed here.

The three strict natural-language misses with their top locator are actionable
relevance cases:

| Task | Query | Gold path | Top returned locator |
|---|---|---|---|
| `c02` | “How does a remote MCP request travel through the Airlock bridge?” | `docs/runbooks/airlock-app-bridge.md` | `docs/inventory/sites/airlock.md:134` |
| `e02` | “What remains available if server-100 fails?” | `infra/access-topology.md` | `docs/inventory/devices/android.md:14` |
| `f02` | “Which nodes must be able to reach the Windows operator workstation?” | `infra/access-topology.md` | `docs/inventory/devices/windows.md:12` |

All four negative queries returned hits. The personal S21 PIN query had only
weak matches (top `docs/projects/MATRIX.md:303`); the personal Matrix room
query also had only weak matches (top `docs/projects/GEN.md:6`). The router SSH
key-value query produced a strong hit (`docs/inventory/services/exmanager.md:165`),
as did the Windows password query (`docs/inventory/devices/windows-beyondinfinity.md:9`).
The benchmark records locator metadata only, not document text or secret
values. These strong negatives are abstention/relevance defects for the caller.

## Interleaved exact latency

Baseline `19429` and candidate `19430` were queried alternately for the same
three representative tasks, 30 pairs each (90 pairs, 180 successful calls).
All calls were complete and untruncated. Exact output matched across every
pair: paths, relevant ranks, reciprocal rank, nDCG, recall, and negative-hit
status.

| Engine | Warm P50 | P95 | P99 |
|---|---:|---:|---:|
| Baseline exact | 113.09 ms | 151.19 ms | 172.45 ms |
| Candidate exact | 125.74 ms | 151.82 ms | 159.24 ms |

Per-task P95 baseline→candidate was `a01` 121.29→139.16 ms, `a02`
146.28→146.50 ms, and `d04` 151.19→154.05 ms. Aggregate paired latency
paired latency deltas had P50 +14.37 ms and P95 +42.99 ms. The task-level P95s
remain close; pair-difference tails and concurrent indexing make this a noisy
shared-host measurement, not evidence of a stable regression. Candidate
aggregate P95 is below the 206.56 ms comparison threshold.

The ranked-mode warm P50/P95/P99 during candidate measurements was
180.25/252.39/292.91 ms. This was collected while a separate full-source
indexing job increased host I/O pressure, so it should not be compared directly
with the earlier v1 ranked latency sample.

## Host and corpus integrity

During ranked measurements, load1 ranged 47.88–49.63, available memory stayed
above 27.82 GiB, aggregate CPU busy averaged 30.19% (max 38.29%), and I/O PSI
`some avg10` ranged up to 21.12%. During the interleaved exact run, load1 ranged
46.98–48.81, available memory was 27.37–29.46 GiB, CPU busy averaged 33.63%
(max 42.3%), and I/O PSI `some avg10` ranged 21.0–28.19%. These measurements
confirm the host was under background pressure; they do not apportion latency
to an individual process.

SHA-256 manifests before and after the v2 quality and interleaved runs recorded
299 files / 2,359,136 bytes under the docs root, with zero added, removed or
changed files. Git HEAD remained `6299c0efacf81d53ee0f34ca19d16f40641dbc9f`;
the scoped existing dirty paths were unchanged. No historical baseline hash
manifest exists. XERJ reported 15,042 records but no index timestamp or file
hash metadata, so source freshness of that comparison index remains unknown.

These measurements are client-observed Streamable HTTP timings. GrepMesh does
not expose a per-search engine duration in this response, so engine time and
MCP transport overhead cannot be separated. Native agent token usage remains
unknown; no token-savings claim is supported.

Receipts: `candidate-ranked-v2.jsonl`, `candidate-ranked-v2.host.json`,
`latency-interleaved-v2.jsonl`, `latency-interleaved-v2.host.json`,
`docs-files-pre-v2.json`, `docs-files-post-v2.json`, and
`xerj-snapshot-pre-v2.json`.
