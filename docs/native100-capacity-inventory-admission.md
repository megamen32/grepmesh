# Native100 capacity preflight: current result is NOT ADMITTED

Baseline: 50e56651ee099f00e210e34d2a362939336bdec2.
No source archive body was read, no partial oversized archive was accessed,
and no production builder, runtime route, credential or ACL changed here.

## Actual inventory blocker and exact source fix

The original inventory loader failed on 2026-11-03 with
"Duplicate object in cloud inventory" before any source/batch publication.
A bounded, original-lock-protected metadata-only diagnostic captured the
complete existing lsjson output: 5,605 rows, 5,604 distinct valid SQLite keys,
ONE byte-for-byte-equivalent JSON metadata record repeated, zero conflicting
same-name records, zero malformed records. The saved 1,845,527-byte listing has
SHA256 e79bdc094f231815b16f74ee11d195bcde103e43a6cafe0b0a6184428fea2d6d.
No source part or object body was downloaded by this diagnostic.

The patch accepts only an identical FULL metadata record repeated in the same
complete bounded listing. Size, MD5, path, file type and all previous checks
are performed before deduplication. Different ID, timestamp, size, hash or any
other field remains an error. Distinct keys never collapse. Cloud transport,
write/reuse logic, listing byte cap, caller identity and all reader/admission
limits are unchanged. This fixes a reproducible parser rejection, not ACLs or
cloud object consistency. No cloud object is rewritten or removed.

27 tests passed (8 new + 19 existing verified-reuse tests), including offline
replay of the actual captured listing, identical and conflicting duplicates,
malformed data, lost upload acknowledgement and unchanged error/timeout behavior.
The cached-list replay is NOT another cloud request or a runtime acceptance.

## Full-snapshot evidence and missing dimensions

The full independent source audit covers 443 parts / 3,187,870 source rows,
including 2,696 oversize rows and 3,185,174 searchable rows. It persisted only
EIGHT sha256(path)[0] % 8 aggregate counts:
397081, 397508, 399271, 398396, 397777, 397712, 399049, 398380.
Its source retained exact path hashes only in a process-local set, not a
sixteen-partition metadata projection. These eight counts are sums of pairs
(i, i+8), not individual counts for the current 16-partition layout.

Using those complete eight counts and already measured cache425 counts yields
only ranges for the sixteen individual counts. The upper endpoints imply
762..767 count-only shards for an individual partition, exceeding 500. This
DOES NOT prove that the actual snapshot overflows: these are evidence bounds,
not an extrapolation, measurement, probability claim or predicted distribution.

The global count-only floor is 6,222 shards versus 8,000 total slots. There
are 45 source checkpoint batches. Neither number includes actual per-partition
byte-pressure flushes, queue-pressure flushes, batch tails or SQLite splits.
Those dimensions are NOT present in the saved full-source audit. No safe
per-partition bound on the final path-index bytes is available either.

## Dimensions that can be bounded without source rereads

- Inventory: 5,604 current unique objects + at most 8,000 admitted FTS shards
  + 16 path indexes = 13,620, below the 32,768-object admission cap for one
  successful run. This assumes the per-partition 500-shard bound is proved.
- Fixed manifest emitter: a maximum-width synthetic 500-descriptor JSON sizing
  probe was 11,126,410 bytes, below 16,777,216 bytes. This is a descriptor-size
  test, NOT a fabricated source manifest or full-corpus capacity acceptance.
- The source425 actual 224-shard result remains valid but does not fill in
  missing full-snapshot partition/size/split data.

## Next permitted boundary

No full builder start follows from this assessment. A complete, pinned
16-partition capacity profile must include batch counts and text-size/order
information, queue-pressure bounds, verified SQLite split/size bounds and
path-index sizing. Supply an existing such metadata artifact, or explicitly
permit a serialized read-only metadata/admission pass over immutable source
parts not already available in cache. Persist metadata checkpoints so that
completed source portions are not repeatedly downloaded. Do not restore the
old SQLite, run another builder or relax caps to compensate for missing proof.

The actual source425/424 caches remain untouched. The separately denied
partial-oversize reconciliation remains forbidden. Real signed-client
search/read_text and post-reboot/rollback gates remain independent and open.
