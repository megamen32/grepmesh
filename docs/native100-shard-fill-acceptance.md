# Native100 filled segments: source acceptance and integration boundary

This change is based on immutable eecb1d280dec42ef1c76d124b83ba93da81c2c57.
It does not install a service, start a production builder, change a reader/ACL,
or download source data. The real-data canary uses only the already verified
part425 local cache and emits aggregate metrics, never document contents.

## Defect and correction

The old global 512-document Python buffer is distributed across 16 hash buckets.
When it fills, the byte-largest bucket (often only tens of records) becomes a
whole immutable FTS shard. Part425 produced 1,833 such flushes for 110,282 rows.
Raising the global buffer or changing path routing is not a valid shortcut:
MultiReader.read_text chooses its native100 scope by sha256(path)[0] % 16.

The corrected builder retains that hash routing. Pending partial groups are
compressed, length-framed, SHA-checked records in 16 fixed private tmpfs queues.
Only one group is decoded, within the SAME global Python body/document limits.
A group normally flushes at 512 documents; earlier flushes occur only at its
8 MiB body bound, total scratch pressure, or a confirmed source-batch boundary.
No full path list or corpus-growing tmpfs spool is retained.

## Bounds, not additional allowances

- Python pending bodies: <=8 MiB and <=512 documents globally (unchanged).
- Serialized fill queues: at most one quarter of existing SQLite scratch cap,
  capped at 16 MiB, NOT another 16 MiB on top of the scratch allowance.
- Before SQLite creation, max_page_count is reduced by bytes in fill queues.
  Queue files + SQLite remain <=64 MiB, plus the small provenance marker.
- One metadata page <=256 KiB. Source packed/line bounds, 500 shards/partition,
  16 MiB manifests, 16 hash partitions, source/inventory caps are unchanged.
- Scratch, all queued frames and the immutable output format are verified.
  Unknown/symlink/hardlink/public-mode scratch fails closed before cleanup.
  Only marked reproducible scratch is reconciled after process loss; source
  cache, partial oversized archives and immutable objects are never removed.

## Checkpoint compatibility and failure behavior

All queues must drain before a commit/head can advance. The existing sequence
of immutable output -> metadata -> commit -> verified head remains intact.
The packing policy is pinned in commits and gets a distinct private v3-fill1
checkpoint namespace. No old head is overwritten or silently reinterpreted.
Old informational 420/443 is not a checkpoint. A first run in this namespace
starts at part1; a later new process resumes its own confirmed batches.

Synthetic regressions include actual SIGKILL/new-process recovery after queue
append, before shard creation, after shards, after metadata, before head,
after head before acknowledgement, and after confirmed checkpoint. Each
compares all paths and full bodies and proves that confirmed source parts
are not reread. Torn/missing/corrupt checkpoints, lost ACK, idempotency,
duplicates, source routing and bounded total scratch remain separate checks.

## Real part425 measurement contract

Run test_native100_part425_fill only with explicit opt-in, under a test cgroup
and the ORIGINAL private-build.lock, after original builder and transfer are
confirmed inactive. The source cache inode, size and SHA are checked read-only.
Before uses the exact immutable eecb packing method with counted flushes; it
is a lower bound on shards, not a claim that 1,833 SQLite shards were built.
After builds actual FTS SQLite shards and checks every stored path, SHA and
full body, then discards only its own test scratch. The echo sink is NOT durable
cloud evidence. No real checkpoint or catalog pointer is published.

## Scale and next integration step

A single-part result is not full-corpus admission. Near-full 512-record shards
remove the demonstrated tiny-shard defect; body-size pressure, SQLite splits,
batch tails and hash skew can still require more shards. Keep all caps and
fail closed, rather than silently enlarging the reader/cache budget.

Before one guarded same-unit run, review exact source/test hashes and the
real425 metrics; check full-corpus per-partition segment/manifest admission
using existing permitted aggregate evidence or a separately scoped bounded
admission pass. Preserve unit/drop-ins, cache424+425 and rollback. Do not
replay the rejected partial-oversize verification or start another builder.
The signed client, real search/read_text/history/live parity and reboot/rollback
remain separate production gates, not implied by these source/canary results.


## Recorded exact candidate measurements (2026-11-03 UTC)

- 51 synthetic tests PASS in 39.675s; seven actual SIGKILL/new-process tests,
  including death with a partially filled queue and between queue and shard.
- One real-cache test PASS in 123.978s: all 110,282 source425 rows, cache SHA
  2afeae188593ca70e437df139a375ce8071c93eda504a0e9d1ebc8ef93db36dd.
- Before: 1,833 counted flushes / minimum shards (112..117 per hash partition),
  mean60.1648 docs / 11.7509% of512, zero full512 groups. No old FTS build.
- After: 224 ACTUAL FTS shards (14 per partition), 208 full512 +16 batch tails,
  mean492.3304 docs /96.1583% of512. No byte-pressure/scratch-pressure flush.
  Every stored path and full body was compared; no records lost or duplicated.
- Combined queue+SQLite peak4,551,006B; queue peak3,802,251B; decoded pending
  peak845,282B/512 docs; largest raw shard1,277,952B. Worker RSS40,436KiB;
  whole test cgroup peak49,602,560B, memory.high/max/OOM events all0.
- Test cgroup MemoryHigh256MiB/MemoryMax512MiB,swap0,CPU70%,Tasks32,IOWeight10.
  Cloud calls0. The original lock was held. The original service was not started.

These numbers establish the corrected packing on THIS real part. They do not
establish whole-corpus500/partition admission, true signed-client ACL, cloud
transport durability or a production search result. The before count-only
run and after actual SQLite build are not comparable performance timings.
