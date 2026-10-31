# Cloud index: compact contentless FTS5 on real Airlock corpus (2026-10-31)

## Result

Published a new immutable Yandex.Disk generation of `airlock-members-cloud` with **FTS5 `detail=none,columnsize=0,content=''`**, a single compressed body store, and bounded AND-of-individual-trigram candidate retrieval followed by exact substring verification. Prior cloud generations and the original server-100 GrepMesh FTS database remain intact. This improvement does NOT yet reclaim the native server SSD.

### Identical corpus size comparison

Same 688 tracked, authenticated files from `/home/roomhacker/agents-projects/airlock-members-api`, no document content printed into reports:

| Metric | Previous full-detail FTS | Optimized contentless detail=none |
|---|---:|---:|
| Full-text SQLite shard files, raw | 28,942,336 B | 5,808,128 B |
| Full-text cloud shards, compressed | 15,184,713 B | **2,843,267 B** |
| Number of full-text shards | 2 | 2 |
| Publishing build (CPU-bounded, memory-only, in-memory store) | 5.81 s | 4.89 s |
| Query-count parity with source, bounded | 16/16 | **16/16** |

Compressed full-text shard reduction: **81.28%**. The path-only index is unchanged at 323,584 B raw, 64,740 B compressed. No new index is written to source SSD.

### Real Yandex.Disk publication and verification

- New immutable cloud generation successfully published at `yadisk:Backups/GrepMesh/cloud-index-v1/`; SHA-256 and uploaded MD5 verified per object.
- Explicit prior scope `latest.json` pointer saved to a mode-0600 file in server-44 tmpfs; immutable previous objects retained as rollback.
- Fetched the optimized generation from cloud into empty RAM and compared it to the **original tracked source files** for 17 query patterns: **17/17 identical path sets, no partial responses**. The very broad `token` query exceeded the 200-result verification window and was excluded, not counted as pass.
- Cold `oauth`: 108 matches, two cloud shard downloads, 3.04 seconds. Warm `oauth`/other terms generally tens of milliseconds; after clearing the RAM cache, new two-shard fetch ~6.97 seconds in that sample. These numbers are not a network or production SLA.
- GrepMesh `server-44-cloud-text` user service restarted and confirmed ready with **896 total documents across 3 declared scopes**, **0 persistent index bytes**, and a shared 512 MiB RAM cache limit. The path routing indexes occupy ~463 KiB RAM immediately after startup.
- Authenticated direct cloud full-text consumer: `oauth` 108 matches, `финанс` 15 matches, both without partial results. Main GrepMesh scoped `find_paths(*.vue)` returns **56 hits, partial=false**, without downloading text shards on that request.
- An initial `find_paths` request run concurrently with two cold content searches returned an initial GPT Admin child timeout. Repeating the consumer canary serially succeeded; service load/transport behavior under concurrent cold misses remains a rollout gate.
- Main GrepMesh federated full-text through the native ACL gate is NOT yet enabled; the authenticated child service remains the read path for actual snippets. Do not disable/forge the ACL check.

### Implementation details

- Old stored full-detail FTS cannot answer the new detail-none format with the old phrase `MATCH` query, so deploy the reader first. The new reader also queries old segments using a conjunction of safe three-character tokens.
- Queries containing shorter or tokenizer-unfriendly content, and segments with Unicode expanding case-folding, use exact per-document fallback rather than pretending no match.
- Synthetic regression: **37 cloud-core assertions**, **10 legacy PDF/OCR bootstrap assertions**, **12 multi-root assertions** (all pass). Mixed source formats `.vue`, `.proto`, `.jsonl`, `.mjs`, `.cjs` are included in explicitly scoped source policies.
- The Airlock code corpus now uses its **direct Git source scan for updates**, not the 81 GiB local SQLite. During source migration, all 684 documents present in the old FTS and Git had identical content SHA-256; Git added 4 more safe files, with 2 secret-named files excluded.

### Remaining work before fleet native-index deletion

1. Existing native GrepMesh index on server-100 is **81.1 GiB**; 44/88/Macs retain their native indexes. New cloud pilots cover only a small, explicit subset of those corpora.
2. Implement incremental direct extraction for non-Git files, PDF/OCR/audio source changes, proper deletion/tombstones and reliable multi-host reconciliation without the old source-side SQLite.
3. Replace the current <=500-shard-per-scope and <=8-scope routing limits with a bounded hierarchical global catalogue and predictable cold-query behavior.
4. Complete and audit authenticated filesystem ACL propagation through native GrepMesh full-text federation, including outage semantics.
5. Once full-corpus parity, backups and rollback are proven, switch native writers and then reclaim the large SSD files.

Rollback: restore the retained previous manifest pointer and restart the unified cloud reader if a real search parity failure appears. Never delete the old native FTS until the coverage gates have passed.
