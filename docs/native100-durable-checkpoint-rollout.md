# GrepMesh native100 durable checkpoints and bounded reader acceptance

SOURCE CANDIDATE ONLY. No production route/ACL/OAuth, no original SQLite, no host reboot, no builder or second source/cloud read initiated in this patch.

## Writer commit and resume
- The immutable snapshot is pinned to 20261031T233108Z-5732b979: 443 source parts, 3,187,870 archived rows. This is NOT a consistent live snapshot.
- All derived checkpoint objects stay beneath the existing approved PRIVATE Yandex cloud destination yadisk:Backups/GrepMesh/cloud-index-v1, under private-source-checkpoints/native100/20261031T233108Z-5732b979. The build head.json is NOT a production index pointer.
- Default publisher behavior is unchanged. Explicit GREPMESH_DURABLE_BUILD_MODE=init requires authoritative absence of any prior head; resume verifies the exact pinned source digest, private head, immutable manifest, every metadata segment SHA/gzip/rows, remote shard inventory and per-partition path digest before skipping ANY source part.
- Every ten source parts, except last 441-443, flush all pending immutable FTS shards, verify publication, publish and read back a bounded compressed metadata segment, publish/read back hashed checkpoint manifest, and ONLY THEN advance the private checkpoint head. Progress JSON is informational and written after the confirmed batch commit.
- On restart, restore path metadata and original 16 partition prefixes from bounded cloud segments, without reading old source bodies. On ambiguous head writes, reconcile actual remote head; corrupt, missing or incomplete checkpoint fails closed. Uncommitted object-addressed shards are not trusted until independently reused/verified.
- Staging stays private tmpfs, bounded compressed 48 MiB, decoded 128 MiB, batch 200k records. Original builder cgroup remains MemoryHigh1.5GiB, Max3GiB, swap0, CPU70%, Tasks64, IOWeight10, Restart=no; no budget increase.

## Explicit operational gates
1. The terminal failure at source part 000425 offset0 timeout is STILL unresolved; previous part424 cached bytes do not fix 425. Do not silently restart. Coordinate owner scope and approved one-shot same-unit recovery.
2. Require project owner/release lease reconciliation, independent source review and required CI before merging or guarded deploy. Retain existing drop-ins, unit and rollback receipt.
3. Verify 443/443 source parts, exact source rows, hashes and 16 private candidate FTS manifests. No production pointer or SSD FTS.
4. Offload full server100 reader to host44 over existing authorized private network. Complete server44 oversized75, tail764 incl six deleted-but-preserved, 47 later high-rowid, plus OCR/STT/regex/live updates.
5. Verify genuinely authenticated dedicated-client signed Hub-to-private-reader positive search and read_text, and ACL-specific denial on /dev/null and /; no global OAuth binding, bearer/signature copying or fake _meta.
6. Canonical API ONLY search with rank=true default, rank=false opt-out; ranked_search absent. Compare true path/snippet and exact text from both source hosts, including deleted history. Preserve honest partial coverage warnings.
7. In a controlled tenant window, test actual cold reboot/restore, read-after-reboot and rollback. ONLY after all gates green perform guarded production cutover and disable the existing automation.

## Bounded query-cache contract (independent of builder)
The server44 private reader must fetch only required immutable cloud shards, not a complete RAM or SSD copy. Its shard cache remains <=512MiB and MemoryMax2GiB unless separately reviewed; verify per-shard compressed/raw bounds and SHA, bounded eviction, private loopback and signed per-request ACL. Search/read_text must not bypass ACL even when using cached content. An HTTP health 200 or unsigned denial is not signed-client acceptance. A green private publisher is not a real search/read_text parity result.

All current source tests are synthetic on in-memory cloud. They do NOT authorize release or prove cloud reader/ACL/reboot behavior.
