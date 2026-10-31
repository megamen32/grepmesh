# Storage follow-up and Git refresh serialization — 2026-10-31

## Confirmed action on server-44

- Root filesystem before: 20,426,481,664 B available (96% used).
- Root filesystem after: 21,326,221,312 B available (still 96% used).
- **Actual increase: 899,739,648 B = 0.838 GiB**, measured with `df -B1`.
- Commands performed: `apt-get clean` (package cache: 174,141,936 B → 61,440 B) and `gzip -1 /var/log/syslog.1` (572,956,850 B → 56,457,771 B). Verified `syslog.1` was not held open by any process. Active `syslog` was not modified.
- Native GrepMesh indexes, cloud search service, databases, Docker volumes and mounted filesystems were not deleted or altered by these operations.

## Potentially recoverable Docker volume, retained

Docker reports 80 dangling volumes on server-44, about 9.01 GiB in total. One named volume `telegramauto-quorum-pg44-data` occupies **8,822.54 MiB**. Its current Docker status is dangling and the running `telegramauto-quorum-pg44` PostgreSQL container mounts `/srv/quorum-pg/data`, not this volume.

**Do not delete on that evidence alone**: read-only `pg_controldata` shows the old cluster and live cluster have different system identifiers, so the old volume is not a bitwise copy of the active database and may contain unique historical material. Existing on-host TelegramAuto backups date from October 3 and are insufficient evidence that the older cluster can be discarded. Make a verified external backup/restore before reclaiming this volume. No Docker volume was removed.

Likewise, the live `k3s-demo-data.ext4` file (20 GiB) and `telegramauto-ha/co-state-v2/state.ext4` are attached to loop devices and were not touched.

## Cloud GrepMesh state

The existing unified `server-44-cloud-text` service reports 896 documents, a shared 512 MiB RAM cache limit, and `persistent_index_bytes=0`. Production legacy FTS SQLite files remain on the 100/44/88 hosts while their coverage is not replaced.

Main GrepMesh remote full-text `search` (with no delegated verified filesystem ACL) still returns a partial result: `remote search requires a verified GPTAdmin filesystem ACL`. Never forge or bypass this ACL; a separate caller identity with the correctly scoped access profile is required.

## Concurrent refresh safety

`scripts/cloud_index/refresh_git.py` now shares the nonblocking tmpfs `publisher.lock` with the existing manual cloud publisher. It refuses simultaneous writes, skips unchanged corpora, aborts an incomplete or unexpectedly empty/shrunken source stream, preserves old immutable generations and releases the lock on source failure. Five new targeted regression assertions bring the updater test harness to **19 passing cases**.

The updated refresher is **committed to the repo, not yet deployed as a timer**. Prior transfer to server-44 was blocked by the execution safety guard; do not route around that control. Source installation and the 81.1 GiB native index cutover have not taken place.
