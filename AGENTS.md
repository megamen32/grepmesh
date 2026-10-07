# GrepMesh agent instructions

## Resource budget

GrepMesh is a fleet-wide indexer on shared hosts. Do not run it, its test
suite, or a release build outside an explicit cgroup budget.

For the measured 125 GiB Linux hosts (server-100, server-88, and server-44),
the reviewed runtime envelope is:

- `MemoryHigh=4G`, `MemoryMax=6G`, `MemorySwapMax=1G`;
- `CPUQuota=200%` and `TasksMax=384`;
- `IOWeight=20` so background indexing yields to interactive and database IO;
- one GrepMesh rebuild per host at a time.

The values are an outer safety boundary, not a target. On 2026-10-03 the
resident process was about 0.6 GiB on server-88; its 25 GiB cgroup charge was
mostly inactive file cache from indexing. `MemoryHigh` must remain in place so
that cache is reclaimed before it displaces other tenants. Do not raise these
limits to accommodate file cache.

The effective service user must own and be able to write
`/var/lib/grepmesh-mcp`. A stale numeric owner previously prevented topology
cache writes on server-88 and server-44, silently collapsing them back to
static peers. Verify ownership after any unit-user or package migration.

For builds and tests use at most two Cargo jobs, `CPUQuota=200%`,
`MemoryHigh=4G`, `MemoryMax=6G`, `MemorySwapMax=1G`, and `TasksMax=512` in a
transient systemd unit. Run the narrowest relevant test first. Keep build and
temporary artifacts below 20 GiB and do not run a second heavy workload in
parallel.

The Mac LaunchAgent is not protected by systemd. Keep local indexing roots
narrow, retain the configured rebuild interval, and do not add broad roots
without measuring a complete scan first. Its reviewed operating budget is one
index/reconciliation worker, less than 2% CPU averaged over a 60-second idle
window, less than 1 GiB resident memory in steady state (2 GiB fault ceiling),
no sustained swap growth, at most 64 tasks, and at most 8 GiB for the SQLite
database plus WAL/SHM and 2 GiB of build/test temporary data. Full rebuilds stay
at least 24 hours apart on the current Mac roots. A rebuild may use one core,
but it must finish and return to the idle budget; continuous one-core use is a
defect, not an accepted background allowance.

Any budget increase requires fresh steady/peak measurements on that host, an
impact review for co-tenants, a documented change here, and a post-change
consumer canary.
