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
without measuring a complete scan first.

Any budget increase requires fresh steady/peak measurements on that host, an
impact review for co-tenants, a documented change here, and a post-change
consumer canary.
