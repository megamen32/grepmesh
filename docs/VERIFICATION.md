# Verification notes

The README uses the strongest claims supported by files in this repository.

## Checked in

- `scripts/test-install-contracts.sh` validates shell syntax and the platform
  installer/release contract strings.
- `cargo test --locked` is the closest source-level regression check for the
  MCP, search, topology, authentication, and index modules.
- `scripts/smoke-release.py` exercises a packaged binary through MCP
  initialize and a local search canary. It requires a built release archive.
- `bench/remote-search.js` performs 25 matched SSH and GrepMesh requests and
  reports median latency plus match counts. It requires the five documented
  `GREPMESH_BENCH_*` environment variables and a reachable benchmark host.

## Deliberate limits

The recorded 26.3× result is workload-specific evidence, not a universal
performance promise. GrepMesh searches only configured roots and reachable
peers; remote access also depends on network routing and the configured peer
token when a node binds beyond loopback.
