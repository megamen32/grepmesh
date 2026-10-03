# Multi-machine setup

Install GrepMesh on each Linux machine. Each node gets a unique `host_id`, one
or more narrow named roots, and the routable URLs of its peers.

```json
{
  "host_id": "workstation-a",
  "bind": "10.0.0.10:9419",
  "local_bind": "127.0.0.1:9419",
  "root": "/home/user/projects",
  "roots": {
    "projects": ["/home/user/projects"]
  },
  "peers": [
    {
      "host_id": "workstation-b",
      "local_url": "http://127.0.0.1:9419/mcp",
      "routable_url": "http://10.0.0.11:9419/mcp",
      "gptadmin_proxy_url": "http://127.0.0.1:3126"
    }
  ],
  "peer_auth_token_env": "GREPMESH_PEER_TOKEN"
}
```

Each node keeps its own persistent SQLite FTS index (under `~/.cache/grepmesh/` by default). Office documents, EPUB, RTF, CSV, and text-based PDFs are converted locally with Firecrawl AnyDoc before indexing. The documents themselves stay on that node. A search with `hosts: "*"` fans out to every reachable node and merges the results, so the mesh behaves as one logical index without a central document store. Scanned-PDF OCR is not enabled by the Rust integration.

GrepMesh listens on `127.0.0.1:9419` by default. For GPTAdmin Network Tunnel
deployments, keep that loopback default and do not add a second peer token.
`peer_auth_token_env` is optional and enables bearer authentication explicitly
for deployments that expose GrepMesh on another transport. Named roots are a convenience and performance control, not a security boundary: choose the obvious directories you want searchable, and add narrower exclusions only when you explicitly need them.

## GPTAdmin registry and relay fallback

Every node remains a complete local index. Register its loopback endpoint as a
GrepMesh child MCP in that host's ShellMCP supervisor. Configure
`gptadmin_topology_url`, `gptadmin_token_env`, `topology_cache_path`, and a
bounded `topology_ttl_ms`. GrepMesh reads the Hub projection and child registry,
probes each child with `list_locations(hosts="local", hop_count=1)`, and caches
only routes and host IDs. Tokens never enter the topology cache.

Use a managed credential restricted to the GrepMesh tools (`search`, the
legacy `search_text` alias, `find_paths`, `read_text`, `list_locations`,
`list_directory`, and `search_status`). A Hub owner/control token is not an
acceptable node credential. Fixed machines may keep direct private-network
routes and use the relay after connection failure. Do not retain a stale LAN
route for a roaming laptop: a non-connect HTTP error on the obsolete direct
route is intentionally surfaced rather than silently bypassed.

For a roaming Mac, use `bind: "127.0.0.1:9419"`, `local_bind: null`, and an
empty static `peers` list. The LaunchAgent should execute
`deploy/grepmesh-mcp-wrapper-macos`, which reads the private STT and GPTAdmin
environment files before starting the binary. This keeps local search working
off-LAN while remote searches use the authenticated Hub relay.

The checked-in fleet examples are `deploy/mvp-config-server-100.json`,
`deploy/mvp-config-server-88.json`, `deploy/mvp-config-server-44.json`, and
`deploy/config-mac-m1.json`. Linux services use the resource envelope in the
root `AGENTS.md` and `grepmesh-mcp.service`; do not deploy a service whose
effective user cannot write `/var/lib/grepmesh-mcp`.

Available tools: `search`, `find_paths`, `read_text`, `list_locations`,
`list_directory`, and `search_status`. `search_text` remains an accepted
compatibility alias for older peers.
