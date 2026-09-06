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

## Optional GPTAdmin Network Tunnel fallback

`routable_url` is always tried first. `gptadmin_proxy_url` is optional and is
used only when the direct TCP connection cannot be made. It must be a
credential-free loopback HTTP CONNECT endpoint, normally
`http://127.0.0.1:3126`, supplied by a locally managed GPTAdmin Network Tunnel
client. GrepMesh does not create capabilities, issue grants, select an agent,
or store relay credentials.

For M1 to a LAN-only mini endpoint, the operator must first deploy the
GPTAdmin relay and a local connector that obtains a fresh approved `lan`
capability grant for the mini's exact address and port. Configure the mini's
plain `http://…/mcp` URL as `routable_url` and the connector's loopback address
as `gptadmin_proxy_url`. Do not add a relay URL, a Hub credential, or a
capability ID to GrepMesh. If the connector is unavailable or the capability
is denied, GrepMesh reports the fallback failure in that peer's status.

Available tools: `search_text`, `find_paths`, `read_text`, and `search_status`.
