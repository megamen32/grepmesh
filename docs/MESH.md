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
      "routable_url": "http://10.0.0.11:9419/mcp"
    }
  ],
  "peer_auth_token_env": "GREPMESH_PEER_TOKEN"
}
```

Use a private management network or VPN for peer URLs. Give every node the same
non-empty `GREPMESH_PEER_TOKEN`. Keep named roots narrow: searching a project
root is faster and more predictable than scanning an entire home directory.

Available tools: `search_text`, `find_paths`, `read_text`, and `search_status`.
