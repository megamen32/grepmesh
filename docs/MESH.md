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

Use a private management network or VPN for peer URLs. Give every node the same
non-empty `GREPMESH_PEER_TOKEN`. Keep named roots narrow: searching a project
root is faster and more predictable than scanning an entire home directory.

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
