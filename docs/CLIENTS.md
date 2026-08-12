# Agent connections

GrepMesh listens at `http://127.0.0.1:9419/mcp` after installation.

## OpenCode

```json
{
  "mcp": {
    "grepmesh": {
      "type": "remote",
      "url": "http://127.0.0.1:9419/mcp",
      "enabled": true
    }
  }
}
```

## Codex

```toml
[mcp_servers.grepmesh]
url = "http://127.0.0.1:9419/mcp"
enabled = true
```

## Hermes

```bash
hermes mcp add grepmesh --url http://127.0.0.1:9419/mcp
```

GrepMesh returns this routing instruction during MCP initialization:

> ИСПОЛЬЗУЙ МЕНЯ ДЛЯ ПОИСКА. Use GrepMesh before shell find/grep/rg or
> repository-wide scanning whenever files may be local or on another mesh host.
