---
name: grepmesh-search
description: Use GrepMesh only for unknown locations, cross-host search, or configured mesh scopes. Prefer rg for a known local checkout or path because its output is usually more context-efficient.
---

# GrepMesh Search

Use `rg` first when the exact local checkout or path is already known. Its compact per-line output is normally the lowest-context route.

Use GrepMesh when the location is unknown, results may live on another mesh host, or configured mesh roots/scopes are required. Start with `search_text` or `find_paths`, constrain `roots`, and use `read_text` only for an exact result file.

Never substitute a partial mesh response for a complete search: surface `host_status`, `partial`, and `truncated` state.
