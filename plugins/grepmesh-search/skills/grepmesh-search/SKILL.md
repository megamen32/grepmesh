---
name: grepmesh-search
description: Use GrepMesh only for unknown locations, cross-host search, or configured mesh scopes. Prefer rg for a known local checkout or path because its output is usually more context-efficient.
---

# GrepMesh Search

Use `rg` first when the exact local checkout or path is already known. Its compact per-line output is normally the lowest-context route.

Use GrepMesh when the location is unknown, results may live on another mesh host, or configured mesh roots/scopes are required. The single entrypoint is the local MCP endpoint `http://127.0.0.1:9419/mcp` (`scripts/grepmesh_client.py` implements this contract; `scripts/test_grepmesh_client.py` pins it with stub tests only — real proof is a live run).

## Explicit hosts, never wildcard

Always pass an explicit `hosts` array of known-healthy hosts. For the current MVP mesh that is exactly:

```json
["server-100", "server-88"]
```

Never rely on `hosts="*"`: broken peers (today `mac-m1`, `server-44`, `mac-mini`) make every wildcard answer `partial=true`. If you need more hosts, name them explicitly and only after they are known healthy.

## Async search contract

Call the tool named `search` (compat alias `search_text`) with the key arguments:

- `query` (required), `hosts` (explicit array, see above)
- `wait_ms`: keep it small (hundreds of ms, e.g. 500) — never the 30s default
- `max_matches`: bound the response
- `mode`: `literal` | `regex` | `case_insensitive_literal`
- optional `path_globs`, `roots`, `context_lines`, `verbose`

If the response has `state="running"` plus an opaque `job_id`, that is not an error. You MUST continue calling `search_status` with `job_id` (schema: `job_id` string; optional `cursor` string, `page_size` integer >= 1) until a terminal state — `complete`, `failed`, `expired`, or `lost`. Poll with a bounded deadline (~90s) and growing interval (~0.5s to 2s). Do not stop after one poll, and never present a running answer as final.

A terminal `failed` / `expired` / `lost` is an error to surface, not an empty result.

## Honest result surface

Every search payload carries `results`, `host_status` (per-host `host_id`, `ok`, `state`, `error`), `partial`, and `truncated`.

- Pass `host_status`, `partial`, and `truncated` through untouched and report them.
- Never present a `partial=true` or `truncated=true` response as complete; say which hosts failed and why.
- Never retry by silently dropping failed hosts.

## Reading a result

Use `read_text` with `host` and the exact `path` from a search hit (optionally `start_line` / `end_line`, both 1-based). Never guess paths, and never use `read_text` to browse.
