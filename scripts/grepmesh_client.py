#!/usr/bin/env python3
"""GrepMesh MCP client and CLI implementing the fixed MVP client contract.

Talks to the local GrepMesh MCP endpoint (default
``http://127.0.0.1:9419/mcp``) with JSON-RPC 2.0 over HTTP POST using only
the Python standard library (Python >= 3.8).

Contract implemented here (verified against the grepmesh source and live
probing of ``127.0.0.1:9419``):

- Every JSON-RPC POST must carry ``Mcp-Method: <method>``; a
  ``tools/call`` additionally requires ``Mcp-Name: <tool-name>``
  (live endpoint answers HTTP 400 with JSON-RPC error -32020 "Mcp-Method
  header is required" otherwise).
- ``search`` (compat alias ``search_text``) with a small ``wait_ms`` may
  answer ``state="running"`` plus an opaque ``job_id``. That is not an
  error: the caller must keep polling ``search_status`` with ``job_id``
  (optionally ``cursor``/``page_size``) until a terminal state
  (``complete`` | ``failed`` | ``expired`` | ``lost``).
- Every search payload carries ``results``, ``host_status`` (per-host
  ``host_id``/``ok``/``state``/``error``), ``partial`` and ``truncated``.
  This client passes them through untouched; it never rewrites a partial
  answer into a complete-looking one and never drops failed hosts.

Testing honesty: the unittest suite in ``scripts/test_grepmesh_client.py``
runs against a stub ``http.server`` instance. Those are narrow
local-logic tests only (request shape, headers, polling, error surfacing).
They are NOT evidence that the GrepMesh mesh works; real end-to-end proof
happens against the live runtime separately.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

DEFAULT_BASE_URL = "http://127.0.0.1:9419/mcp"
MCP_PROTOCOL_VERSION = "2026-07-28"
MVP_HOSTS = ["server-100", "server-88"]

RUNNING_STATE = "running"
COMPLETE_STATE = "complete"
# Terminal job states reported by the search_status envelope (src/jobs.rs).
TERMINAL_JOB_STATES = ("complete", "failed", "expired", "lost")

DEFAULT_WAIT_MS = 500
DEFAULT_POLL_DEADLINE_S = 90.0
DEFAULT_POLL_INTERVAL_S = 0.5
MAX_POLL_INTERVAL_S = 2.0
DEFAULT_HTTP_TIMEOUT_S = 40.0


class GrepMeshError(Exception):
    """Base class for every GrepMesh client failure."""


class GrepMeshHTTPError(GrepMeshError):
    """The MCP endpoint answered with a non-200 HTTP status."""

    def __init__(self, status: int, body: str = "") -> None:
        self.status = status
        self.body = body
        detail = ": {}".format(body[:200]) if body else ""
        super().__init__("HTTP {} from GrepMesh MCP endpoint{}".format(status, detail))


class GrepMeshProtocolError(GrepMeshError):
    """Malformed transport body or a JSON-RPC error object."""


class GrepMeshContractError(GrepMeshError):
    """A payload violates the fixed client contract."""


class GrepMeshJobError(GrepMeshError):
    """A search job reached a non-complete terminal state."""

    def __init__(self, state: str, payload: Dict[str, Any]) -> None:
        self.state = state
        self.payload = payload
        super().__init__(
            "search job reached terminal state {!r}; payload carried through, "
            "not retried".format(state)
        )


class GrepMeshTimeoutError(GrepMeshError):
    """A running job did not reach a terminal state within the poll deadline."""


def _rpc(
    base_url: str,
    method: str,
    params: Dict[str, Any],
    timeout: float,
    extra_headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": MCP_PROTOCOL_VERSION,
        # The live endpoint rejects every request without Mcp-Method
        # (HTTP 400, JSON-RPC -32020); tools/call also needs Mcp-Name.
        "Mcp-Method": method,
    }
    if extra_headers:
        headers.update(extra_headers)
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode("utf-8")
    request = urllib.request.Request(base_url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as err:
        detail = ""
        try:
            detail = err.read().decode("utf-8", "replace")
        except Exception:
            pass
        raise GrepMeshHTTPError(err.code, detail) from None
    except urllib.error.URLError as err:
        raise GrepMeshError(
            "cannot reach GrepMesh MCP endpoint {}: {}".format(base_url, err.reason)
        ) from None
    try:
        envelope = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as err:
        raise GrepMeshProtocolError("malformed JSON-RPC body: {}".format(err)) from None
    if not isinstance(envelope, dict):
        raise GrepMeshProtocolError("JSON-RPC envelope is not an object")
    if envelope.get("error") is not None:
        error = envelope["error"]
        raise GrepMeshProtocolError(
            "JSON-RPC error {} from {}: {}".format(
                error.get("code"), method, error.get("message")
            )
        )
    result = envelope.get("result")
    if not isinstance(result, dict):
        raise GrepMeshProtocolError("JSON-RPC response has no result object")
    return result


def call(
    base_url: str,
    tool: str,
    args: Dict[str, Any],
    timeout: float = DEFAULT_HTTP_TIMEOUT_S,
) -> Dict[str, Any]:
    """POST ``tools/call`` for ``tool`` and return the parsed result payload.

    The live endpoint requires ``Mcp-Method: tools/call`` (sent by
    ``_rpc`` for every request) plus ``Mcp-Name: <tool>`` on tools/call;
    without them it answers JSON-RPC error -32020 "Mcp-Method header is
    required".

    The MCP result must be ``result.content[0].text`` holding a JSON object
    string; anything else is a protocol error. Non-200 answers raise
    ``GrepMeshHTTPError`` carrying the status code. The parsed payload is
    returned verbatim (search envelopes carry ``partial``/``truncated``/
    ``host_status`` at the top level; nothing is unwrapped or rewritten).
    """
    result = _rpc(
        base_url,
        "tools/call",
        {"name": tool, "arguments": args},
        timeout,
        extra_headers={"Mcp-Name": tool},
    )
    content = result.get("content")
    if not isinstance(content, list) or not content:
        raise GrepMeshProtocolError("tool result has no content array")
    first = content[0]
    if not isinstance(first, dict) or not isinstance(first.get("text"), str):
        raise GrepMeshProtocolError("tool result content[0] has no text")
    try:
        payload = json.loads(first["text"])
    except ValueError as err:
        raise GrepMeshProtocolError("tool result text is not JSON: {}".format(err)) from None
    if not isinstance(payload, dict):
        raise GrepMeshProtocolError("tool result text is not a JSON object")
    return payload


def list_tools(base_url: str, timeout: float = DEFAULT_HTTP_TIMEOUT_S) -> Dict[str, Any]:
    """Return the raw ``tools/list`` result object."""
    return _rpc(base_url, "tools/list", {}, timeout)


def search_status(
    job_id: str,
    cursor: Optional[str] = None,
    page_size: Optional[int] = None,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = DEFAULT_HTTP_TIMEOUT_S,
) -> Dict[str, Any]:
    """Poll one async search job by its opaque ``job_id``.

    Public argument schema confirmed from src/server.rs (tool_meta):
    ``job_id`` (string), optional ``cursor`` (string), optional
    ``page_size`` (integer >= 1), optional ``hosts``.
    """
    args: Dict[str, Any] = {"job_id": job_id}
    if cursor is not None:
        args["cursor"] = cursor
    if page_size is not None:
        args["page_size"] = int(page_size)
    return call(base_url, "search_status", args, timeout=timeout)


def _await_terminal(
    payload: Dict[str, Any],
    base_url: str,
    timeout: float,
    poll_deadline_s: float,
    poll_interval_s: float,
    poll_interval_max_s: float,
    sleep=time.sleep,
) -> Dict[str, Any]:
    """Resolve a possibly-running search envelope to its final payload."""
    deadline = time.monotonic() + poll_deadline_s
    interval = poll_interval_s
    while payload.get("state") == RUNNING_STATE:
        job_id = payload.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            raise GrepMeshContractError(
                'search response has state="running" but no opaque job_id'
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise GrepMeshTimeoutError(
                "search job {} still running after {:.0f}s poll deadline; "
                "keep polling search_status or raise the deadline".format(job_id, poll_deadline_s)
            )
        sleep(min(interval, remaining))
        payload = search_status(job_id, base_url=base_url, timeout=timeout)
        interval = min(interval * 2, poll_interval_max_s)
    state = payload.get("state")
    if state is None or state == COMPLETE_STATE:
        return payload
    if state in TERMINAL_JOB_STATES:
        # failed/expired/lost are explicit errors; never pretend they are
        # an empty but successful result.
        raise GrepMeshJobError(state, payload)
    raise GrepMeshContractError("unknown terminal search state {!r}".format(state))


def search(
    query: str,
    hosts: List[str],
    wait_ms: int = DEFAULT_WAIT_MS,
    max_matches: Optional[int] = None,
    mode: Optional[str] = None,
    path_globs: Optional[List[str]] = None,
    roots: Optional[List[str]] = None,
    verbose: Optional[bool] = None,
    context_lines: Optional[int] = None,
    base_url: str = DEFAULT_BASE_URL,
    poll_deadline_s: float = DEFAULT_POLL_DEADLINE_S,
    poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
    poll_interval_max_s: float = MAX_POLL_INTERVAL_S,
    timeout: Optional[float] = None,
    sleep=time.sleep,
) -> Dict[str, Any]:
    """Run a bounded mesh search and return its final payload.

    Hosts must be an explicit list of known-healthy host ids (the current
    MVP mesh is ``["server-100", "server-88"]``); wildcard ``"*"`` is not
    part of this client contract. If the first answer is still running,
    ``search_status`` is polled (default deadline ~90s, interval starting
    ~500ms and growing to ~2s) until a terminal state. ``partial``,
    ``truncated`` and ``host_status`` are carried through untouched.
    """
    args: Dict[str, Any] = {"query": query, "hosts": list(hosts), "wait_ms": int(wait_ms)}
    if max_matches is not None:
        args["max_matches"] = int(max_matches)
    if mode is not None:
        args["mode"] = mode
    if path_globs is not None:
        args["path_globs"] = list(path_globs)
    if roots is not None:
        args["roots"] = list(roots)
    if verbose is not None:
        args["verbose"] = bool(verbose)
    if context_lines is not None:
        args["context_lines"] = int(context_lines)
    if timeout is None:
        timeout = max(wait_ms / 1000.0, DEFAULT_HTTP_TIMEOUT_S)
    payload = call(base_url, "search", args, timeout=timeout)
    payload = _await_terminal(
        payload,
        base_url=base_url,
        timeout=timeout,
        poll_deadline_s=poll_deadline_s,
        poll_interval_s=poll_interval_s,
        poll_interval_max_s=poll_interval_max_s,
        sleep=sleep,
    )
    return validate_search_payload(payload)


def validate_search_payload(payload: Any) -> Dict[str, Any]:
    """Reject payloads that violate the search contract.

    A search answer must carry the ``partial`` flag and a non-empty
    ``host_status`` array; otherwise the caller could mistake a broken
    response for a complete one.
    """
    if not isinstance(payload, dict):
        raise GrepMeshContractError("search payload is not a JSON object")
    if "partial" not in payload:
        raise GrepMeshContractError("search payload missing required 'partial' flag")
    host_status = payload.get("host_status")
    if not isinstance(host_status, list) or not host_status:
        raise GrepMeshContractError("search payload missing non-empty 'host_status'")
    for entry in host_status:
        if not isinstance(entry, dict) or "host_id" not in entry or "ok" not in entry:
            raise GrepMeshContractError(
                "host_status entries must carry at least 'host_id' and 'ok'"
            )
    return payload


def read_text(
    host: str,
    path: str,
    start_line: Optional[int] = None,
    end_line: Optional[int] = None,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = DEFAULT_HTTP_TIMEOUT_S,
) -> Dict[str, Any]:
    """Read one exact file from one host via the ``read_text`` tool."""
    args: Dict[str, Any] = {"host": host, "path": path}
    if start_line is not None:
        args["start_line"] = int(start_line)
    if end_line is not None:
        args["end_line"] = int(end_line)
    return call(base_url, "read_text", args, timeout=timeout)


def _parse_hosts(raw: str) -> List[str]:
    hosts = [part.strip() for part in raw.split(",") if part.strip()]
    if not hosts:
        raise SystemExit("error: --hosts must name at least one host id")
    return hosts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="grepmesh_client.py",
        description="GrepMesh MCP client: search (with async search_status polling), read, tools.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="MCP endpoint (default: %(default)s)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    search_parser = subparsers.add_parser("search", help="bounded mesh search with async polling")
    search_parser.add_argument("query")
    search_parser.add_argument(
        "--hosts",
        default=",".join(MVP_HOSTS),
        help="comma-separated explicit host ids; MVP mesh default: %(default)s",
    )
    search_parser.add_argument("--wait-ms", type=int, default=DEFAULT_WAIT_MS)
    search_parser.add_argument("--max-matches", type=int, default=None)
    search_parser.add_argument(
        "--mode", choices=["literal", "regex", "case_insensitive_literal"], default=None
    )
    search_parser.add_argument("--path-globs", default=None, help="comma-separated globs")
    search_parser.add_argument("--roots", default=None, help="comma-separated roots")
    search_parser.add_argument("--context-lines", type=int, default=None)
    search_parser.add_argument("--verbose", action="store_true")
    search_parser.add_argument(
        "--poll-deadline-s", type=float, default=DEFAULT_POLL_DEADLINE_S
    )

    read_parser = subparsers.add_parser("read", help="read_text for one exact host/path")
    read_parser.add_argument("host")
    read_parser.add_argument("path")
    read_parser.add_argument("--start-line", type=int, default=None)
    read_parser.add_argument("--end-line", type=int, default=None)

    subparsers.add_parser("tools", help="list MCP tools via tools/list")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "search":
            payload = search(
                args.query,
                _parse_hosts(args.hosts),
                wait_ms=args.wait_ms,
                max_matches=args.max_matches,
                mode=args.mode,
                path_globs=_parse_hosts(args.path_globs) if args.path_globs else None,
                roots=_parse_hosts(args.roots) if args.roots else None,
                verbose=True if args.verbose else None,
                context_lines=args.context_lines,
                base_url=args.base_url,
                poll_deadline_s=args.poll_deadline_s,
            )
        elif args.command == "read":
            payload = read_text(
                args.host,
                args.path,
                start_line=args.start_line,
                end_line=args.end_line,
                base_url=args.base_url,
            )
        else:
            payload = list_tools(args.base_url)
    except GrepMeshError as err:
        failure: Dict[str, Any] = {"error": str(err), "kind": type(err).__name__}
        if isinstance(err, GrepMeshHTTPError):
            failure["status"] = err.status
        if isinstance(err, GrepMeshJobError):
            failure["state"] = err.state
            failure["payload"] = err.payload
        print(json.dumps(failure, indent=2))
        return 1
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
