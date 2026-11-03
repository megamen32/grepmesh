#!/usr/bin/python3
"""Private GPTAdmin ShellMCP stdio bridge for the historical server-44 RAM reader.

Only the authenticated ShellMCP parent may send params._meta.
No public listener and no client-controlled filesystem ACL.
Unreviewed mixed-host federation is intentionally not exposed.
"""
from __future__ import annotations

import copy
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sys
import urllib.error
import urllib.request

NATIVE_URL = "http://127.0.0.1:9419/mcp"
OFFLOAD_URL = "http://127.0.0.1:19441/mcp"
SECRET_FILE = Path("/home/roomhacker/.config/grepmesh-private-offload/acl-secret.env")
PROFILE_ID = "grepmesh-private-fts-only-20261103"
EXPECTED_TARGET = "shell:roomhacker-server-100"
HOP_ORIGIN = "server-100"
READ_TOOLS = {"search", "read_text"}
MAX_INPUT = 1024 * 1024
MAX_RESPONSE = 4 * 1024 * 1024
DEFAULT_TIMEOUT = 25


def encoded(data):
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf8")


def failure(message):
    return {"isError": True, "content": [{"type": "text", "text": message}]}


def _require_acl(meta):
    if not isinstance(meta, dict):
        raise PermissionError("Private FTS requires verified GPTAdmin delegated metadata")
    acl = meta.get("io.gptadmin/filesystem-acl")
    if not isinstance(acl, dict):
        raise PermissionError("Missing delegated filesystem ACL from ShellMCP")
    if (acl.get("version") != 1 or acl.get("profile_id") != PROFILE_ID
            or type(acl.get("profile_version")) is not int
            or acl["profile_version"] < 1 or acl.get("target") != EXPECTED_TARGET
            or acl.get("tool") != "mcp_call"):
        raise PermissionError("Unsupported delegated ACL identity")
    for key in ("job_id", "roots_digest", "args_sha256"):
        value = acl.get(key)
        if not isinstance(value, str) or not value:
            raise PermissionError("Delegated ACL binding incomplete")
    for key in ("roots_digest", "args_sha256"):
        if not re.fullmatch("[a-fA-F0-9]{64}", acl[key]):
            raise PermissionError("Delegated ACL digest malformed")
    roots = acl.get("read_roots")
    if not isinstance(roots, list) or not roots:
        raise PermissionError("No delegated read roots")
    for root in roots:
        if not isinstance(root, str) or not root.startswith("/") or os.path.normpath(root) != root:
            raise PermissionError("Noncanonical delegated root")
        if root == "/" or root == "/dev" or root.startswith("/dev/") or ".." in Path(root).parts:
            raise PermissionError("Device or unrestricted root denied")
    if acl.get("write_roots"):
        raise PermissionError("Read-only historical bridge refuses write roots")
    return acl


def _secret():
    if os.getuid() != 1000:
        raise PermissionError("Bridge must run as roomhacker")
    st = SECRET_FILE.lstat()
    if st.st_mode & 0o077:
        raise PermissionError("Signer file has unsafe permissions")
    raw = SECRET_FILE.read_text()
    prefix = "GREPMESH_CLOUD_ACL_SECRET="
    if not raw.startswith(prefix) or not raw.endswith("\n"):
        raise RuntimeError("Signer file format invalid")
    secret = raw[len(prefix):].strip()
    if len(secret) < 64 or not re.fullmatch("[0-9a-fA-F]+", secret):
        raise RuntimeError("Signer secret is not strong")
    return secret.encode()


def _rpc(url, method, params, headers=None, timeout=DEFAULT_TIMEOUT):
    request = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    body = encoded(request)
    h = {"Content-Type": "application/json"}
    h.update(headers or {})
    req = urllib.request.Request(url, data=body, headers=h, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as response:
        if response.status != 200:
            raise RuntimeError("Upstream MCP not ready")
        raw = response.read(MAX_RESPONSE + 1)
    if len(raw) > MAX_RESPONSE:
        raise RuntimeError("Upstream response too large")
    result = json.loads(raw)
    if not isinstance(result, dict):
        raise RuntimeError("Malformed upstream MCP response")
    if isinstance(result.get("error"), dict):
        raise RuntimeError("Upstream MCP RPC error")
    if not isinstance(result.get("result"), dict):
        raise RuntimeError("Upstream MCP result missing")
    return result["result"]


def _remote_signed_call(name, args, acl):
    if name not in READ_TOOLS:
        raise PermissionError("Historical bridge only signs searches and reads")
    a = copy.deepcopy(args)
    a.pop("rank", None)
    a.pop("wait_ms", None)
    a.pop("filesystem_acl", None)
    a.pop("_meta", None)
    a["hop_count"] = 1
    a["origin_host"] = HOP_ORIGIN
    a["hosts"] = ["local"]
    if name == "search":
        a["limit"] = max(1, min(int(a.pop("max_matches", a.get("limit", 20))), 200))
        a.pop("verbose", None)
        a.pop("context_lines", None)
        if a.get("mode") not in (None, "literal", "case_insensitive_literal", "regex"):
            raise ValueError("Unsupported search mode")
        if a.get("mode") == "case_insensitive_literal":
            a["mode"] = None
    else:
        if not isinstance(a.get("path"), str) or not a["path"].startswith("/"):
            raise ValueError("Absolute path required")
        a["host"] = "local"
    body = encoded({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": {"name": name, "arguments": a}})
    policy = encoded(acl).decode("utf8")
    signature = hmac.new(_secret(), body+b"\n"+policy.encode("utf8"), hashlib.sha256).hexdigest()
    request = urllib.request.Request(
        OFFLOAD_URL, data=body, method="POST",
        headers={"Content-Type": "application/json",
                 "X-GrepMesh-Filesystem-Acl": policy,
                 "X-GrepMesh-Acl-Signature": signature})
    with urllib.request.urlopen(request, timeout=DEFAULT_TIMEOUT) as response:
        if response.status != 200:
            raise RuntimeError("Private FTS reader unavailable")
        raw = response.read(MAX_RESPONSE + 1)
    if len(raw) > MAX_RESPONSE:
        raise RuntimeError("Private FTS result too large")
    result = json.loads(raw).get("result")
    if not isinstance(result, dict):
        raise RuntimeError("Private FTS response invalid")
    return result


def _host_selection(args):
    target = args.get("hosts")
    if target is None:
        return "native"
    if isinstance(target, str):
        selected = [target]
    elif isinstance(target, list) and all(isinstance(x, str) for x in target):
        selected = target
    else:
        raise ValueError("hosts must be text or array")
    if selected and set(selected) == {"server-44"}:
        return "historical44"
    if "server-44" in selected or "*" in selected:
        return "mixed"  # Not safe until live-source parity/coverage acceptance.
    return "native"


def _native_tool_call(name, args, meta=None):
    # The native Rust reader enforces delegated read_roots only when it
    # receives MCP params._meta.  Dropping this while proxying would bypass
    # the Hub's read scope on local reads (notably the /dev prohibition).
    params = {"name": name, "arguments": args}
    if meta is not None:
        if name not in READ_TOOLS:
            raise PermissionError("Delegated ACL cannot call non-read GrepMesh tools")
        _require_acl(meta)
        params["_meta"] = copy.deepcopy(meta)
    return _rpc(NATIVE_URL, "tools/call", params)


# Both rank modes request the same bounded, ACL-filtered candidate set.
# Ranking reads returned snippets only: no index, filesystem or extra RPC.
SEARCH_CANDIDATE_LIMIT = 200


def _search_plan(args):
    rank = args.get('rank', True)
    if type(rank) is not bool:
        raise ValueError('rank must be boolean')
    query = args.get('query')
    if not isinstance(query, str) or not query or len(query) > 2048:
        raise ValueError('Query must be 1..2048 characters')
    requested = args.get('max_matches', args.get('limit', 20))
    if type(requested) is not int:
        raise ValueError('Search limit must be integer')
    return rank, max(1, min(requested, SEARCH_CANDIDATE_LIMIT))


def _snippet_score(hit, terms):
    if not terms:
        return 0.0
    context = hit.get('context') or []
    body = ('\n'.join(line.get('text', '') for line in context)
            if context else hit.get('text', ''))[:65536].casefold()
    path = Path(hit['path'])
    name = (path.parent.name + ' ' + path.name).casefold()
    body_coverage = sum(term in body for term in terms) / len(terms)
    path_coverage = sum(term in name for term in terms) / len(terms)
    definition = bool(re.search(
        r'(?m)^\s*(?:(?:pub(?:\([^)]*\))?|async|export)\s+)*(?:def|fn|class|function|struct|enum|interface)\s', body))
    heading = any(line.lstrip().startswith('#') and
                  any(term in line for term in terms) for line in body.splitlines())
    return round(min(100.0, 75 * body_coverage + 10 * path_coverage
                     + 10 * heading + 5 * (definition and body_coverage > 0)), 6)


def _rank_authorized_payload(payload, args, rank, requested):
    results = payload.get('results')
    if not isinstance(results, list) or len(results) > SEARCH_CANDIDATE_LIMIT:
        raise RuntimeError('Invalid bounded historical candidate set')
    for hit in results:
        if (not isinstance(hit, dict) or not isinstance(hit.get('path'), str)
                or not isinstance(hit.get('text', ''), str)
                or type(hit.get('line_number', 0)) is not int
                or hit.get('line_number', 0) < 0
                or not isinstance(hit.get('source_host', hit.get('host_id', '')), str)):
            raise RuntimeError('Malformed historical search hit')
        context = hit.get('context') or []
        if (not isinstance(context, list) or any(not isinstance(line, dict)
                or not isinstance(line.get('text', ''), str) for line in context)):
            raise RuntimeError('Malformed historical search context')
    out = copy.deepcopy(payload)
    candidates = out['results']
    # Terms affect ordering ONLY. Regex, Unicode, OCR/STT and deleted hits
    # remain in the candidate set even when their score is zero.
    terms = list(dict.fromkeys(re.findall(r'[^\W_]+', args['query'].casefold())))[:32]
    if rank:
        for hit in candidates:
            hit['score'] = _snippet_score(hit, terms)
        candidates.sort(key=lambda hit: (-hit['score'],
            hit.get('source_host', hit.get('host_id', '')),
            hit['path'], hit.get('line_number', 0)))
    else:
        for hit in candidates:
            hit.pop('score', None)
    count = len(candidates)
    out['results'] = candidates[:requested]
    out['truncated'] = bool(payload.get('truncated')) or count > requested
    out['ranking'] = {'enabled': rank, 'candidate_count': count,
        'candidate_limit': SEARCH_CANDIDATE_LIMIT,
        'scope': 'authorized_returned_snippets', 'before_top_k': True,
        'candidate_scan_complete': not bool(payload.get('partial') or
            payload.get('truncated')) and count < SEARCH_CANDIDATE_LIMIT}
    return out


def call(name, args, meta):
    if not isinstance(args, dict):
        raise ValueError("Arguments must be an object")
    if "filesystem_acl" in args or "_meta" in args:
        raise PermissionError("Caller-supplied metadata/ACL is forbidden")
    if name in ("ranked_search", "search_text"):
        raise ValueError("Removed tool")
    if name not in READ_TOOLS:
        if meta is not None:
            raise PermissionError("Delegated ACL cannot call non-read GrepMesh tools")
        return _native_tool_call(name, args)
    target = _host_selection(args) if name == "search" else (
        "historical44" if args.get("host") == "server-44" else "native")
    if target == "mixed":
        return failure("Full mixed-host RAM FTS is not live-safe; historical parity pending")
    if target == "native":
        return _native_tool_call(name, args, meta)
    acl = _require_acl(meta)
    remote_args = args
    if name == 'search':
        rank, requested = _search_plan(args)
        remote_args = copy.deepcopy(args)
        remote_args['limit'] = SEARCH_CANDIDATE_LIMIT
        remote_args['max_matches'] = SEARCH_CANDIDATE_LIMIT
    result = _remote_signed_call(name, remote_args, acl)
    if result.get("isError"):
        return result
    # Historical snapshot has deliberately unaccepted live deltas.
    # Never report a full-green production search from this private bridge.
    text = result.get("content", [{}])[0].get("text", "")
    try:
        payload = json.loads(text)
        if isinstance(payload, dict):
            if name == 'search':
                payload = _rank_authorized_payload(payload, args, rank, requested)
            payload["partial"] = True
            warnings = payload.get("warnings")
            if isinstance(warnings, list):
                warnings.append("Historical-only snapshot: live updates not yet accepted")
            else:
                payload["warnings"] = ["Historical-only snapshot: live updates not yet accepted"]
            result["content"] = [{"type": "text", "text": json.dumps(payload, ensure_ascii=False)}]
    except (ValueError, TypeError, IndexError):
        raise RuntimeError("Invalid remote historical payload")
    return result


def tool_list():
    tools = _rpc(NATIVE_URL, "tools/list", {}).get("tools", [])
    if not isinstance(tools, list):
        raise RuntimeError("Native GrepMesh catalog unavailable")
    filtered = []
    for tool in tools:
        if tool.get("name") in ("ranked_search", "search_text"):
            continue
        item = copy.deepcopy(tool)
        if item.get("name") == "search":
            item.setdefault("inputSchema", {}).setdefault("properties", {})["rank"] = {
                "type": "boolean", "default": True,
                "description": "Rank relevance by default, rank=false disables it."
            }
            item["description"] = (
                "GrepMesh unified search; explicit hosts=server-44 uses signed historical "
                "snapshot, reported partial until live/delta parity. "
                "Otherwise the existing production names-only backend is unchanged."
            )
            # The existing native MCP schema knows only local/*.
            item["inputSchema"]["properties"]["hosts"] = {
                "anyOf": [{"type": "string"},
                          {"type": "array", "items": {"type": "string"}}]
            }
        filtered.append(item)
    return {"tools": filtered}


def handle(request):
    method = request.get("method")
    ident = request.get("id")
    if method and method.startswith("notifications/"):
        return None
    try:
        if method == "initialize":
            result = {"protocolVersion": "2024-11-05",
                      "capabilities": {"tools": {}},
                      "serverInfo": {"name": "grepmesh-signed-private-bridge",
                                     "version": "2026-11-03"}}
        elif method == "ping":
            result = {}
        elif method == "tools/list":
            result = tool_list()
        elif method == "tools/call":
            params = request.get("params") or {}
            name = params.get("name")
            if not isinstance(name, str):
                raise ValueError("Tool name required")
            result = call(name, params.get("arguments") or {}, params.get("_meta"))
        else:
            raise ValueError("Unsupported MCP method")
        return {"jsonrpc": "2.0", "id": ident, "result": result}
    except Exception as exc:
        return {"jsonrpc": "2.0", "id": ident,
                "result": failure(type(exc).__name__ + ": " + str(exc)[:180])}


def main():
    os.umask(0o077)
    for line in sys.stdin.buffer:
        if len(line) > MAX_INPUT:
            continue
        try:
            req = json.loads(line)
            if not isinstance(req, dict):
                continue
            answer = handle(req)
            if answer is not None and answer.get("id") is not None:
                sys.stdout.write(json.dumps(answer, ensure_ascii=False) + "\n")
                sys.stdout.flush()
        except (ValueError, UnicodeDecodeError):
            continue


if __name__ == "__main__":
    main()
