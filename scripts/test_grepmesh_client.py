#!/usr/bin/env python3
"""Stub-server unit tests for the GrepMesh MCP client.

These tests run ``grepmesh_client`` against a local ``http.server`` stub
that mimics the GrepMesh MCP envelope, including the live rule that a
``tools/call`` without ``Mcp-Method``/``Mcp-Name`` headers is rejected
with JSON-RPC error -32020.

Scope honesty: these are narrow local-logic tests only (request shape,
header contract, async polling, honest error surfacing). They are NOT
evidence that the real GrepMesh runtime or mesh works; real end-to-end
proof happens against the live runtime separately.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import unittest
import urllib.request
from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import grepmesh_client as client  # noqa: E402

MVP_HOSTS = ["server-100", "server-88"]


def enforce_live_header_contract(record):
    """Mirror the live endpoint: every request needs Mcp-Method; tools/call also Mcp-Name."""
    body = record["body"] or {}
    method = body.get("method")
    rejected = (
        200,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32020, "message": "Mcp-Method header is required"},
        },
    )
    if method is None:
        return None
    if record["headers"].get("Mcp-Method") != method:
        return rejected
    if method == "tools/call":
        tool = (body.get("params") or {}).get("name")
        if record["headers"].get("Mcp-Name") != tool:
            return rejected
    return None


class _StubHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        try:
            body = json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            body = None
        record = {"path": self.path, "headers": self.headers, "body": body}
        self.server.requests.append(record)
        status, payload = self.server.responder(record)
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, format, *args):
        return


class StubMCPServer:
    """Minimal MCP endpoint stub with one responder callback per test."""

    def __init__(self, responder):
        self.requests = []

        def guarded_responder(record):
            enforced = enforce_live_header_contract(record)
            if enforced is not None:
                return enforced
            return responder(record)

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), _StubHandler)
        self._httpd.requests = self.requests
        self._httpd.responder = guarded_responder
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    @property
    def url(self):
        return "http://127.0.0.1:{}/mcp".format(self._httpd.server_address[1])

    def stop(self):
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)


def tool_call_envelope(text_payload):
    text = json.dumps(text_payload)
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {"content": [{"type": "text", "text": text}], "isError": False},
    }


def search_payload(state=None, **overrides):
    """Build a realistic search envelope; ``state`` adds the job fields."""
    payload = {
        "request_id": "req-stub",
        "origin_host": "server-100",
        "hop_count": 0,
        "host_id": "server-100",
        "partial": False,
        "truncated": False,
        "results": [],
        "host_status": [
            {"host_id": "server-100", "ok": True, "state": "ok", "error": None},
            {"host_id": "server-88", "ok": True, "state": "ok", "error": None},
        ],
    }
    if state is not None:
        payload.update(
            {
                "state": state,
                "job_id": "job-stub-1",
                "cursor": "cursor-0",
                "pending_hosts": ["server-88"],
                "next_poll_after_ms": 30000,
                "message": "Search continues; poll search_status.",
            }
        )
    payload.update(overrides)
    return payload


class ClientContractTests(unittest.TestCase):
    def stub(self, responder):
        server = StubMCPServer(responder)
        self.addCleanup(server.stop)
        return server

    def run_search(self, server, **kwargs):
        kwargs.setdefault("query", "canary-marker")
        kwargs.setdefault("hosts", list(MVP_HOSTS))
        kwargs.setdefault("wait_ms", 100)
        kwargs.setdefault("poll_interval_s", 0.02)
        kwargs.setdefault("poll_interval_max_s", 0.05)
        kwargs.setdefault("base_url", server.url)
        return client.search(**kwargs)

    def test_immediate_search_verbatim_and_header_contract(self):
        payload = search_payload(
            results=[
                {
                    "host_id": "server-88",
                    "path": "/tmp/canary.txt",
                    "line_number": 1,
                    "column": 1,
                    "text": "canary-marker",
                    "context": [],
                }
            ]
        )
        seen = {}

        def responder(record):
            seen["record"] = record
            return 200, tool_call_envelope(payload)

        server = self.stub(responder)
        result = self.run_search(server, max_matches=5)
        self.assertEqual(result, payload)
        self.assertEqual(len(server.requests), 1)
        record = seen["record"]
        body = record["body"]
        self.assertEqual(body["jsonrpc"], "2.0")
        self.assertEqual(body["method"], "tools/call")
        self.assertEqual(body["params"]["name"], "search")
        self.assertEqual(
            body["params"]["arguments"],
            {
                "query": "canary-marker",
                "hosts": MVP_HOSTS,
                "wait_ms": 100,
                "max_matches": 5,
            },
        )
        headers = record["headers"]
        self.assertTrue(headers.get("Content-Type").startswith("application/json"))
        self.assertEqual(headers.get("Accept"), "application/json, text/event-stream")
        self.assertEqual(headers.get("MCP-Protocol-Version"), "2026-07-28")
        self.assertEqual(headers.get("Mcp-Method"), "tools/call")
        self.assertEqual(headers.get("Mcp-Name"), "search")

    def test_search_polls_running_job_until_complete(self):
        terminal = search_payload(
            state="complete",
            results=[
                {
                    "host_id": "server-88",
                    "path": "/tmp/async.txt",
                    "line_number": 3,
                    "column": 1,
                    "text": "canary-marker",
                    "context": [],
                }
            ],
            pending_hosts=[],
        )

        def responder(record):
            body = record["body"]
            if body["params"]["name"] == "search":
                return 200, tool_call_envelope(search_payload(state="running", job_id="job-abc"))
            self.assertEqual(body["params"]["name"], "search_status")
            self.assertEqual(body["params"]["arguments"], {"job_id": "job-abc"})
            return 200, tool_call_envelope(terminal)

        server = self.stub(responder)
        result = self.run_search(server)
        self.assertEqual(result, terminal)
        self.assertEqual(result["state"], "complete")
        self.assertEqual(result["partial"], False)
        self.assertEqual(len(server.requests), 2)
        self.assertEqual(server.requests[1]["body"]["params"]["name"], "search_status")

    def test_running_without_job_id_is_contract_violation(self):
        broken = search_payload(state="running")
        del broken["job_id"]

        def responder(record):
            return 200, tool_call_envelope(broken)

        server = self.stub(responder)
        with self.assertRaises(client.GrepMeshContractError) as ctx:
            self.run_search(server)
        self.assertIn("job_id", str(ctx.exception))

    def test_failed_and_expired_terminal_states_raise_explicitly(self):
        for terminal_state in ("failed", "expired", "lost"):
            terminal = search_payload(
                state=terminal_state,
                partial=True,
                host_status=[
                    {
                        "host_id": "server-88",
                        "ok": False,
                        "state": "failed",
                        "error": "peer timeout",
                    }
                ],
            )

            def responder(record, terminal=terminal):
                if record["body"]["params"]["name"] == "search":
                    return 200, tool_call_envelope(search_payload(state="running"))
                return 200, tool_call_envelope(terminal)

            server = self.stub(responder)
            with self.assertRaises(client.GrepMeshJobError) as ctx:
                self.run_search(server)
            self.assertEqual(ctx.exception.state, terminal_state)
            # The terminal payload is carried on the error, never swallowed.
            self.assertEqual(ctx.exception.payload, terminal)

    def test_partial_result_preserved_verbatim(self):
        host_status = [
            {"host_id": "server-100", "ok": True, "state": "ok", "error": None},
            {
                "host_id": "server-88",
                "ok": False,
                "state": "partial",
                "error": "index building",
            },
        ]
        payload = search_payload(partial=True, truncated=True, host_status=host_status)

        def responder(record):
            return 200, tool_call_envelope(payload)

        server = self.stub(responder)
        result = self.run_search(server)
        self.assertEqual(result, payload)
        self.assertIs(result["partial"], True)
        self.assertIs(result["truncated"], True)
        self.assertEqual(result["host_status"], host_status)

    def test_missing_partial_or_host_status_is_contract_violation(self):
        missing_partial = search_payload()
        del missing_partial["partial"]
        missing_hosts = search_payload()
        del missing_hosts["host_status"]
        for broken in (missing_partial, missing_hosts, search_payload(host_status=[])):

            def responder(record, broken=broken):
                return 200, tool_call_envelope(broken)

            server = self.stub(responder)
            with self.assertRaises(client.GrepMeshContractError):
                self.run_search(server)

    def test_unknown_terminal_state_is_contract_violation(self):
        payload = search_payload(state="weird")

        def responder(record):
            return 200, tool_call_envelope(payload)

        server = self.stub(responder)
        with self.assertRaises(client.GrepMeshContractError) as ctx:
            self.run_search(server)
        self.assertIn("weird", str(ctx.exception))

    def test_poll_deadline_raises_timeout_with_job_id(self):
        running = search_payload(state="running", job_id="job-forever")

        def responder(record):
            return 200, tool_call_envelope(running)

        server = self.stub(responder)
        with self.assertRaises(client.GrepMeshTimeoutError) as ctx:
            self.run_search(server, poll_deadline_s=0.2, poll_interval_s=0.05)
        self.assertIn("job-forever", str(ctx.exception))

    def test_read_text_sends_tool_headers_and_returns_verbatim(self):
        payload = {
            "request_id": "req-stub",
            "origin_host": "server-100",
            "hop_count": 0,
            "host_id": "server-100",
            "target_host_id": "server-88",
            "partial": False,
            "truncated": False,
            "path": "/tmp/canary.txt",
            "start_line": 1,
            "end_line": 1,
            "chunks": [{"start_line": 1, "end_line": 1, "lines": [{"line_number": 1, "text": "canary-marker"}]}],
            "host_status": [{"host_id": "server-88", "ok": True, "state": "ok", "error": None}],
        }

        def responder(record):
            body = record["body"]
            self.assertEqual(body["params"]["name"], "read_text")
            self.assertEqual(
                body["params"]["arguments"],
                {"host": "server-88", "path": "/tmp/canary.txt", "start_line": 2, "end_line": 4},
            )
            return 200, tool_call_envelope(payload)

        server = self.stub(responder)
        result = client.read_text(
            "server-88",
            "/tmp/canary.txt",
            start_line=2,
            end_line=4,
            base_url=server.url,
        )
        self.assertEqual(result, payload)
        self.assertEqual(server.requests[0]["headers"].get("Mcp-Name"), "read_text")

    def test_http_500_raises_error_with_status(self):
        def responder(record):
            return 500, {"message": "boom"}

        server = self.stub(responder)
        with self.assertRaises(client.GrepMeshHTTPError) as ctx:
            self.run_search(server)
        self.assertEqual(ctx.exception.status, 500)

    def test_jsonrpc_error_raises_protocol_error(self):
        def responder(record):
            return (
                200,
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32020, "message": "Mcp-Method header is required"},
                },
            )

        server = self.stub(responder)
        with self.assertRaises(client.GrepMeshProtocolError) as ctx:
            self.run_search(server)
        self.assertIn("-32020", str(ctx.exception))

    def test_tools_call_without_mcp_method_headers_is_rejected(self):
        def responder(record):
            raise AssertionError("enforcement should reject before responder")

        server = self.stub(responder)
        body = json.dumps(
            {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "search", "arguments": {}}}
        ).encode("utf-8")
        request = urllib.request.Request(
            server.url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "MCP-Protocol-Version": "2026-07-28",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            envelope = json.loads(response.read())
        self.assertEqual(envelope["error"]["code"], -32020)

    def test_tools_list_sends_mcp_method_header(self):
        catalog = {"tools": [{"name": "search"}, {"name": "read_text"}, {"name": "search_status"}]}

        def responder(record):
            self.assertEqual(record["body"]["method"], "tools/list")
            return 200, {"jsonrpc": "2.0", "id": 1, "result": catalog}

        server = self.stub(responder)
        result = client.list_tools(server.url)
        self.assertEqual(result, catalog)
        self.assertEqual(server.requests[0]["headers"].get("Mcp-Method"), "tools/list")


class CliTests(unittest.TestCase):
    def stub(self, responder):
        server = StubMCPServer(responder)
        self.addCleanup(server.stop)
        return server

    def test_cli_search_success_prints_json_and_exits_zero(self):
        payload = search_payload(
            results=[{"host_id": "server-100", "path": "/tmp/a.txt", "line_number": 1}]
        )

        def responder(record):
            return 200, tool_call_envelope(payload)

        server = self.stub(responder)
        stdout = StringIO()
        with redirect_stdout(stdout):
            code = client.main(
                [
                    "--base-url",
                    server.url,
                    "search",
                    "canary-marker",
                    "--hosts",
                    "server-100,server-88",
                    "--wait-ms",
                    "100",
                    "--max-matches",
                    "5",
                ]
            )
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), payload)

    def test_cli_contract_violation_exits_non_zero(self):
        broken = search_payload()
        del broken["host_status"]

        def responder(record):
            return 200, tool_call_envelope(broken)

        server = self.stub(responder)
        stdout = StringIO()
        with redirect_stdout(stdout):
            code = client.main(["--base-url", server.url, "search", "canary-marker"])
        self.assertEqual(code, 1)
        failure = json.loads(stdout.getvalue())
        self.assertEqual(failure["kind"], "GrepMeshContractError")

    def test_cli_http_500_exits_non_zero_with_status(self):
        def responder(record):
            return 500, {"message": "boom"}

        server = self.stub(responder)
        stdout = StringIO()
        with redirect_stdout(stdout):
            code = client.main(["--base-url", server.url, "search", "canary-marker"])
        self.assertEqual(code, 1)
        failure = json.loads(stdout.getvalue())
        self.assertEqual(failure["status"], 500)

    def test_cli_read_and_tools(self):
        read_payload = search_payload()
        catalog = {"tools": [{"name": "search"}]}

        def responder(record):
            if record["body"]["method"] == "tools/list":
                return 200, {"jsonrpc": "2.0", "id": 1, "result": catalog}
            return 200, tool_call_envelope(read_payload)

        server = self.stub(responder)
        stdout = StringIO()
        with redirect_stdout(stdout):
            code = client.main(
                ["--base-url", server.url, "read", "server-88", "/tmp/canary.txt"]
            )
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), read_payload)

        stdout = StringIO()
        with redirect_stdout(stdout):
            code = client.main(["--base-url", server.url, "tools"])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), catalog)


if __name__ == "__main__":
    unittest.main()
