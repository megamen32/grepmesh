#!/usr/bin/env python3
"""Bounded transport tests with an isolated local HTTP fixture, no fleet calls."""
import contextlib
import http.server
import importlib.util
import io
import json
import pathlib
import threading
import tempfile
import hashlib
import unittest

spec = importlib.util.spec_from_file_location("grepmesh_stdio", pathlib.Path(__file__).with_name("grepmesh_stdio.py"))
bridge_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge_module)


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.seen.append((request, dict(self.headers)))
        if self.server.mode == "redirect":
            self.send_response(302)
            self.send_header("Location", "http://127.0.0.1:1/leak")
            self.end_headers()
            return
        self.send_response(200)
        self.end_headers()
        response = {"jsonrpc": "2.0", "id": request.get("id"), "result": {"ok": True}}
        if request["method"] == "initialize":
            response["result"]["protocolVersion"] = "2026-07-28"
        if self.server.mode == "wrong-id":
            response["id"] = "wrong"
        if self.server.mode == "oversized":
            response["result"]["text"] = "x" * 2048
        self.wfile.write(json.dumps(response).encode())


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.seen = []
        self.server.mode = "normal"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.bridge = bridge_module.Bridge("http://127.0.0.1:{}/mcp".format(self.server.server_port), timeout=2)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_dynamic_headers_init_and_tool_roundtrip(self):
        for message in ({"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {}},
                        {"jsonrpc": "2.0", "id": "a", "method": "tools/call", "params": {"name": "ranked_search", "arguments": {}}}):
            self.assertEqual(self.bridge.handle(json.dumps(message))["id"], message["id"])
        self.assertEqual(self.server.seen[0][1]["Mcp-Method"], "initialize")
        self.assertEqual(self.server.seen[1][1]["Mcp-Name"], "ranked_search")
        self.assertNotIn("Authorization", self.server.seen[0][1])

    def test_notifications_never_emit_a_response(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertIsNone(self.bridge.handle('{"jsonrpc":"2.0","method":"notifications/initialized"}'))
            self.server.mode = "redirect"
            self.assertIsNone(self.bridge.handle('{"jsonrpc":"2.0","method":"notifications/cancelled","params":{}}'))
        self.assertEqual(len(self.server.seen), 2)

    def test_stdio_stdout_has_only_requested_envelopes(self):
        source = io.BytesIO(b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n{"jsonrpc":"2.0","id":2,"method":"tools/list"}\n')
        destination = io.BytesIO()
        bridge_module.serve(self.bridge, source, destination)
        lines = destination.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["id"], 2)

    def test_loopback_only_and_no_endpoint_credentials(self):
        for endpoint in ("http://example.org/mcp", "http://127.0.0.1@evil.test/mcp", "http://127.0.0.1/mcp?token=secret", "file:///tmp/x", "http://[::ffff:192.0.2.1]/mcp"):
            with self.assertRaises(bridge_module.BridgeError):
                bridge_module.endpoint_parts(endpoint)
        self.assertEqual(bridge_module.endpoint_parts("http://localhost:9419/mcp")[1], "127.0.0.1")

    def test_wrong_id_oversized_and_redirect_are_not_retried(self):
        request = '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
        for mode in ("wrong-id", "oversized", "redirect"):
            self.server.mode = mode
            self.bridge.max_response = 1024
            self.assertIn("error", self.bridge.handle(request))
        self.assertEqual(len(self.server.seen), 3)

    def test_secret_never_in_transport_error(self):
        self.bridge.bearer = "sensitive-fixture-value"
        self.server.mode = "redirect"
        response = self.bridge.handle('{"jsonrpc":"2.0","id":1,"method":"tools/list"}')
        self.assertNotIn(self.bridge.bearer, json.dumps(response))
        self.assertEqual(self.server.seen[0][1]["Authorization"], "Bearer sensitive-fixture-value")

    def test_invalid_notification_and_nonfinite_json_do_not_crash_transport(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertIsNone(self.bridge.handle('{"jsonrpc":"2.0","method":"notifications/initialized","params":[]}'))
        self.assertEqual(self.bridge.handle('{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ranked_search","arguments":{"x":NaN}}}')["error"]["code"], -32700)


class SourceGeneratorTests(unittest.TestCase):
    def test_reference_uses_validated_corpus_name_and_denies_collision(self):
        path = pathlib.Path(__file__).parents[1] / "deploy" / "ranked-local-config.py"
        spec = importlib.util.spec_from_file_location("ranked_config_reference", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory) / "reference-httpx-b5addb6"
            root.mkdir()
            entries = []
            commit = "b5addb64f0161ff6bfe94c124ef76f6a1fba5254"
            for relative in ("LICENSE.md", "README.md", "httpx/_transports/default.py", "httpx/_config.py", "docs/advanced/transports.md"):
                file = root / relative
                file.parent.mkdir(parents=True, exist_ok=True)
                data = b"Redistribution and use in source and binary forms; Neither the name" if relative == "LICENSE.md" else b"source fixture\n"
                file.write_bytes(data)
                entries.append({"path": relative, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                                "url": "https://raw.githubusercontent.com/encode/httpx/{}/{}".format(commit, relative)})
            manifest = {"version": 1, "corpus": "httpx-reference", "repository": "https://github.com/encode/httpx",
                        "commit": commit, "license": "BSD-3-Clause", "license_file": "LICENSE.md", "files": entries}
            manifest_path = root / "corpus-manifest.json"
            manifest_path.write_text(json.dumps(manifest))
            roots = {"project": ["/project"]}
            references = []
            files = module.add_reference(roots, [], references, manifest_path)
            self.assertEqual(roots["httpx-reference"], [str(root)])
            self.assertNotIn(root.name, roots)
            self.assertEqual(references[0]["corpus"], "httpx-reference")
            self.assertEqual(len(files), 6)
            collision = {"httpx-reference": ["/project"]}
            with self.assertRaises(ValueError):
                module.add_reference(collision, [], [], manifest_path)
            self.assertEqual(collision, {"httpx-reference": ["/project"]})
            manifest["corpus"] = "unknown-corpus"
            manifest_path.write_text(json.dumps(manifest))
            with self.assertRaises(ValueError):
                module.reference_files(manifest_path)

    def test_positive_eligibility_rejects_private_generated_and_runtime(self):
        path = pathlib.Path(__file__).parents[1] / "deploy" / "ranked-local-config.py"
        spec = importlib.util.spec_from_file_location("ranked_config", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertFalse(module.eligible("scripts/dev/x.py"))
        for source in ("src/main.rs", "docs/intro.md", "Cargo.toml", "LICENSE", "httpx/_config.py"):
            self.assertTrue(module.eligible(source), source)
        for denied in ("support_secretary/x.py", "notify-migration-backups/x.py", "deploy/private-upgrade/x.sh", "service/gitcredentials/store.go", ".env", ".github/workflows/build.yml", "src/config.json", "src/cookie-config.yaml", "src/session.toml", "src/secret.json", "private/main.rs", "runtime/state.json", "build/generated.rs", "sessions/token.json", "node_modules/a/index.js", "static/index.js", "assets/schema.json", "evidence/result.md", "src/key.pem", "../outside.rs", "README.exe"):
            self.assertFalse(module.eligible(denied), denied)


if __name__ == "__main__":
    unittest.main()
