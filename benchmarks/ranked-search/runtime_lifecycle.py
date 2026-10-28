#!/usr/bin/env python3
"""Isolated, stdlib-only source-manifest lifecycle proof for an immutable binary.

Starts one loopback-only GrepMesh process with a disposable root and SQLite DB.
The JSON receipt contains state, locators, row counts and phase outcomes only;
it never records indexed document bodies or the fake canary text.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import time
import secrets
import string
import urllib.error
import urllib.request
from pathlib import Path


class MCP:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        self.n = 0
        self.session = None
        self.ready = False

    def call(self, method: str, params=None, notification=False):
        self.n += 1
        body = {"jsonrpc": "2.0", "method": method, "params": params or {}}
        if not notification:
            body["id"] = self.n
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        if self.session:
            headers["Mcp-Session-Id"] = self.session
        if self.ready:
            headers["MCP-Protocol-Version"] = "2025-03-26"
        req = urllib.request.Request(self.endpoint, data=json.dumps(body).encode(), headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=5) as response:
            raw = response.read().decode("utf-8", "replace")
            self.session = response.headers.get("Mcp-Session-Id", self.session)
            if "text/event-stream" in response.headers.get("Content-Type", ""):
                raw = "\n".join(line[5:].strip() for line in raw.splitlines() if line.startswith("data:"))
        if notification:
            return None
        result = json.loads(raw)
        if "error" in result:
            raise RuntimeError("MCP RPC error: " + str(result["error"])[:300])
        return result.get("result", result)

    def initialize(self):
        self.call("initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
                                  "clientInfo": {"name": "ranked-search-lifecycle", "version": "1"}})
        self.call("notifications/initialized", notification=True)
        self.ready = True

    def tool(self, name, args):
        result = self.call("tools/call", {"name": name, "arguments": args})
        if result.get("isError"):
            raise RuntimeError("MCP tool error: " + str(result.get("content", ""))[:300])
        for item in result.get("content", []):
            if item.get("type") == "text":
                try:
                    return json.loads(item["text"])
                except (TypeError, ValueError):
                    pass
        return result


def _health(endpoint: str):
    url = endpoint.rsplit("/mcp", 1)[0] + "/health"
    with urllib.request.urlopen(url, timeout=1) as response:
        return response.status == 200


def wait_for(predicate, label, seconds=60):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (OSError, urllib.error.URLError, RuntimeError, ValueError):
            pass
        time.sleep(0.25)
    raise TimeoutError(f"{label} did not converge within {seconds}s")


def manifest_write(path: Path, files):
    path.write_text(json.dumps({"version": 1, "files": [str(x) for x in files]}, indent=2) + "\n", encoding="utf-8")


def mcp_status(client):
    return client.tool("search_status", {"hosts": ["local"]}).get("local", {})


def row_count(db: Path, path: Path):
    if not db.exists():
        return 0
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=2) as conn:
        try:
            return conn.execute("SELECT count(*) FROM grepmesh_documents WHERE path=?", (str(path),)).fetchone()[0]
        except sqlite3.Error:
            return 0


def search_paths(client, tool, query):
    args = {"query": query, "hosts": ["local"], "limit": 10, "context_lines": 0} if tool == "ranked_search" else {
        "query": query, "hosts": ["local"], "mode": "literal", "max_matches": 10,
        "context_lines": 0, "wait_ms": 1000,
    }
    result = client.tool(tool, args)
    hits = result.get("results", []) if isinstance(result, dict) else []
    paths = []
    evidence = []
    for hit in hits:
        path = hit.get("path") or hit.get("ax_path")
        if path:
            paths.append(str(path))
            evidence.append({k: hit[k] for k in ("path", "line", "freshness", "match_quality") if k in hit})
    return {"matched": bool(paths), "paths": paths, "evidence": evidence,
            "partial": result.get("partial"), "truncated": result.get("truncated")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", required=True, type=Path, help="immutable grepmesh-mcp executable")
    ap.add_argument("--out", type=Path, default=Path("runtime-lifecycle.json"))
    ap.add_argument("--timeout", type=int, default=60, choices=range(1, 61))
    args = ap.parse_args()
    binary = args.binary.resolve(strict=True)
    if not os.access(binary, os.X_OK):
        ap.error("--binary is not executable")
    receipt = {"binary": str(binary), "binary_sha256": None, "phases": [], "cleanup": "pending"}
    import hashlib
    receipt["binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
    temp = Path(tempfile.mkdtemp(prefix="grepmesh-lifecycle-"))
    proc = None
    log = None
    failure = None
    try:
        root = temp / "root"
        root.mkdir()
        a = root / "A.md"
        b = root / "B.md"
        created = root / "admitted-create.md"
        secret = root / "secret.json"
        outside_dir = temp / "outside"
        outside_dir.mkdir()
        outside = outside_dir / "outside.md"
        a.write_text("lifecycle marker A version one\n", encoding="utf-8")
        b.write_text("lifecycle marker B existing original\n", encoding="utf-8")
        created.write_text("lifecycle admitted create marker one\n", encoding="utf-8")
        manifest = temp / "source-manifest.json"
        manifest_write(manifest, [a, b, created])
        db = temp / "index.sqlite3"
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        endpoint = f"http://127.0.0.1:{port}/mcp"
        config = {
            "host_id": "grepmesh-lifecycle-qa", "bind": f"127.0.0.1:{port}", "root": str(root),
            "roots": {"qa": [str(root)]}, "peers": [], "index_path": str(db),
            "gptadmin_topology_url": None, "gptadmin_token_env": None, "peer_auth_token_env": None,
            "remote_index_source": None, "remote_index_store": None,
            "ocr": {"enabled": False}, "stt": {"enabled": False},
            "limits": {"full_rebuild_min_interval_ms": 0,
                       "index_activity": {"source_manifest": str(manifest)}},
            "exclude_globs": ["**/.git/**", "**/.agents/**"],
        }
        config_path = temp / "config.json"
        config_path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
        log = open(temp / "process.log", "wb")

        def phase(name, ok, **evidence):
            row = {"name": name, "ok": bool(ok), **evidence}
            receipt["phases"].append(row)
            if not row["ok"]:
                raise RuntimeError("phase failed: " + name)

        def start():
            nonlocal proc
            proc = subprocess.Popen([str(binary), "--config", str(config_path)], cwd=temp,
                                    stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
            client = MCP(endpoint)
            wait_for(lambda: proc.poll() is None, "isolated process start", 5)
            # Initialize once; then poll status with the same MCP session.
            wait_for(lambda: _health(endpoint), "loopback health", 10)
            client.initialize()
            return client, mcp_status(client)

        def stop():
            nonlocal proc
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill(); proc.wait(timeout=3)
            proc = None

        client, state = start()
        state = wait_for(lambda: (s if (s := mcp_status(client)).get("index_state") == "Ready" else None), "initial index Ready", args.timeout)
        receipt["initial"] = {"state": state.get("index_state"), "indexed_files": state.get("indexed_files")}
        counts = {p.name: row_count(db, p) for p in (a, b, created)}
        phase("initial_manifest_three_files", counts == {"A.md": 1, "B.md": 1, "admitted-create.md": 1}, row_counts=counts)

        created.unlink()
        wait_for(lambda: row_count(db, created) == 0, "admitted delete pruning", args.timeout)
        created.write_text("lifecycle admitted create marker two\n", encoding="utf-8")
        wait_for(lambda: row_count(db, created) == 1, "admitted create indexing", args.timeout)
        phase("admitted_delete_recreate", row_count(db, created) == 1,
              deleted_row_pruned=True, recreated_row_count=row_count(db, created))

        a.write_text("lifecycle marker A version two\n", encoding="utf-8")
        def edited():
            hit = search_paths(client, "ranked_search", "lifecycle marker A version two")
            verified = any(x.endswith("/A.md") for x in hit["paths"]) and any(
                h.get("freshness") == "source_verified" and str(h.get("path", "")).endswith("/A.md")
                for h in hit["evidence"]
            )
            return hit if verified else None
        edit_result = wait_for(edited, "edited marker source verification", args.timeout)
        old = search_paths(client, "search", "lifecycle marker A version one")
        a_evidence = [h for h in edit_result["evidence"] if str(h.get("path", "")).endswith("/A.md")]
        phase("edit_source_verified", not old["matched"] and any(h.get("freshness") == "source_verified" for h in a_evidence),
              old_marker_match=old["matched"], new_marker_paths=[p for p in edit_result["paths"] if p.endswith("/A.md")],
              freshness=[h.get("freshness") for h in a_evidence])

        os.replace(a, b)
        def renamed():
            return row_count(db, a) == 0 and row_count(db, b) == 1 and search_paths(client, "search", "lifecycle marker A version two")["matched"]
        wait_for(renamed, "rename old-row removal and new-path indexing", args.timeout)
        phase("rename_over_admitted_B", row_count(db, a) == 0 and row_count(db, b) == 1,
              old_A_rows=row_count(db, a), new_B_rows=row_count(db, b))
        b.unlink()
        wait_for(lambda: row_count(db, b) == 0, "delete B row pruning", args.timeout)
        phase("delete_B_row", row_count(db, b) == 0, B_rows=row_count(db, b))

        unadmitted_marker = "fakecanary" + "".join(secrets.choice(string.ascii_lowercase) for _ in range(32))
        secret.write_text(unadmitted_marker + "\n", encoding="utf-8")
        time.sleep(1.0)
        denied_row = row_count(db, secret)
        denied_search = search_paths(client, "ranked_search", unadmitted_marker)
        phase("unadmitted_file_denied", denied_row == 0 and not denied_search["matched"],
              row_count=denied_row, ranked_match=denied_search["matched"], result_paths=denied_search["paths"])

        outside_marker = "outsidefakecanary" + "".join(secrets.choice(string.ascii_lowercase) for _ in range(32))
        outside.write_text(outside_marker + "\n", encoding="utf-8")
        a.symlink_to(outside)
        symlink_search = search_paths(client, "ranked_search", outside_marker)
        symlink_rows = row_count(db, a)
        phase("allowed_path_symlink_denied", symlink_rows == 0 and not symlink_search["matched"],
              row_count=symlink_rows, ranked_match=symlink_search["matched"], result_paths=symlink_search["paths"])
        a.unlink()

        # Refresh to only the one surviving regular file and restart: this
        # explicitly revokes A/B/secret and checks persisted-row pruning.
        manifest_write(manifest, [created])
        stop()
        client, state = start()
        wait_for(lambda: (s if (s := mcp_status(client)).get("index_state") == "Ready" else None), "post-revocation Ready", args.timeout)
        counts = {str(p.name): row_count(db, p) for p in (a, b, created, secret)}
        phase("restart_manifest_revocation_prunes", counts == {"A.md": 0, "B.md": 0, "admitted-create.md": 1, "secret.json": 0}, row_counts=counts)

        # A malformed live manifest is picked up only on restart by contract.
        manifest.write_text("{invalid\n", encoding="utf-8")
        stop()
        client, state = start()
        degraded = wait_for(lambda: (mcp_status(client) if mcp_status(client).get("index_state") == "Degraded" else None), "invalid-manifest Degraded", args.timeout)
        partial = search_paths(client, "ranked_search", "lifecycle admitted create marker two")
        phase("invalid_manifest_degraded_partial", degraded.get("index_state") == "Degraded" and partial.get("partial") is True,
              state=degraded.get("index_state"), partial=partial.get("partial"), indexed_files=degraded.get("indexed_files"))
        receipt["final_state"] = degraded.get("index_state")
        receipt["all_phases_pass"] = all(x.get("ok") for x in receipt["phases"])
        log.close()
    except Exception as exc:
        failure = {"type": type(exc).__name__, "message": str(exc).splitlines()[0][:160]}
        receipt["failure"] = failure
        receipt["all_phases_pass"] = False
    finally:
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill(); proc.wait(timeout=3)
        try:
            if log:
                log.close()
        except Exception:
            pass
        receipt["cleanup"] = "isolated process stopped; temporary directory removed"
        shutil.rmtree(temp, ignore_errors=True)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in receipt.items() if k not in ("phases",)}, ensure_ascii=False))
    if not receipt.get("all_phases_pass"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
