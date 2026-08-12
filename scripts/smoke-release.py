#!/usr/bin/env python3
"""Extract a release bundle and prove its MCP initialize and local search path."""

from __future__ import annotations

import argparse
import json
import subprocess
import tarfile
import tempfile
import time
import urllib.request
import zipfile
from pathlib import Path


def rpc(url: str, method: str, params: dict) -> dict:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    request = urllib.request.Request(url, body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True)
    parser.add_argument("--archive", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="grepmesh-smoke-") as temp:
        root = Path(temp)
        if args.archive.suffix == ".zip":
            with zipfile.ZipFile(args.archive) as archive:
                archive.extractall(root)
            binary = root / "grepmesh-mcp.exe"
        else:
            with tarfile.open(args.archive) as archive:
                archive.extractall(root)
            binary = root / "grepmesh-mcp"
            binary.chmod(0o755)
            (root / "rg").chmod(0o755)
        canary = root / "canary"
        canary.mkdir()
        (canary / "proof.txt").write_text("grepmesh release canary\n", encoding="utf-8")
        config = json.loads((root / "config.example.json").read_text(encoding="utf-8"))
        config.update({"host_id": args.target, "bind": "127.0.0.1:19419", "root": str(canary), "roots": {"canary": [str(canary)]}, "peers": []})
        config_path = root / "smoke-config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        process = subprocess.Popen([str(binary), "--config", str(config_path)])
        try:
            url = "http://127.0.0.1:19419/mcp"
            for _ in range(50):
                try:
                    initialized = rpc(url, "initialize", {"protocolVersion": "2025-06-18"})
                    break
                except Exception:
                    time.sleep(0.1)
            else:
                raise SystemExit("packaged GrepMesh did not start")
            assert initialized["result"]["serverInfo"]["name"] == "grepmesh"
            searched = rpc(url, "tools/call", {"name": "search_text", "arguments": {"query": "release canary", "hosts": "local"}})
            payload = json.loads(searched["result"]["content"][0]["text"])
            assert payload["results"], payload
        finally:
            process.terminate()
            process.wait(timeout=10)
    print(f"{args.target} packaged MCP/search smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
