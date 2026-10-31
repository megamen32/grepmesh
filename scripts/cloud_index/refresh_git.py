#!/usr/bin/env python3
"""Fail-closed, bounded cloud index refresh for explicit Git source scopes.

Run on server-44 (under a systemd CPU/RAM cgroup) with its existing authenticated
SSH gateway to server-100. No local content index, credentials or source files
are written to SSD. Never modifies or deletes native GrepMesh databases.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import pathlib
import selectors
import shlex
import subprocess
import sys
import tempfile
import time

from cloud_index import CloudStore, Publisher, INDEX_LAYOUT, MAX_DOCUMENT
from cloud_index import digest, ensure_tmpfs, json_bytes, safe_scope

P = pathlib.Path
SOURCE_HOST = "server-100"
GATEWAY = "roomhacker@192.168.2.100"
SCANNER = "/home/roomhacker/agents-projects/grepmesh/scripts/cloud_index/cloud_index.py"
ROOT_PREFIX = "/home/roomhacker/agents-projects/"
MAX_STREAM = 64 * 1024 * 1024
MAX_LINE = MAX_DOCUMENT * 7
MAX_FILES = 100000
SCAN_TIMEOUT = 120


def load_scopes(path: P):
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or raw.get("version") != 1:
        raise ValueError("Versioned cloud refresh policy is required")
    items = raw.get("scopes")
    if not isinstance(items, list) or not 1 <= len(items) <= 8:
        raise ValueError("Explicit list of 1..8 Git source scopes required")
    seen = set()
    for entry in items:
        if not isinstance(entry, dict):
            raise ValueError("Invalid source entry")
        scope = safe_scope(entry.get("scope", ""))
        root = entry.get("source_root")
        if (
            scope in seen
            or entry.get("source_host") != SOURCE_HOST
            or not isinstance(root, str)
            or not root.startswith(ROOT_PREFIX)
            or root == ROOT_PREFIX.rstrip("/")
            or ".." in P(root).parts
            or P(root).as_posix() != root
            or len(P(root).parts) < 5
        ):
            raise ValueError("Unauthorized, duplicate or overly broad Git source root")
        seen.add(scope)
    return items


def acquire_source_stream(entry, output, max_bytes=MAX_STREAM, timeout=SCAN_TIMEOUT):
    """Bound bytes and time; remote scanner reads tracked originals, not SQLite."""
    command = [
        "ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
        "-o", "ServerAliveInterval=15", GATEWAY,
        shlex.join([
            "python3", SCANNER, "scan", "--source-root", entry["source_root"],
            "--source-host", entry["source_host"],
        ]),
    ]
    copied = 0
    started = time.monotonic()
    # No persistent stderr spool or credential-bearing diagnostics.
    process = subprocess.Popen(
        command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL
    )
    assert process.stdout is not None
    poller = selectors.DefaultSelector()
    poller.register(process.stdout, selectors.EVENT_READ)
    try:
        while True:
            if time.monotonic() - started > timeout:
                raise TimeoutError("Source scan exceeded the configured deadline")
            ready = poller.select(timeout=0.5)
            if not ready:
                continue
            chunk = os.read(process.stdout.fileno(), 65536)
            if not chunk:
                break
            copied += len(chunk)
            if copied > max_bytes:
                raise ValueError("Source stream exceeds maximum RAM-only bytes")
            output.write(chunk)
        code = process.wait(timeout=5)
        if code:
            raise RuntimeError(
                f"Git source scan failed ({code}); cloud generation preserved"
            )
        output.flush()
        output.seek(0)
        return copied
    except BaseException:
        process.kill()
        with contextlib.suppress(Exception):
            process.wait(timeout=5)
        raise
    finally:
        poller.close()
        process.stdout.close()


def verify_stream(stream, source_root, source_host):
    """Verify all documents and full-stream footer before uploading even one shard."""
    stream.seek(0)
    header_raw = stream.readline(MAX_LINE + 1)
    if not header_raw or len(header_raw) > MAX_LINE:
        raise ValueError("Missing/oversized source header")
    header = json.loads(header_raw)
    if (
        header.get("kind") != "begin"
        or header.get("format") != 1
        or header.get("source_root") != source_root
        or header.get("source_host") != source_host
        or not isinstance(header.get("policy"), str)
    ):
        raise ValueError("Source identity/policy not authorized")
    count = 0
    sha = hashlib.sha256()
    names = set()
    footer = None
    for raw in stream:
        if len(raw) > MAX_LINE:
            raise ValueError("Oversized source record")
        item = json.loads(raw)
        kind = item.get("kind")
        if kind == "end":
            if footer is not None:
                raise ValueError("Duplicate source footer")
            footer = item
            continue
        if kind != "document" or footer is not None:
            raise ValueError("Malformed record or trailing data")
        path = item.get("path")
        body = item.get("body")
        key = item.get("sha256")
        if not isinstance(path, str) or not path.startswith(source_root.rstrip("/") + "/"):
            raise ValueError("Source path outside explicitly allowed root")
        if (
            ".." in P(path).parts
            or not isinstance(body, str)
            or not isinstance(key, str)
            or len(body.encode("utf-8")) > MAX_DOCUMENT
            or digest(body.encode("utf-8")) != key
            or path in names
        ):
            raise ValueError("Invalid, inconsistent or duplicate source document")
        names.add(path)
        sha.update(json_bytes([path, key]))
        count += 1
        if count > MAX_FILES:
            raise ValueError("Source scope document limit exceeded")
    if (
        not footer
        or footer.get("complete_for_policy") is not True
        or footer.get("count") != count
        or footer.get("logical_sha256") != sha.hexdigest()
        or not isinstance(footer.get("skipped"), dict)
    ):
        raise RuntimeError("Incomplete or mismatched source stream")
    stream.seek(0)
    return {
        "documents": count, "logical_sha256": sha.hexdigest(),
        "policy": header["policy"], "skipped": footer["skipped"],
    }


def load_existing(store, entry):
    """Refuse to overwrite a missing/unknown generation or corrupt manifest."""
    prefix = "scopes/" + entry["scope"] + "/latest.json"
    pointer_bytes = store.get(prefix, 65536)
    pointer = json.loads(pointer_bytes)
    content = store.get(pointer["manifest"], 16 * 1024 * 1024)
    if digest(content) != pointer["sha256"]:
        raise RuntimeError("Existing cloud manifest checksum mismatch")
    manifest = json.loads(content)
    if (
        manifest.get("complete_for_policy") is not True
        or manifest.get("scope") != entry["scope"]
        or manifest.get("source_host") != entry["source_host"]
        or manifest.get("source_root") != entry["source_root"]
    ):
        raise ValueError("Existing cloud generation is not the authorized source")
    return manifest


def refresh_one(store, ram_root, entry, stream, shard_kib=4096):
    source = verify_stream(stream, entry["source_root"], entry["source_host"])
    current = load_existing(store, entry)
    if source["documents"] < 1:
        raise RuntimeError("Refusing to replace a nonempty cloud scope with an empty scan")
    if current.get("documents", 0) and source["documents"] * 2 < current["documents"]:
        raise RuntimeError("Source lost over half of its documents; manual review required")
    keys = ("documents", "logical_sha256", "policy", "skipped")
    unchanged = (
        all(current.get(k) == source.get(k) for k in keys)
        and current.get("index_layout") == INDEX_LAYOUT
        and bool(current.get("path_index"))
    )
    if unchanged:
        return {
            "scope": entry["scope"], "action": "unchanged",
            "documents": source["documents"], "cloud_uploads": 0,
        }
    stream.seek(0)
    before = store.puts
    manifest = Publisher(store, ram_root, shard_kib * 1024).publish(
        entry["scope"], (line.decode("utf-8") for line in stream)
    )
    if manifest.get("index_layout") != INDEX_LAYOUT:
        raise RuntimeError("Published index layout is not the verified format")
    return {
        "scope": entry["scope"], "action": "published",
        "documents": manifest["documents"], "shards": len(manifest["parts"]),
        "cloud_uploads": store.puts - before,
        "compressed_shard_bytes": sum(p["packed_bytes"] for p in manifest["parts"]),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=P, required=True)
    parser.add_argument("--root", type=P, default=P("/run/user") / str(os.getuid()) / "grepmesh-cloud-index")
    parser.add_argument("--max-stream-mib", type=int, default=64)
    args = parser.parse_args()
    if not 1 <= args.max_stream_mib <= 64:
        raise ValueError("Max stream bytes must be <=64 MiB")
    if hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:2])
    os.nice(10)
    ensure_tmpfs(args.root)
    items = load_scopes(args.config)
    store = CloudStore()
    outputs = []
    for entry in items:
        # Separate temporary source stream, never retained on SSD.
        with tempfile.TemporaryFile(mode="w+b", dir=args.root) as stream:
            copied = acquire_source_stream(
                entry, stream, max_bytes=args.max_stream_mib * 1024 * 1024
            )
            result = refresh_one(store, args.root, entry, stream)
            result["source_stream_bytes"] = copied
            outputs.append(result)
    print(json.dumps({"ok": True, "results": outputs}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
