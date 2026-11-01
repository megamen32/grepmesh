#!/usr/bin/env python3
"""Independently validate a live, PROVISIONAL rolling-text cloud checkpoint.

This is neither a consistent SQLite snapshot nor a source/ACL/search-parity
certificate. In particular, NEVER authorize deletion of the native index
from this report, even if every listed chunk is independently verified.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import re

from cloud_index import CloudStore, digest, json_bytes
from export_native_rolling import MAX_RECORD

# Never accept a caller-selected SQLite path or arbitrary cloud prefix.
SOURCES = {
    "server-100": "/home/roomhacker/.cache/grepmesh/server-100-65415a79c0e91f4b.sqlite3",
    "server-44": "/home/roomhacker/.cache/grepmesh/server-44-65415a79c0e91f4b.sqlite3",
}
MAX_CHECKPOINT = 16 * 1024 * 1024
MAX_PACKED = MAX_RECORD + 1024 * 1024


def fail(message: str):
    raise ValueError(message)


def verify(store, snapshot: str, sample_indices=None, host='server-100'):
    if host not in SOURCES:
        fail('Untrusted source host')
    if not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}", snapshot):
        fail("Invalid rolling archive ID")
    prefix = f"archives/{host}/rolling/{snapshot}"
    state = json.loads(store.get(prefix + "/progress.json", MAX_CHECKPOINT))
    if (state.get("format") != "grepmesh-rolling-text-v1"
        or state.get("snapshot") != snapshot
        or state.get("source") != SOURCES[host]):
        fail("Unexpected checkpoint identity/source")
    if state.get("consistent_snapshot") is not False or state.get("safe_for_deletion") is not False:
        fail("Provisional checkpoint must categorically deny deletion")
    upper = state.get("upper_rowid")
    if type(upper) is not int or upper < 1:
        fail("Missing or invalid upper rowid")
    lower = state.get("lower_rowid", 0)
    parent = state.get("base_snapshot")
    if type(lower) is not int or not 0 <= lower < upper:
        fail("Invalid catch-up lower row ID")
    if lower:
        if (not isinstance(parent, str) or parent == snapshot
            or not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}",parent)
            or state.get("capture_mode") != "provisional_high_rowid_only"):
            fail("Missing or invalid immutable catch-up parent")
        base = json.loads(store.get(f"archives/{host}/rolling/{parent}/progress.json", MAX_CHECKPOINT))
        if (base.get("snapshot")!=parent or base.get("format")!=state["format"]
            or base.get("source")!=state["source"]
            or base.get("upper_rowid")!=lower
            or base.get("finished") is not True
            or base.get("consistent_snapshot") is not False
            or base.get("safe_for_deletion") is not False):
            fail("Catch-up base checkpoint cannot be verified")
    elif parent is not None:
        fail("Unexpected base for full provisional rolling capture")
    parts = state.get("chunks")
    if not isinstance(parts, list) or not parts:
        fail("Missing rolling chunks")
    previous = lower
    total_rows = total_raw = total_packed = 0
    for i, p in enumerate(parts):
        if not isinstance(p, dict):
            fail("Invalid chunk descriptor")
        sha = p.get("sha256")
        if not isinstance(sha, str) or not re.fullmatch("[0-9a-f]{64}", sha):
            fail("Invalid chunk digest")
        if p.get("key") != f"{prefix}/parts/{i:06d}-{sha}.jsonl.gz":
            fail("Wrong immutable chunk key")
        first, last = p.get("first_rowid"), p.get("last_rowid")
        rows, raw, packed = p.get("rows"), p.get("raw_bytes"), p.get("compressed_bytes")
        if (any(type(x) is not int for x in (first, last, rows, raw, packed))
            or first <= previous or last < first or last > upper
            or not 1 <= rows <= last - first + 1
            or not 1 <= raw <= MAX_PACKED
            or not 1 <= packed <= MAX_PACKED):
            fail("Invalid, overlapping, out-of-bounds or unbounded chunk")
        previous = last
        total_rows += rows
        total_raw += raw
        total_packed += packed
    if (state.get("last_rowid") != previous
        or state.get("rows") != total_rows
        or state.get("raw_bytes") != total_raw):
        fail("Checkpoint counts/cursor inconsistent with its parts")
    chosen = (list(range(len(parts))) if sample_indices is None else list(dict.fromkeys(sample_indices)))
    if not chosen or any(type(i) is not int or not 0 <= i < len(parts) for i in chosen):
        fail("Invalid sample selection")
    checked_rows = checked_packed = 0
    for i in chosen:
        part = parts[i]
        compressed = store.get(part["key"], part["compressed_bytes"])
        if len(compressed) != part["compressed_bytes"] or digest(compressed) != part["sha256"]:
            fail(f"Chunk {i} corrupted in private cloud")
        count = size = 0
        first = last = None
        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:
            while True:
                line = stream.readline(MAX_RECORD + 1024)
                if not line:
                    break
                if not line.endswith(b"\n") or len(line) >= MAX_RECORD + 1024:
                    fail(f"Malformed or excessive record in chunk {i}")
                row = json.loads(line)
                if (not isinstance(row, list) or len(row) != 3
                    or type(row[0]) is not int
                    or not isinstance(row[1], str) or not isinstance(row[2], str)):
                    fail(f"Invalid native FTS row in chunk {i}")
                if last is not None and row[0] <= last:
                    fail(f"Chunk {i} has unordered rows")
                last = row[0]
                if first is None:
                    first = last
                count += 1
                size += len(line)
                if size > part["raw_bytes"]:
                    fail(f"Chunk {i} decompression exceeds declared size")
        if (count != part["rows"] or size != part["raw_bytes"]
            or first != part["first_rowid"] or last != part["last_rowid"]):
            fail(f"Chunk {i} does not match checkpoint accounting")
        checked_rows += count
        checked_packed += len(compressed)
    return {
        "provisional_checkpoint_verified": True,
        "snapshot": snapshot,
        "host": host,
        "checkpoint_rows": total_rows,
        "checkpoint_chunks": len(parts),
        "last_rowid": previous,
        "upper_rowid": upper,
        "lower_rowid": lower,
        "base_snapshot": parent,
        "finished": bool(state.get("finished")),
        "verified_chunks": len(chosen),
        "verified_rows": checked_rows,
        "verified_compressed_bytes": checked_packed,
        "all_chunks_replayed": len(chosen) == len(parts),
        "source_sqlite_parity_verified": False,
        "consistent_snapshot": False,
        "safe_for_deletion": False,
        "ssd_reclaimed_bytes": 0,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", required=True)
    ap.add_argument("--host", choices=sorted(SOURCES), default="server-100")
    ap.add_argument("--sample", type=int, action="append", default=None,
                    help="Check specific chunk index; omit to verify all chunks")
    args = ap.parse_args()
    result = verify(CloudStore(), args.snapshot, args.sample, host=args.host)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
