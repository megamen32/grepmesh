#!/usr/bin/env python3
"""Stage cold >1 MiB native server-100 history from an ALREADY verified rolling archive.

Private cloud only. Never scans source SQLite, mutates routes/ACL, or authorizes
deleting an old index. A source/identity/count mismatch fails BEFORE the
oversize pointer can be published. Durable checkpoint is deliberately
consistent_snapshot=false / safe_for_deletion=false.
"""
from __future__ import annotations
import fcntl
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import time

from cloud_index import CloudStore, MAX_DOCUMENT, MIB, digest, ensure_tmpfs, json_bytes
from oversize_support import (
    MAX_OVERSIZE_DOCUMENT, OversizeIndex, oversize_pointer_key,
    publish_oversize,
)

HOST = "server-100"
SNAPSHOT = "20261031T233108Z-5732b979"
ARCHIVE = "archives/server-100/rolling/" + SNAPSHOT
EXPECTED_ROWS = 3187870
EXPECTED_PARTS = 443
EXPECTED_OVERSIZE = 2696
SOURCE = "/home/roomhacker/.cache/grepmesh/server-100-65415a79c0e91f4b.sqlite3"
AUDIT = Path("/home/roomhacker/.local/state/grepmesh-native100-rolling-coverage-audit.json")
BUILDER = Path("/home/roomhacker/.local/state/grepmesh-native100-private-search-candidate.json")
RECEIPT = Path("/home/roomhacker/.local/state/grepmesh-native100-rolling-oversize-candidate.json")
ROOT = Path("/run/user/1000/grepmesh-native100-oversize-build")
PROGRESS = ROOT / "progress.json"


def persist(path, state):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temp = path.with_name(path.name + ".tmp." + str(os.getpid()))
    with temp.open("x", encoding="utf-8") as handle:
        os.chmod(temp, 0o600)
        json.dump(state, handle, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


def source_docs(store, checkpoint, audit, stats, progress=None):
    """Stream and validate every original row; yield ONLY oversized historical text.

    Validation at generator exhaustion is intentionally inside this generator:
    publish_oversize cannot write its manifest/pointer until these assertions
    have passed, preserving fail-closed publication.
    """
    previous = 0
    for idx, part in enumerate(checkpoint["chunks"], 1):
        length = part["compressed_bytes"]
        packed = store.get(part["key"], length)
        if len(packed) != length or digest(packed) != part["sha256"]:
            raise RuntimeError("Immutable rolling chunk integrity mismatch")
        count = 0
        with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
            for line in stream:
                row = json.loads(line)
                if not isinstance(row, list) or len(row) != 3:
                    raise RuntimeError("Invalid immutable rolling row")
                rowid, path, body = row
                if (type(rowid) is not int or rowid <= previous
                        or not isinstance(path, str) or not path.startswith("/")
                        or "\x00" in path or ".." in Path(path).parts
                        or not isinstance(body, str)):
                    raise RuntimeError("Unsafe historical row identity or body")
                previous = rowid
                count += 1
                stats["rows"] += 1
                size = len(body.encode("utf-8"))
                stats["text_bytes"] += size
                if size > MAX_DOCUMENT:
                    if size > MAX_OVERSIZE_DOCUMENT:
                        raise RuntimeError("Historical body exceeds cold-reader cap")
                    stats["oversize"] += 1
                    stats["oversize_bytes"] += size
                    yield {"path": path, "body": body}
        if count != part["rows"] or previous != part["last_rowid"]:
            raise RuntimeError("Historical rolling part count or last row ID mismatch")
        if progress is not None and (idx % 10 == 0 or idx == len(checkpoint["chunks"])):
            persist(progress, {
                "parts_checked": idx, "rows_checked": stats["rows"],
                "oversize_seen": stats["oversize"],
                "consistent_snapshot": False, "safe_for_deletion": False,
            })
    if (stats["rows"] != EXPECTED_ROWS
            or previous != checkpoint["last_rowid"]
            or stats["oversize"] != EXPECTED_OVERSIZE
            or stats["oversize"] != audit["oversize_documents"]
            or stats["text_bytes"] != audit["total_text_body_bytes"]):
        raise RuntimeError("Original historical archive parity failed")


def main():
    os.umask(0o077)
    if os.environ.get("GREPMESH_CLOUD_GATEWAY") != "local":
        raise RuntimeError("Only pre-existing private Yandex.Disk rclone owner is authorized")
    ensure_tmpfs(ROOT)
    with (ROOT / "publisher.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if RECEIPT.exists():
            raise RuntimeError("Cold candidate already published: inspect receipt, never republish")
        if PROGRESS.exists():
            raise RuntimeError("Previous cold publication incomplete: inspect progress before retry")
        if not BUILDER.exists():
            raise RuntimeError("Parent private FTS candidate is not yet fully published")
        builder = json.loads(BUILDER.read_text())
        if not (builder.get("archive_snapshot") == SNAPSHOT
                and builder.get("historical_source_documents") == EXPECTED_ROWS
                and builder.get("consistent_snapshot") is False
                and builder.get("safe_for_deletion") is False):
            raise RuntimeError("Parent candidate is not the exact original provisional source")
        audit = json.loads(AUDIT.read_text())
        if not (audit.get("snapshot") == SNAPSHOT
                and audit.get("cloud_parts_fully_replayed") == EXPECTED_PARTS
                and audit.get("historical_documents") == EXPECTED_ROWS
                and audit.get("oversize_documents") == EXPECTED_OVERSIZE
                and audit.get("duplicate_paths") == 0
                and audit.get("malformed_paths") == 0
                and audit.get("consistent_snapshot") is False
                and audit.get("safe_for_deletion") is False):
            raise RuntimeError("Original historical source coverage gate not met")
        store = CloudStore()
        checkpoint = json.loads(store.get(ARCHIVE + "/progress.json", 16 * MIB))
        if not (checkpoint.get("snapshot") == SNAPSHOT
                and checkpoint.get("source") == SOURCE
                and checkpoint.get("finished") is True
                and checkpoint.get("rows") == EXPECTED_ROWS
                and len(checkpoint.get("chunks", [])) == EXPECTED_PARTS
                and checkpoint.get("consistent_snapshot") is False
                and checkpoint.get("safe_for_deletion") is False):
            raise RuntimeError("Original immutable source checkpoint changed")
        start = time.monotonic()
        stats = {"rows": 0, "text_bytes": 0, "oversize": 0, "oversize_bytes": 0}
        result = publish_oversize(
            store, HOST, SNAPSHOT,
            source_docs(store, checkpoint, audit, stats, PROGRESS),
        )
        if result["documents"] != EXPECTED_OVERSIZE or result["raw_bytes"] != stats["oversize_bytes"]:
            raise RuntimeError("Cold publication body count changed")
        index = OversizeIndex(store, HOST, SNAPSHOT)
        index.refresh()
        if index.manifest["documents"] != EXPECTED_OVERSIZE:
            raise RuntimeError("Cold cloud index failed re-admission")
        receipt = {
            "event": "private_provisional_server100_oversize_published",
            "snapshot": SNAPSHOT,
            "source_rows_reverified": stats["rows"],
            "historical_oversize_documents": stats["oversize"],
            "historical_oversize_body_bytes": stats["oversize_bytes"],
            "cold_chunks_uploaded": result["chunks"],
            "manifest_sha256": result["manifest_sha256"],
            "pointer_key": oversize_pointer_key(HOST, SNAPSHOT),
            "immutable_archive_consistent_snapshot": False,
            "safe_for_deletion": False,
            "verified_fs_acl_accepted": False,
            "production_route_switched": False,
            "ssd_reclaimed_bytes": 0,
            "elapsed_s": round(time.monotonic() - start, 2),
        }
        persist(RECEIPT, receipt)
        print(json.dumps(receipt, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
