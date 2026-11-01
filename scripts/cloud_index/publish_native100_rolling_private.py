#!/usr/bin/env python3
"""Publish an UNEXPOSED, provisional native server-100 FTS corpus to private cloud.

Source: immutable completed ROLLING archive, not a consistent live snapshot.
All intermediate SQLite scratch is in tmpfs. Never changes native SQLite,
live GrepMesh, GPTAdmin routes/ACLs, or source archive progress. Fail closed.
"""
from __future__ import annotations
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import time

from cloud_index import (CloudStore, Publisher, digest, json_bytes,
                         document, ensure_tmpfs, INDEX_LAYOUT,
                         MAX_DOCUMENT, MAX_MANIFEST, MIB)

SNAPSHOT = "20261031T233108Z-5732b979"
PREFIX = "archives/server-100/rolling/" + SNAPSHOT
ARCHIVED = 3187870
PARTS = 443
PARTITIONS = 16
ROOT = Path("/run/user/1000/grepmesh-native100-private-build")
AUDIT = Path("/home/roomhacker/.local/state/grepmesh-native100-rolling-coverage-audit.json")
RECEIPT = Path("/home/roomhacker/.local/state/grepmesh-native100-private-search-candidate.json")
PROGRESS = Path("/run/user/1000/grepmesh-native100-private-search-build.progress.json")
POLICY = ("PROVISIONAL archived native FTS history; exact original path and body; "
          "source files never read; strict verified filesystem ACL mandatory; "
          "NOT a consistent snapshot or production source")
TARGET_BYTES = 16 * MIB


def safe_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + ".tmp." + str(os.getpid()))
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump(payload, handle, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


class Partition:
    def __init__(self, index, pub):
        self.index = index
        self.publisher = pub
        self.scope = "rolling100-20261031t233108z-5732b979-%02d" % index
        self.paths = []
        self.parts = []
        self.docs = []
        self.pending_bytes = 0
        self.count = 0
        self.logical = hashlib.sha256()

    def add(self, path, body):
        doc = document(path, body, 0, 0)
        self.paths.append((path, 0, 0, doc["sha256"]))
        self.docs.append(doc)
        self.pending_bytes += len(body.encode("utf-8"))
        self.count += 1
        self.logical.update(json_bytes([path, doc["sha256"]]))
        if self.pending_bytes >= TARGET_BYTES or len(self.docs) >= 512:
            self.flush()

    def flush(self):
        if not self.docs:
            return
        self.parts.extend(self.publisher.shard(self.docs))
        if len(self.parts) > 500:
            raise RuntimeError("Shard count overflow; keep unexposed, use more partitions")
        self.docs = []
        self.pending_bytes = 0


def run():
    os.umask(0o077)
    ensure_tmpfs(ROOT)
    if RECEIPT.exists():
        raise RuntimeError("Existing private search receipt must be inspected; no duplicate run")
    if not AUDIT.is_file():
        raise RuntimeError("Full immutable native100 archive audit has not completed")
    audit = json.loads(AUDIT.read_text())
    if not (audit.get("snapshot") == SNAPSHOT
            and audit.get("historical_documents") == ARCHIVED
            and audit.get("cloud_parts_fully_replayed") == PARTS
            and audit.get("duplicate_paths") == 0
            and audit.get("malformed_paths") == 0
            and audit.get("safe_for_deletion") is False
            and audit.get("consistent_snapshot") is False):
        raise RuntimeError("Full independent source audit has not passed")
    store = CloudStore()
    progress = json.loads(store.get(PREFIX + "/progress.json", 16 * MIB))
    if not (progress.get("finished") is True
            and progress.get("safe_for_deletion") is False
            and progress.get("consistent_snapshot") is False
            and progress.get("rows") == ARCHIVED
            and len(progress["chunks"]) == PARTS):
        raise RuntimeError("Provisional cloud source checkpoint was changed")
    started = time.monotonic()
    publisher = Publisher(store, ROOT, shard_text_bytes=TARGET_BYTES)
    partitions = [Partition(i, publisher) for i in range(PARTITIONS)]
    count = 0
    oversize = 0
    raw_body_bytes = 0
    last_rowid = 0
    first_samples = {}
    for idx, part in enumerate(progress["chunks"]):
        packed = store.get(part["key"], part["compressed_bytes"])
        if len(packed) != part["compressed_bytes"] or digest(packed) != part["sha256"]:
            raise RuntimeError("Source archive part failed SHA256")
        part_count = 0
        with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
            for line in stream:
                row = json.loads(line)
                if not isinstance(row, list) or len(row) != 3:
                    raise RuntimeError("Invalid historical source row")
                rowid, path, body = row
                if (type(rowid) is not int or rowid <= last_rowid
                        or not isinstance(path, str) or not path.startswith("/")
                        or "\x00" in path or ".." in Path(path).parts
                        or not isinstance(body, str)):
                    raise RuntimeError("Historical identity/path/body failed validation")
                last_rowid = rowid
                count += 1
                part_count += 1
                encoded = body.encode("utf-8")
                raw_body_bytes += len(encoded)
                if len(encoded) > MAX_DOCUMENT:
                    oversize += 1  # Exact body already preserved in immutable source.
                    continue
                partition_id = hashlib.sha256(path.encode("utf-8")).digest()[0] % PARTITIONS
                bucket = partitions[partition_id]
                bucket.add(path, body)
                if partition_id not in first_samples:
                    first_samples[partition_id] = {
                        "path": path, "body_sha256": digest(encoded)}
        if part_count != part["rows"] or last_rowid != part["last_rowid"]:
            raise RuntimeError("Archive source row accounting changed")
        if (idx + 1) % 10 == 0 or idx + 1 == PARTS:
            safe_json(PROGRESS, {
                "source_parts_read": idx + 1,
                "source_rows_read": count,
                "searchable_documents": sum(p.count for p in partitions),
                "oversize_preserved_in_source": oversize,
                "cloud_immutable_shards_uploaded": sum(len(p.parts) for p in partitions),
                "elapsed_seconds": round(time.monotonic() - started, 1),
                "consistent_snapshot": False,
                "safe_for_deletion": False,
            })
    if (count != ARCHIVED or last_rowid != progress["last_rowid"]
            or oversize != audit["oversize_documents"]
            or raw_body_bytes != audit["total_text_body_bytes"]
            or sum(p.count for p in partitions) + oversize != ARCHIVED):
        raise RuntimeError("Full source coverage parity failed")
    # No latest pointers exist yet. First publish and verify every immutable
    # per-partition path index and manifest, only then publish private pointers.
    candidates = []
    for p in partitions:
        p.flush()
        path_index = publisher.path_index(p.paths)
        manifest = {
            "path_index": path_index,
            "index_layout": INDEX_LAYOUT,
            "format": 1,
            "scope": p.scope,
            "source_host": "server-100",
            "source_root": "/",
            "policy": POLICY + "; bucket %d/%d" % (p.index, PARTITIONS),
            "skipped": {"oversized_historical_body": oversize},
            "documents": p.count,
            "logical_sha256": p.logical.hexdigest(),
            "parts": p.parts,
            "generated_at": time.time(),
            "complete_for_policy": True,
            "storage": "private immutable Yandex.Disk shards / tmpfs-only bounded cache",
            "build_seconds": round(time.monotonic() - started, 3),
        }
        blob = json_bytes(manifest)
        if len(blob) > MAX_MANIFEST:
            raise RuntimeError("Cloud scope manifest too big; no pointers published")
        key = "manifests/" + p.scope + "/" + digest(blob) + ".json"
        store.put(key, blob)
        # Independent remote admission of manifest, before any latest pointers.
        if digest(store.get(key, MAX_MANIFEST)) != digest(blob):
            raise RuntimeError("Manifest round-trip hash mismatch")
        candidates.append({
            "scope": p.scope, "manifest": key, "sha256": digest(blob),
            "documents": p.count, "parts": len(p.parts),
            "path_index_sha256": path_index["sha256"],
        })
    for p, candidate in zip(partitions, candidates):
        pointer = {
            "manifest": candidate["manifest"], "sha256": candidate["sha256"],
            "documents": p.count, "source_host": "server-100",
            "source_root": "/"}
        store.put("scopes/" + p.scope + "/latest.json", json_bytes(pointer))
    summary = {
        "event": "provisional_server100_private_search_candidate_published",
        "archive_snapshot": SNAPSHOT,
        "historical_source_documents": ARCHIVED,
        "indexed_documents": sum(p.count for p in partitions),
        "oversize_preserved_in_original_archive_only": oversize,
        "partitions": candidates,
        "sample_path_sha256": [
            {"bucket": i, "path_hash": digest(sample["path"].encode()),
             "body_sha256": sample["body_sha256"]}
            for i, sample in sorted(first_samples.items())],
        "consistent_snapshot": False, "safe_for_deletion": False,
        "verified_fs_acl_accepted": False,
        "production_route_switched": False,
        "old_sqlite_deleted": False,
        "ssd_reclaimed_bytes": 0,
        "elapsed_seconds": round(time.monotonic() - started, 1),
    }
    safe_json(RECEIPT, summary)
    print(json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    run()
