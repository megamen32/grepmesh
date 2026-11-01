#!/usr/bin/env python3
"""Fail-closed provisional rolling-source publisher tests, no external cloud."""
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import publish_native100_rolling_private as target
from cloud_index import MemoryStore, json_bytes, digest, Reader, MIB


class PrivateRollingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(
            dir="/run/user/1000", prefix="grepmesh-native100-test-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.store = MemoryStore()
        self.originals = [
            [1, "/private/one.txt", "hello private original"],
            [2, "/private/deleted-file.txt", "historical deleted text"],
            [4, "/etc/locked-key.txt", "do not expose without acl"],
            [9, "/private/oversize.txt", "X" * (MIB + 1)],
        ]
        payload = b"".join(json_bytes(r)+b"\n" for r in self.originals)
        compressed = gzip.compress(payload, compresslevel=1)
        key = target.PREFIX + "/parts/test.jsonl.gz"
        self.store.put(key, compressed)
        self.store.put(target.PREFIX + "/progress.json", json_bytes({
            "finished": True,
            "safe_for_deletion": False,
            "consistent_snapshot": False,
            "rows": 4,
            "last_rowid": 9,
            "chunks": [{"key": key, "rows": 4, "last_rowid": 9,
                        "compressed_bytes": len(compressed),
                        "sha256": digest(compressed)}],
        }))
        self.audit = self.root / "audit.json"
        self.receipt = self.root / "receipt.json"
        self.audit.write_text(json.dumps({
            "snapshot": target.SNAPSHOT,
            "historical_documents": 4,
            "cloud_parts_fully_replayed": 1,
            "duplicate_paths": 0,
            "malformed_paths": 0,
            "oversize_documents": 1,
            "total_text_body_bytes": sum(len(r[2].encode()) for r in self.originals),
            "consistent_snapshot": False, "safe_for_deletion": False
        }))
        self.patches = [
            mock.patch.object(target, "ROOT", self.root),
            mock.patch.object(target, "AUDIT", self.audit),
            mock.patch.object(target, "RECEIPT", self.receipt),
            mock.patch.object(target, "PROGRESS", self.root/"progress.json"),
            mock.patch.object(target, "CloudStore", lambda: self.store),
            mock.patch.object(target, "ARCHIVED", 4),
            mock.patch.object(target, "PARTS", 1),
            mock.patch.object(target, "PARTITIONS", 4),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_private_index_and_verified_roots(self):
        target.run()
        summary = json.loads(self.receipt.read_text())
        self.assertEqual(summary["historical_source_documents"], 4)
        self.assertEqual(summary["indexed_documents"], 3)
        self.assertEqual(summary["oversize_preserved_in_original_archive_only"], 1)
        self.assertFalse(summary["safe_for_deletion"])
        self.assertFalse(summary["production_route_switched"])
        self.assertEqual(len(summary["partitions"]), 4)
        for original in self.originals[:3]:
            path=original[1]
            b=hashlib.sha256(path.encode()).digest()[0]%4
            scope=summary["partitions"][b]["scope"]
            reader=Reader(self.store, self.root, scope, 16*MIB, "server-100", "/")
            reader.refresh()
            got=reader.read_text(path, authorized_roots=[str(Path(path).parent)])
            self.assertEqual(got["body_sha256"], digest(original[2].encode()))
            with self.assertRaises(PermissionError):
                reader.read_text(path, authorized_roots=["/denied"])

    def test_no_audit_no_write(self):
        self.audit.unlink()
        with self.assertRaisesRegex(RuntimeError,"audit has not completed"):
            target.run()
        self.assertFalse(self.receipt.exists())

    def test_archive_corruption_refuses_latest_pointer(self):
        entry=json.loads(self.store.get(target.PREFIX+"/progress.json"))
        entry["chunks"][0]["sha256"]="0"*64
        self.store.put(target.PREFIX+"/progress.json",json_bytes(entry))
        with self.assertRaisesRegex(RuntimeError,"Source archive part"):
            target.run()
        self.assertFalse(any(x.startswith("scopes/") for x in self.store.objects))
        self.assertFalse(self.receipt.exists())

    def test_untrusted_audit_refuses_everything(self):
        d=json.loads(self.audit.read_text())
        d["duplicate_paths"]=1
        self.audit.write_text(json.dumps(d))
        with self.assertRaisesRegex(RuntimeError,"audit has not passed"):
            target.run()
        self.assertFalse(self.receipt.exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
