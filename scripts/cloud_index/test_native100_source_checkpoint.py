"""Pinned immutable checkpoint recovery; synthetic transport only, no cloud writes."""
import hashlib
import json
import subprocess
import unittest
from unittest.mock import patch

import publish_native100_rolling_private as target
from cloud_index import CloudStore


class SourceCheckpointRecoveryTests(unittest.TestCase):
    body = b"immutable archived source body"
    sha = hashlib.sha256(body).hexdigest()
    key = target.PREFIX + "/parts/000424-" + sha + ".jsonl.gz"

    def store(self):
        with patch.object(target.VerifiedImmutableReuseStore, "_load_verified_inventory",
                          return_value={}):
            return target.VerifiedImmutableReuseStore()

    def checkpoint(self, mutate=None):
        progress = {"finished": True, "safe_for_deletion": False,
                    "consistent_snapshot": False, "rows": target.ARCHIVED,
                    "chunks": [{"key": self.key, "compressed_bytes": len(self.body),
                                "sha256": self.sha}]}
        if mutate:
            mutate(progress)
        raw = json.dumps(progress, sort_keys=True).encode()
        receipt = {"snapshot": target.SNAPSHOT, "all_chunks_replayed": True,
                   "checkpoint_chunks": 1, "verified_next_index": 1,
                   "checkpoint_sha256": hashlib.sha256(raw).hexdigest(),
                   "consistent_snapshot": False, "safe_for_deletion": False}
        return raw, receipt

    def bind(self, store, mutate=None):
        raw, receipt = self.checkpoint(mutate)
        with patch.object(target, "PARTS", 1):
            store.bind_verified_source_checkpoint(raw, receipt)

    def remote(self, body, commands, error=None):
        def run(cmd, **kwargs):
            commands.append(cmd)
            if cmd[0] == "lsjson":
                # Exact production failure at part424: metadata stalls for 45s.
                raise subprocess.TimeoutExpired(cmd, 45)
            self.assertEqual(cmd[0], "cat")
            self.assertLessEqual(kwargs["timeout"], 60)
            if error:
                return subprocess.CompletedProcess(cmd, 1, b"", error)
            offset = int(cmd[cmd.index("--offset") + 1])
            count = int(cmd[cmd.index("--count") + 1])
            return subprocess.CompletedProcess(cmd, 0, body[offset:offset+count], b"")
        return run

    def test_part424_metadata_timeout_does_not_block_pinned_range_recovery(self):
        store = self.store()
        # Inject the already verified manifest state to reproduce the exact
        # old transport failure before the binding method itself exists.
        store.verified_source_parts = {self.key: (len(self.body), self.sha)}
        calls = []
        with patch.object(store, "command", side_effect=lambda args: args):
            with patch("subprocess.run", side_effect=self.remote(self.body, calls)):
                self.assertEqual(store._slow_source_part_get(self.key, len(self.body)), self.body)
        self.assertEqual([c[0] for c in calls], ["cat"])
        self.assertEqual(calls[0][-1], str(len(self.body) + 1))
        self.assertEqual(store.puts, 0)

    def test_pinned_checkpoint_fast_timeout_uses_verified_ranges(self):
        store = self.store()
        self.bind(store)
        calls = []
        timed = subprocess.TimeoutExpired(["rclone", "cat"], 90)
        with patch.object(store, "command", side_effect=lambda args: args):
            with patch.object(CloudStore, "get", side_effect=[timed, timed]) as fast:
                with patch("subprocess.run", side_effect=self.remote(self.body, calls)):
                    with patch("time.sleep"):
                        self.assertEqual(store.get(self.key, len(self.body)), self.body)
        self.assertEqual(fast.call_count, 2)
        self.assertEqual([c[0] for c in calls], ["cat"])
        self.assertEqual(store.gets, 1)
        self.assertEqual(store.bytes_in, len(self.body))
        self.assertEqual(store.puts, 0)

    def test_ranges_reject_corrupt_short_and_appended_remote_body(self):
        for body, error in [(b"X" + self.body[1:], "SHA256 mismatch"),
                            (self.body[:-1], "range length mismatch"),
                            (self.body + b"X", "range length mismatch")]:
            with self.subTest(body=body):
                store = self.store()
                self.bind(store)
                calls = []
                with patch.object(store, "command", side_effect=lambda args: args):
                    with patch("subprocess.run", side_effect=self.remote(body, calls)):
                        with self.assertRaisesRegex(ValueError, error):
                            store._slow_source_part_get(self.key, len(self.body))
                self.assertEqual(store.gets, 0)
                self.assertEqual(store.puts, 0)

    def test_pinned_recovery_still_rejects_denial_and_missing(self):
        for error in [b"HTTP 403 Forbidden", b"object not found"]:
            store = self.store()
            self.bind(store)
            calls = []
            with patch.object(store, "command", side_effect=lambda args: args):
                with patch("subprocess.run", side_effect=self.remote(self.body, calls, error)):
                    with self.assertRaisesRegex(RuntimeError, "denied or missing"):
                        store._slow_source_part_get(self.key, len(self.body))
            self.assertEqual(len(calls), 1)
            self.assertEqual(store.gets, 0)

    def test_invalid_receipt_or_changed_checkpoint_never_binds(self):
        raw, receipt = self.checkpoint()
        changes = [{"checkpoint_sha256": "0"*64}, {"snapshot": "other"},
                   {"all_chunks_replayed": False}, {"checkpoint_chunks": 0},
                   {"verified_next_index": 0}, {"safe_for_deletion": True},
                   {"consistent_snapshot": True}]
        for change in changes:
            store = self.store()
            with self.subTest(change=change), patch.object(target, "PARTS", 1):
                with self.assertRaisesRegex(ValueError, "verified source checkpoint"):
                    store.bind_verified_source_checkpoint(raw, dict(receipt, **change))
                self.assertIsNone(store.verified_source_parts)

    def test_invalid_manifest_metadata_never_binds(self):
        mutations = [
            lambda p: p["chunks"][0].update(sha256="0"*64),
            lambda p: p["chunks"][0].update(compressed_bytes=True),
            lambda p: p["chunks"][0].update(compressed_bytes=0),
            lambda p: p["chunks"][0].update(compressed_bytes=64*target.MIB+1),
            lambda p: p["chunks"][0].update(key="objects/not-a-source"),
            lambda p: p.update(finished=False),
        ]
        for mutate in mutations:
            store = self.store()
            with self.subTest(mutate=mutate):
                with self.assertRaises(ValueError):
                    self.bind(store, mutate)
                self.assertIsNone(store.verified_source_parts)

    def test_duplicate_manifest_key_never_binds(self):
        store = self.store()
        raw, receipt = self.checkpoint(lambda p: p["chunks"].append(dict(p["chunks"][0])))
        receipt.update(checkpoint_chunks=2, verified_next_index=2)
        with patch.object(target, "PARTS", 2):
            with self.assertRaisesRegex(ValueError, "source part metadata"):
                store.bind_verified_source_checkpoint(raw, receipt)
        self.assertIsNone(store.verified_source_parts)

    def test_unbound_key_and_wrong_size_fail_before_any_network(self):
        store = self.store()
        self.bind(store)
        for key, size in [(self.key, len(self.body)+1),
                          (self.key.replace("000424", "000425"), len(self.body))]:
            with patch.object(CloudStore, "get") as fast, patch("subprocess.run") as remote:
                with self.assertRaisesRegex(ValueError, "pinned source part"):
                    store.get(key, size)
                fast.assert_not_called()
                remote.assert_not_called()

    def test_fast_read_does_not_bypass_pinned_hash(self):
        store = self.store()
        self.bind(store)
        with patch.object(CloudStore, "get", return_value=b"X"*len(self.body)) as fast:
            with self.assertRaisesRegex(ValueError, "SHA256 mismatch"):
                store.get(self.key, len(self.body))
        self.assertEqual(fast.call_count, 1)


if __name__ == "__main__":
    unittest.main()
