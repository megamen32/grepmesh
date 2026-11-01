#!/usr/bin/env python3
"""Fail-closed retry of immutable private GrepMesh FTS shard publication."""
import hashlib
import json
import subprocess
import unittest
from unittest.mock import patch

from publish_native100_rolling_private import VerifiedImmutableReuseStore


class VerifiedReuseTests(unittest.TestCase):
    data = b"gzip-shard-with-fixed-contents"
    key = "objects/0123456789abcdef.sqlite.gz"

    def metadata(self, data=None):
        payload = self.data if data is None else data
        return json.dumps({"Size": len(payload),
                           "Hashes": {"md5": hashlib.md5(payload).hexdigest()}}).encode()

    def exercise(self, responses):
        store = VerifiedImmutableReuseStore()
        commands = []

        def fake_run(cmd, **kwargs):
            commands.append(cmd)
            if not responses:
                raise AssertionError("Unexpected extra remote command")
            rc, stdout, stderr = responses.pop(0)
            return subprocess.CompletedProcess(cmd, rc, stdout, stderr)

        with patch.object(store, "command", side_effect=lambda args: args):
            with patch("subprocess.run", side_effect=fake_run):
                store.put(self.key, self.data)
        return store, commands

    def test_existing_identical_object_skipped_without_write(self):
        store, calls = self.exercise([(0, self.metadata(), b"")])
        self.assertEqual(store.reused, 1)
        self.assertEqual(store.puts, 0)
        self.assertEqual([call[0] for call in calls], ["lsjson"])

    def test_missing_object_uploaded_and_verified(self):
        store, calls = self.exercise([
            (1, b"", b"NOTICE: Failed to lsjson: directory not found"),
            (0, b"", b""),
            (0, self.metadata(), b""),
        ])
        self.assertEqual(store.reused, 0)
        self.assertEqual(store.puts, 1)
        self.assertEqual([call[0] for call in calls], ["lsjson", "rcat", "lsjson"])

    def test_mismatched_remote_refused(self):
        for metadata in (self.metadata(b"different"), b'{"Size":10,"Hashes":{}}'):
            with self.subTest(metadata=metadata):
                with self.assertRaisesRegex(RuntimeError, "collision or corruption"):
                    self.exercise([(0, metadata, b"")])

    def test_cloud_errors_not_mistaken_for_absence(self):
        with self.assertRaisesRegex(RuntimeError, "existence check failed"):
            self.exercise([(1, b"", b"ERROR: connection timeout")])

    def test_unsafe_object_key_refused_without_remote_call(self):
        store = VerifiedImmutableReuseStore()
        with self.assertRaisesRegex(ValueError, "Unsafe cloud key"):
            store.put("objects/../oops", self.data)


if __name__ == "__main__":
    unittest.main()
