#!/usr/bin/env python3
"""Fail-closed retry of immutable private GrepMesh FTS shard publication."""
import hashlib
import json
import subprocess
import unittest
from unittest.mock import patch

from publish_native100_rolling_private import VerifiedImmutableReuseStore
from cloud_index import CloudStore


class VerifiedReuseTests(unittest.TestCase):
    data = b"gzip-shard-with-fixed-contents"
    key = "objects/" + "0123456789abcdef" * 4 + ".sqlite.gz"

    def metadata(self, data=None):
        payload = self.data if data is None else data
        return json.dumps({"Size": len(payload),
                           "Hashes": {"md5": hashlib.md5(payload).hexdigest()}}).encode()

    def exercise(self, responses, inventory=None):
        with patch.object(VerifiedImmutableReuseStore, "_load_verified_inventory",
                          return_value={} if inventory is None else inventory):
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
        with patch.object(VerifiedImmutableReuseStore, "_load_verified_inventory",
                          return_value={}):
            store = VerifiedImmutableReuseStore()
        with self.assertRaisesRegex(ValueError, "Unsafe cloud key"):
            store.put("objects/../oops", self.data)

    def test_inventory_reuse_without_any_object_probe(self):
        inventory = {self.key: (len(self.data), hashlib.md5(self.data).hexdigest())}
        store, calls = self.exercise([], inventory=inventory)
        self.assertEqual(store.reused, 1)
        self.assertEqual(store.reused_from_inventory, 1)
        self.assertEqual(store.puts, 0)
        self.assertEqual(calls, [])

    def test_inventory_collision_aborts_without_remote_write(self):
        with self.assertRaisesRegex(RuntimeError, "collision or corruption"):
            self.exercise([], inventory={self.key: (1, "0" * 32)})

    def test_cloud_inventory_parses_only_remote_verified_metadata(self):
        name = self.key.split("/", 1)[1]
        entries = [{"Name": name, "Path": name, "Size": len(self.data),
                    "IsDir": False,
                    "Hashes": {"md5": hashlib.md5(self.data).hexdigest()}}]
        with patch.object(VerifiedImmutableReuseStore, "command",
                          side_effect=lambda args: args):
            with patch("subprocess.run", return_value=subprocess.CompletedProcess(
                    ["lsjson"], 0, json.dumps(entries).encode(), b"")) as run:
                store = VerifiedImmutableReuseStore()
        self.assertEqual(store.known[self.key],
                         (len(self.data), hashlib.md5(self.data).hexdigest()))
        self.assertEqual(run.call_count, 1)

    def test_missing_or_untrusted_cloud_inventory_fails_closed(self):
        name = self.key.split("/", 1)[1]
        cases = [
            (1, b"", b"connection error"),
            (0, b"{}", b""),
            (0, b"not-json", b""),
            (0, json.dumps([{"Name": name, "Path": name,
                             "Size": len(self.data), "IsDir": False,
                             "Hashes": {}}]).encode(), b""),
        ]
        for rc, output, err in cases:
            with self.subTest(output=output[:50]):
                with patch.object(VerifiedImmutableReuseStore, "command",
                                  side_effect=lambda args: args):
                    with patch("subprocess.run", return_value=subprocess.CompletedProcess(
                            ["lsjson"], rc, output, err)):
                        with self.assertRaises(RuntimeError):
                            VerifiedImmutableReuseStore()
    def test_transient_private_archive_read_retried(self):
        with patch.object(VerifiedImmutableReuseStore, "_load_verified_inventory", return_value={}):
            store = VerifiedImmutableReuseStore()
        with patch.object(CloudStore, "get", side_effect=[
                RuntimeError("Cloud read failed: archives/test"),
                b"immutable-data"]) as read:
            with patch("time.sleep"):
                result = store.get("archives/test")
        self.assertEqual(result, b"immutable-data")
        self.assertEqual(read.call_count, 2)

    def test_private_archive_read_corruption_not_retried(self):
        with patch.object(VerifiedImmutableReuseStore, "_load_verified_inventory", return_value={}):
            store = VerifiedImmutableReuseStore()
        with patch.object(CloudStore, "get", side_effect=
                          ValueError("Cloud object exceeds declared bound")) as read:
            with self.assertRaisesRegex(ValueError, "exceeds declared bound"):
                store.get("archives/test")
        self.assertEqual(read.call_count, 1)

    def test_private_archive_read_timeout_fails_closed(self):
        with patch.object(VerifiedImmutableReuseStore, "_load_verified_inventory", return_value={}):
            store = VerifiedImmutableReuseStore()
        exc = subprocess.TimeoutExpired(cmd=["rclone", "cat"], timeout=90)
        with patch.object(CloudStore, "get", side_effect=exc) as read:
            with patch("time.sleep"):
                with self.assertRaisesRegex(RuntimeError, "bounded retries"):
                    store.get("archives/test")
        self.assertEqual(read.call_count, 3)



    def test_large_archive_recovery_uses_exact_size_and_ranged_reads(self):
        with patch.object(VerifiedImmutableReuseStore, "_load_verified_inventory", return_value={}):
            store = VerifiedImmutableReuseStore()
        from publish_native100_rolling_private import PREFIX
        key = PREFIX + "/parts/000424-" + "a" * 64 + ".jsonl.gz"
        blob = b"archived"
        timed = subprocess.TimeoutExpired(cmd=["rclone", "cat"], timeout=90)
        metadata = json.dumps({"Size": len(blob), "Hashes": {
            "md5": hashlib.md5(blob).hexdigest()}}).encode()
        replies = [
            subprocess.CompletedProcess(["lsjson"], 0, metadata, b""),
            subprocess.CompletedProcess(["cat"], 0, blob, b""),
        ]

        def respond(cmd, **kwargs):
            return replies.pop(0)

        with patch.object(CloudStore, "get", side_effect=[timed, timed]) as fast:
            with patch.object(store, "command", side_effect=lambda args: args):
                with patch("subprocess.run", side_effect=respond) as slow:
                    with patch("time.sleep"):
                        self.assertEqual(store.get(key, len(blob)), blob)
        self.assertEqual(fast.call_count, 2)
        self.assertEqual([call.args[0][0] for call in slow.call_args_list],
                         ["lsjson", "cat"])
        self.assertEqual(slow.call_args_list[0].kwargs["timeout"], 45)
        self.assertLessEqual(slow.call_args_list[1].kwargs["timeout"], 60)
        self.assertIn("--offset", slow.call_args_list[1].args[0])
        self.assertIn("--count", slow.call_args_list[1].args[0])
        self.assertNotIn("--head", slow.call_args_list[1].args[0])
        self.assertFalse(replies)
        self.assertEqual(store.gets, 1)
        self.assertEqual(store.bytes_in, len(blob))

    def test_range_chunk_retries_exact_offset_and_preserves_complete_body(self):
        from publish_native100_rolling_private import PREFIX, MIB
        blob = b"X" * MIB + b"Y" * MIB + b"Z" * 31
        key = PREFIX + "/parts/000424-" + "a" * 64 + ".jsonl.gz"
        md5 = hashlib.md5(blob).hexdigest()
        with patch.object(VerifiedImmutableReuseStore, "_load_verified_inventory", return_value={}):
            store = VerifiedImmutableReuseStore()
        commands = []
        flaky = {"once": False}

        def remote(cmd, **kwargs):
            commands.append(cmd)
            if cmd[0] == "lsjson":
                return subprocess.CompletedProcess(cmd, 0,
                    json.dumps({"Size": len(blob), "Hashes": {"md5": md5}}).encode(), b"")
            self.assertEqual(cmd[0], "cat")
            offset = int(cmd[cmd.index("--offset") + 1])
            count = int(cmd[cmd.index("--count") + 1])
            self.assertLessEqual(kwargs["timeout"], 60)
            if offset == MIB and not flaky["once"]:
                flaky["once"] = True
                raise subprocess.TimeoutExpired(cmd=cmd, timeout=60)
            return subprocess.CompletedProcess(cmd, 0, blob[offset:offset+count], b"")

        timed = subprocess.TimeoutExpired(["rclone", "cat"], 90)
        with patch.object(store, "command", side_effect=lambda args: args):
            with patch.object(CloudStore, "get", side_effect=[timed, timed]):
                with patch("subprocess.run", side_effect=remote), patch("time.sleep"):
                    self.assertEqual(store.get(key, len(blob)), blob)
        self.assertTrue(flaky["once"])
        self.assertEqual([int(c[c.index("--offset")+1]) for c in commands if c[0] == "cat"],
                         [0, MIB, MIB, 2*MIB])
        self.assertEqual(store.bytes_in, len(blob))
        self.assertEqual(store.gets, 1)
        self.assertEqual(store.puts, 0)

    def test_ranged_archive_rejects_short_chunk_without_write(self):
        from publish_native100_rolling_private import PREFIX
        key = PREFIX + "/parts/000424-" + "a" * 64 + ".jsonl.gz"
        blob = b"ABCD"
        with patch.object(VerifiedImmutableReuseStore, "_load_verified_inventory", return_value={}):
            store = VerifiedImmutableReuseStore()
        replies = iter([
            subprocess.CompletedProcess([], 0, self.metadata(blob), b""),
            subprocess.CompletedProcess([], 0, b"ABC", b"")
        ])
        with patch.object(store, "command", side_effect=lambda args: args):
            with patch("subprocess.run", side_effect=lambda cmd, **kw: next(replies)):
                with self.assertRaisesRegex(ValueError, "range length mismatch"):
                    store._slow_source_part_get(key, 4)
        self.assertEqual(store.gets, 0)
        self.assertEqual(store.puts, 0)

    def test_ranged_archive_rejects_bad_metadata_or_hash(self):
        from publish_native100_rolling_private import PREFIX
        key = PREFIX + "/parts/000424-" + "a" * 64 + ".jsonl.gz"
        with patch.object(VerifiedImmutableReuseStore, "_load_verified_inventory", return_value={}):
            store = VerifiedImmutableReuseStore()
        for info in ({"Size": 3, "Hashes": {"md5": hashlib.md5(b"abc").hexdigest()}},
                     {"Size": 4, "Hashes": {}}):
            with self.subTest(info=info):
                with patch.object(store, "command", side_effect=lambda args: args):
                    with patch("subprocess.run", return_value=subprocess.CompletedProcess(
                        [], 0, json.dumps(info).encode(), b"")) as run:
                        with self.assertRaisesRegex(ValueError, "remote size/hash mismatch"):
                            store._slow_source_part_get(key, 4)
                self.assertEqual(run.call_count, 1)
        with patch.object(store, "command", side_effect=lambda args: args):
            with patch("subprocess.run", side_effect=[
                subprocess.CompletedProcess([], 0, self.metadata(b"AAAA"), b""),
                subprocess.CompletedProcess([], 0, b"BBBB", b""),
            ]):
                with self.assertRaisesRegex(ValueError, "complete MD5 mismatch"):
                    store._slow_source_part_get(key, 4)
        self.assertEqual(store.gets, 0)

    def test_ranged_archive_bounded_timeout_and_denial_fail_closed(self):
        from publish_native100_rolling_private import PREFIX
        key = PREFIX + "/parts/000424-" + "a" * 64 + ".jsonl.gz"
        with patch.object(VerifiedImmutableReuseStore, "_load_verified_inventory", return_value={}):
            store = VerifiedImmutableReuseStore()
        meta = subprocess.CompletedProcess([], 0, self.metadata(b"ABCD"), b"")
        timed = subprocess.TimeoutExpired(cmd=["rclone", "cat"], timeout=60)
        with patch.object(store, "command", side_effect=lambda args: args):
            with patch("subprocess.run", side_effect=[meta, timed, timed, timed]) as call:
                with patch("time.sleep"):
                    with self.assertRaisesRegex(RuntimeError, "bounded retries"):
                        store._slow_source_part_get(key, 4)
        self.assertEqual(call.call_count, 4)
        with patch.object(store, "command", side_effect=lambda args: args):
            with patch("subprocess.run", side_effect=[
                meta, subprocess.CompletedProcess([], 1, b"", b"HTTP 403 Forbidden")
            ]) as call:
                with self.assertRaisesRegex(RuntimeError, "denied or missing"):
                    store._slow_source_part_get(key, 4)
        self.assertEqual(call.call_count, 2)
        self.assertEqual(store.gets, 0)

    def test_post_upload_timeout_reconciles_matching_remote_without_reupload(self):
        with patch.object(VerifiedImmutableReuseStore, "_load_verified_inventory",
                          return_value={}):
            store = VerifiedImmutableReuseStore()
        responses = [
            (1, b"", b"directory not found"),  # initial remote object absent
            (0, b"", b""),  # rcat uploaded immutable object
            subprocess.TimeoutExpired(["rclone", "lsjson"], 45),  # verify timed out
            (0, self.metadata(), b""),  # object is safely present, verified
        ]
        commands = []

        def fake_run(cmd, **kwargs):
            commands.append(cmd[0])
            reply = responses.pop(0)
            if isinstance(reply, Exception):
                raise reply
            return subprocess.CompletedProcess(cmd, *reply)

        with patch.object(store, "command", side_effect=lambda args: args):
            with patch("subprocess.run", side_effect=fake_run):
                store.put(self.key, self.data)
        self.assertEqual(commands, ["lsjson", "rcat", "lsjson", "lsjson"])
        self.assertEqual(store.puts, 1)
        self.assertEqual(store.known[self.key], (len(self.data),
                                                 hashlib.md5(self.data).hexdigest()))
        self.assertEqual(responses, [])

    def test_post_upload_timeout_fails_closed_on_wrong_remote_md5(self):
        with patch.object(VerifiedImmutableReuseStore, "_load_verified_inventory",
                          return_value={}):
            store = VerifiedImmutableReuseStore()
        responses = iter([
            (1, b"", b"directory not found"),
            (0, b"", b""),
            subprocess.TimeoutExpired(["rclone", "lsjson"], 45),
            (0, self.metadata(b"wrong body"), b""),
        ])

        def fake_run(cmd, **kwargs):
            reply = next(responses)
            if isinstance(reply, Exception):
                raise reply
            return subprocess.CompletedProcess(cmd, *reply)

        with patch.object(store, "command", side_effect=lambda args: args):
            with patch("subprocess.run", side_effect=fake_run):
                with self.assertRaisesRegex(RuntimeError, "size/hash mismatch"):
                    store.put(self.key, self.data)
        self.assertEqual(store.puts, 0)

if __name__ == "__main__":
    unittest.main()
