"""Pinned immutable source-part cache: synthetic tmpfs-only, no network."""
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import publish_native100_rolling_private as target
from cloud_index import CloudStore


class PinnedSourcePartCacheTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(
            prefix="grepmesh-pinned-cache-test-", dir="/run/user/1000")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        scope = patch.object(target, "ROOT", self.root)
        scope.start()
        self.addCleanup(scope.stop)
        self.cache = self.root / "verified-source-cache"
        self.cache.mkdir(mode=0o700)
        self.body = b"immutable source bytes test only"
        self.sha = hashlib.sha256(self.body).hexdigest()
        self.key = target.PREFIX + "/parts/000424-" + self.sha + ".jsonl.gz"
        with patch.object(target.VerifiedImmutableReuseStore,
                          "_load_verified_inventory", return_value={}):
            self.store = target.VerifiedImmutableReuseStore()
        self.store.verified_source_parts = {self.key: (len(self.body), self.sha)}
        self.path = self.cache / self.key.rsplit("/", 1)[-1]

    def stage_fixture(self, contents=None):
        self.path.write_bytes(self.body if contents is None else contents)
        self.path.chmod(0o600)

    def test_verified_cache_precedes_all_network(self):
        self.stage_fixture()
        with patch.object(CloudStore, "get", side_effect=AssertionError("network")):
            self.assertEqual(self.store.get(self.key, len(self.body)), self.body)
        self.assertEqual((self.store.gets, self.store.bytes_in, self.store.puts),
                         (1, len(self.body), 0))

    def test_required_ordinal_preflight_and_cache_read(self):
        self.stage_fixture()
        progress = {"chunks": [{"key": self.key, "sha256": self.sha,
                                "compressed_bytes": len(self.body)}]}
        with patch.dict(os.environ, {"GREPMESH_REQUIRE_CACHED_SOURCE_PART": "000424"}):
            target.preflight_required_source_cache(self.store, progress)
            with patch.object(CloudStore, "get", side_effect=AssertionError("network")):
                self.assertEqual(self.store.get(self.key, len(self.body)), self.body)
        self.assertEqual(self.store.gets, 2)

    def test_absent_required_part_fails_closed_before_cloud(self):
        with patch.dict(os.environ, {"GREPMESH_REQUIRE_CACHED_SOURCE_PART": "000424"}):
            with patch.object(CloudStore, "get", side_effect=AssertionError("network")) as cloud:
                with self.assertRaisesRegex(RuntimeError, "Required verified source cache missing"):
                    self.store.get(self.key, len(self.body))
                cloud.assert_not_called()
        with patch.dict(os.environ, {"GREPMESH_REQUIRE_CACHED_SOURCE_PART": "000424"}):
            with self.assertRaisesRegex(RuntimeError, "missing before replay"):
                target.preflight_required_source_cache(
                    self.store, {"chunks": [{"key": self.key, "sha256": self.sha,
                                             "compressed_bytes": len(self.body)}]})

    def test_hash_corruption_refuses_cloud_fallback(self):
        self.stage_fixture(b"X" + self.body[1:])
        with patch.object(CloudStore, "get", side_effect=AssertionError("network")) as cloud:
            with self.assertRaisesRegex(ValueError, "SHA256 mismatch"):
                self.store.get(self.key, len(self.body))
            cloud.assert_not_called()

    def test_truncated_cache_refuses_cloud_fallback(self):
        self.stage_fixture(self.body[:-1])
        with patch.object(CloudStore, "get", side_effect=AssertionError("network")) as cloud:
            with self.assertRaisesRegex(ValueError, "size mismatch"):
                self.store.get(self.key, len(self.body))
            cloud.assert_not_called()

    def test_symlink_or_world_readable_cache_refused(self):
        self.stage_fixture()
        self.path.chmod(0o644)
        with self.assertRaises(PermissionError):
            self.store.get(self.key, len(self.body))
        self.path.unlink()
        target_file = self.root / "other"
        target_file.write_bytes(self.body)
        self.path.symlink_to(target_file)
        with self.assertRaises(OSError):
            self.store.get(self.key, len(self.body))

    def test_hardlink_cache_refused(self):
        self.stage_fixture()
        os.link(self.path, self.root / "second-link")
        with self.assertRaises(PermissionError):
            self.store.get(self.key, len(self.body))

    def test_wrong_key_and_size_rejected_before_cloud(self):
        self.stage_fixture()
        for key, size in [(self.key.replace("000424", "000425"), len(self.body)),
                          (self.key, len(self.body)+1)]:
            with self.subTest(key=key, size=size):
                with patch.object(CloudStore, "get", side_effect=AssertionError("network")):
                    with self.assertRaisesRegex(ValueError, "pinned source part"):
                        self.store.get(key, size)

    def test_absent_optional_cache_keeps_old_verified_transport(self):
        with patch.dict(os.environ, {"GREPMESH_REQUIRE_CACHED_SOURCE_PART": ""}):
            with patch.object(CloudStore, "get", return_value=self.body) as cloud:
                self.assertEqual(self.store.get(self.key, len(self.body)), self.body)
                cloud.assert_called_once()

    def test_bad_required_ordinal_and_unbound_manifest_denied(self):
        with patch.dict(os.environ, {"GREPMESH_REQUIRE_CACHED_SOURCE_PART": "424;bad"}):
            with self.assertRaisesRegex(RuntimeError, "Invalid required source cache ordinal"):
                self.store.get(self.key, len(self.body))
        with patch.dict(os.environ, {"GREPMESH_REQUIRE_CACHED_SOURCE_PART": "000424"}):
            with self.assertRaisesRegex(RuntimeError, "not unique"):
                target.preflight_required_source_cache(self.store, {"chunks": []})


if __name__ == "__main__":
    unittest.main()
