"""Inventory parser regressions, including captured listing, never cloud I/O."""
import copy
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import unittest
from unittest.mock import patch

from publish_native100_rolling_private import VerifiedImmutableReuseStore
from cloud_index import CloudStore, MIB


class InventoryDuplicateTests(unittest.TestCase):
    def row(self, char='a'):
        name = char * 64 + '.sqlite.gz'
        return {'Name': name, 'Path': name, 'Size': 1234, 'IsDir': False,
                'Hashes': {'md5': 'b' * 32}, 'ID': 'synthetic-owner-object',
                'ModTime': '2026-11-01T00:00:00Z', 'MimeType': 'application/gzip'}

    def parse(self, rows=None, raw=None, rc=0):
        raw = json.dumps(rows).encode() if raw is None else raw
        response = subprocess.CompletedProcess(['lsjson'], rc, raw, b'')
        with patch.object(VerifiedImmutableReuseStore, 'command', side_effect=lambda a: a):
            with patch('subprocess.run', return_value=response) as call:
                with patch.object(CloudStore, 'put', side_effect=AssertionError('write forbidden')):
                    store = VerifiedImmutableReuseStore()
        self.assertEqual(call.call_count, 1)
        self.assertEqual(store.puts, 0)
        return store

    def test_identical_full_record_collapses_without_additional_io(self):
        row = self.row()
        s = self.parse([row, copy.deepcopy(row)])
        self.assertEqual(len(s.known), 1)
        self.assertEqual(s.inventory_listed_rows, 2)
        self.assertEqual(s.inventory_identical_duplicate_rows, 1)
        self.assertEqual(next(iter(s.known.values())), (1234, 'b' * 32))

    def test_nonadjacent_duplicates_and_dictionary_key_order(self):
        a, b = self.row(), self.row('c')
        reordered = dict(reversed(list(a.items())))
        s = self.parse([a, b, reordered, copy.deepcopy(b), a])
        self.assertEqual(len(s.known), 2)
        self.assertEqual(s.inventory_identical_duplicate_rows, 3)

    def test_conflicting_size_hash_identity_time_or_extra_field_fails_closed(self):
        a = self.row()
        variants = [('Size', 1235), ('Hashes', {'md5': 'c' * 32}),
                    ('ID', 'another-object'), ('ModTime', '2026-11-02T00:00:00Z'),
                    ('new_field', 'unreviewed'), ('MimeType', 'application/octet-stream')]
        for field, value in variants:
            b = copy.deepcopy(a); b[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, 'Conflicting duplicate'):
                self.parse([a, b])

    def test_malformed_repeated_record_cannot_skip_validation(self):
        a = self.row()
        for field, value in [('Path', 'other'), ('Size', True), ('IsDir', True), ('Hashes', {})]:
            b = copy.deepcopy(a); b[field] = value
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                self.parse([a, b])

    def test_listing_error_or_torn_json_cannot_be_deduplicated_into_success(self):
        for raw, rc in [(b'[]', 1), (b'[', 0), (b'{}', 0), (b'null', 0)]:
            with self.subTest(rc=rc, raw=raw), self.assertRaises(RuntimeError):
                self.parse(raw=raw, rc=rc)

    def test_listing_payload_limit_unchanged(self):
        with self.assertRaisesRegex(RuntimeError, 'too large'):
            self.parse(raw=b' ' * (32*MIB + 1))

    def test_distinct_objects_remain_distinct(self):
        s = self.parse([self.row('a'), self.row('c')])
        self.assertEqual(len(s.known), 2)
        self.assertEqual(s.inventory_identical_duplicate_rows, 0)

    @unittest.skipUnless(os.environ.get('GREPMESH_TEST_REAL_CAPACITY_INVENTORY') == '1',
                         'explicit read-only cached metadata replay required')
    def test_captured_real_listing_replays_offline(self):
        path = Path('/run/user/1000/grepmesh-native100-capacity-metadata-20261103/inventory.json')
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as f:
            info = os.fstat(f.fileno())
            self.assertTrue(stat.S_ISREG(info.st_mode))
            self.assertEqual(info.st_uid, os.getuid())
            self.assertEqual(info.st_nlink, 1)
            self.assertEqual(info.st_mode & 0o077, 0)
            self.assertEqual(info.st_size, 1845527)
            raw = f.read(32*MIB + 1)
        self.assertEqual(hashlib.sha256(raw).hexdigest(),
            'e79bdc094f231815b16f74ee11d195bcde103e43a6cafe0b0a6184428fea2d6d')
        s = self.parse(raw=raw)
        self.assertEqual(s.inventory_listed_rows, 5605)
        self.assertEqual(len(s.known), 5604)
        self.assertEqual(s.inventory_identical_duplicate_rows, 1)


if __name__ == '__main__':
    unittest.main()
