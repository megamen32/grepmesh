#!/usr/bin/env python3
"""Distinct machines may contain different files at the same path."""
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest

from cloud_index import (MemoryStore, Publisher, Reader, document, ensure_tmpfs,
                         json_bytes, quadgram_bloom_for)
from multi_cloud import MultiReader


def stream(host, body):
    doc = document('/shared/answer.txt', body)
    digest = hashlib.sha256(json_bytes([doc['path'], doc['sha256']])).hexdigest()
    rows = [{'kind': 'begin', 'format': 1, 'source_host': host,
             'source_root': '/shared', 'policy': 'fixture'}, doc,
            {'kind': 'end', 'count': 1, 'logical_sha256': digest,
             'skipped': {}, 'complete_for_policy': True}]
    return io.StringIO(''.join(json.dumps(row) + '\n' for row in rows))


class SourceHostTests(unittest.TestCase):
    def test_overlay_keeps_exact_results_and_is_bound_to_manifest(self):
        base = Path('/run/user') / str(os.getuid()) / 'grepmesh-cloud-multi-tests'
        ensure_tmpfs(base)
        with tempfile.TemporaryDirectory(dir=base) as tmp:
            store = MemoryStore()
            manifest = Publisher(store, tmp, 64 * 1024).publish(
                'first', stream('server-100', 'first host text'))
            pointer = json.loads(store.objects['scopes/first/latest.json'])
            overlay = {'format': 1, 'scope': 'first', 'source_host': 'server-100',
                       'manifest_sha256': pointer['sha256'], 'documents': 1,
                       'filters': [{'shard_sha256': part['sha256'],
                                    'quadgram': quadgram_bloom_for(['first host text\n/shared/answer.txt'])}
                                   for part in manifest['parts']]}
            raw = json_bytes(overlay)
            key = 'overlays/quadgram/first/test.json'
            store.put(key, raw)
            reader = Reader(store, tmp, 'first', allowed_host='server-100',
                            quadgram_overlay={'key': key, 'sha256': hashlib.sha256(raw).hexdigest()})
            reader.refresh()
            self.assertEqual(reader.query('host text')['results'][0]['path'], '/shared/answer.txt')
            self.assertEqual(reader.read_text('/shared/answer.txt', authorized_roots=['/shared'])
                             ['chunks'][0]['lines'][0]['text'], 'first host text')
            self.assertEqual(reader.query('text-that-is-absent')['results'], [])
            overlay['manifest_sha256'] = '0' * 64
            raw = json_bytes(overlay)
            store.put(key, raw)
            mismatch = Reader(store, tmp, 'first', allowed_host='server-100',
                              quadgram_overlay={'key': key, 'sha256': hashlib.sha256(raw).hexdigest()})
            with self.assertRaisesRegex(ValueError, 'binding'):
                mismatch.refresh()

    def test_oversize_source_must_match_scope(self):
        base = Path('/run/user') / str(os.getuid()) / 'grepmesh-cloud-multi-tests'
        ensure_tmpfs(base)
        with tempfile.TemporaryDirectory(dir=base) as tmp:
            with self.assertRaisesRegex(ValueError, 'source_host'):
                MultiReader(MemoryStore(), tmp, [
                    {'scope': 'first', 'source_root': '/shared', 'source_host': 'server-100',
                     'oversize': {'host': 'server-88', 'snapshot': 'test'}}], cache_mib=64)

    def test_same_path_requires_source_and_reads_the_selected_host(self):
        base = Path('/run/user') / str(os.getuid()) / 'grepmesh-cloud-multi-tests'
        ensure_tmpfs(base)
        with tempfile.TemporaryDirectory(dir=base) as tmp:
            store = MemoryStore()
            publisher = Publisher(store, tmp, 64 * 1024)
            publisher.publish('first', stream('server-100', 'first host text'))
            publisher.publish('second', stream('server-88', 'second host text'))
            reader = MultiReader(store, tmp, [
                {'scope': 'first', 'source_root': '/shared', 'source_host': 'server-100'},
                {'scope': 'second', 'source_root': '/shared', 'source_host': 'server-88'},
            ], cache_mib=128)
            reader.refresh()
            with self.assertRaisesRegex(ValueError, 'source_host'):
                reader.read_text('/shared/answer.txt', authorized_roots=['/shared'])
            for host, expected in [('server-100', 'first host text'),
                                   ('server-88', 'second host text')]:
                result = reader.read_text('/shared/answer.txt', source_host=host,
                                          authorized_roots=['/shared'])
                self.assertEqual(result['source_host'], host)
                self.assertEqual(result['chunks'][0]['lines'][0]['text'], expected)
            with self.assertRaises(PermissionError):
                reader.read_text('/shared/answer.txt', source_host='server-88',
                                 authorized_roots=['/elsewhere'])
            with self.assertRaises(ValueError):
                reader.read_text('/shared/answer.txt', source_host='server-44',
                                 authorized_roots=['/shared'])
            self.assertEqual({hit['source_host'] for hit in reader.query('host text')['results']},
                             {'server-100', 'server-88'})
            # A single-scope Reader can be created without a configured allowed_host.
            # Validate against its verified manifest rather than that optional setting.
            single = Reader(store, tmp, 'first')
            self.assertEqual(single.read_text('/shared/answer.txt', source_host='server-100',
                                              authorized_roots=['/shared'])['source_host'],
                             'server-100')
            with self.assertRaises(ValueError):
                single.read_text('/shared/answer.txt', source_host='server-88',
                                 authorized_roots=['/shared'])


if __name__ == '__main__':
    unittest.main()
