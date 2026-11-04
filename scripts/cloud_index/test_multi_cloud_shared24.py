#!/usr/bin/env python3
"""Synthetic isolated mixed 16+8 RAM-reader routing and shared-cache checks.

No real source corpus, private cloud, credential, production service or profiler.
"""
import collections
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest

from cloud_index import (MemoryStore, Publisher, Reader, SharedShardCache,
                         MIB, document, ensure_tmpfs, json_bytes)
from multi_cloud import MultiReader


def make_stream(host, paths):
    rows = [{'kind': 'begin', 'format': 1, 'source_host': host,
             'source_root': '/', 'policy': 'synthetic private history fixture'}]
    digest = hashlib.sha256()
    for path, body in sorted(paths):
        d = document(path, body)
        rows.append(d)
        digest.update(json_bytes([d['path'], d['sha256']]))
    rows.append({'kind': 'end', 'count': len(paths),
                 'logical_sha256': digest.hexdigest(), 'skipped': {},
                 'complete_for_policy': True})
    return io.StringIO(''.join(json.dumps(row) + '\n' for row in rows))


def find_bucket_path(host, bucket, modulus):
    for i in range(300):
        path = '/private/history/%s/item-%02d-%d.txt' % (host, bucket, i)
        if hashlib.sha256(path.encode()).digest()[0] % modulus == bucket:
            return path
    raise AssertionError('No deterministic synthetic bucket fixture')


class SharedCacheMixedSources(unittest.TestCase):
    def setUp(self):
        self.user_root = Path('/run/user') / str(os.getuid())
        ensure_tmpfs(self.user_root)
        self.directory = tempfile.TemporaryDirectory(
            prefix='grepmesh-mixed24-source-only-', dir=self.user_root)
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.store = MemoryStore()

    def test_24_private_scopes_one_aggregate_cache_and_two_host_route(self):
        publisher = Publisher(self.store, self.root, 64 * 1024)
        scopes = []
        unique = {}
        collision = '/private/history/collision.txt'
        for host, count, prefix in [
            ('server-100', 16, 'rolling100-20261031t233108z-5732b979-'),
            ('server-44', 8, 'legacy-server44-')]:
            collision_bucket = hashlib.sha256(collision.encode()).digest()[0] % count
            for bucket in range(count):
                path = find_bucket_path(host, bucket, count)
                text = 'private marker %s %s' % (host, bucket)
                unique[(host, bucket)] = (path, text)
                paths = [(path, text)]
                if bucket == collision_bucket:
                    paths.append((collision, 'collision distinct body '+host))
                scope = prefix + '%02d' % bucket
                publisher.publish(scope, make_stream(host, paths))
                scopes.append({'scope': scope, 'source_host': host, 'source_root': '/'})
        self.assertEqual(len(scopes), 24)
        federation = MultiReader(self.store, self.root, scopes, cache_mib=512)
        self.assertIsNotNone(federation.shared_cache)
        federation.refresh()
        self.assertEqual(federation.manifest['documents'], 26)
        self.assertLessEqual(federation.used, 512 * MIB)
        for host, bucket in [('server-100', 0), ('server-100', 15),
                              ('server-44', 0), ('server-44', 7)]:
            path, body = unique[(host, bucket)]
            result = federation.read_text(path, 1, 1, authorized_roots=['/private'])
            self.assertEqual(result['source_host'], host)
            self.assertEqual(result['chunks'][0]['lines'][0]['text'], body)
            qualified = federation.read_text(path, 1, 1,
                                            authorized_roots=['/private'],
                                            source_host=host)
            self.assertEqual(qualified['chunks'][0]['lines'][0]['text'], body)
        for host in ('server-100', 'server-44'):
            result = federation.read_text(collision, 1, 1,
                                          authorized_roots=['/private'],
                                          source_host=host)
            self.assertEqual(result['chunks'][0]['lines'][0]['text'],
                             'collision distinct body '+host)
        with self.assertRaisesRegex(ValueError, 'Ambiguous historical path'):
            federation.read_text(collision, 1, 1, authorized_roots=['/private'])
        with self.assertRaises(ValueError):
            federation.read_text(collision, 1, 1, source_host='unknown-host')
        with self.assertRaises(PermissionError):
            federation.read_text(collision, 1, 1,
                                 authorized_roots=['/dev'], source_host='server-44')
        found = federation.query('private marker', limit=64,
                                  authorized_roots=['/private'], deadline_seconds=25)
        self.assertFalse(found['partial'], found['warnings'])
        self.assertEqual(len(found['results']), 24)
        self.assertEqual({h['source_host'] for h in found['results']},
                         {'server-100', 'server-44'})
        self.assertLessEqual(federation.used, 512 * MIB)

    def test_shared_cache_eviction_and_clear_keep_aggregate_accounting(self):
        shared = SharedShardCache(64 * MIB)
        left = Reader(self.store, self.root, 'synthetic-left', 64 * MIB,
                      shared_cache=shared)
        right = Reader(self.store, self.root, 'synthetic-right', 64 * MIB,
                       shared_cache=shared)
        def install(owner, label):
            ident = hashlib.sha256(label.encode()).hexdigest()
            size = 20 * MIB
            shared.admit(size)
            path = owner.directory / (ident + '.sqlite')
            with path.open('xb') as f:
                f.truncate(size)  # sparse tmpfs test file, no bulk data writes
            owner.cache[ident] = path
            owner.used += size
            shared.commit(owner, ident, path, size)
            self.assertLessEqual(shared.used, 64 * MIB)
            return ident, path
        first, first_path = install(left, 'first')
        install(right, 'second')
        install(left, 'third')
        install(right, 'fourth')
        self.assertNotIn(first, left.cache)
        self.assertFalse(first_path.exists())
        self.assertEqual(shared.used, 60 * MIB)
        self.assertEqual(shared.used, left.used + right.used)
        right.clear()
        self.assertEqual(shared.used, left.used)
        left.clear()
        self.assertEqual(shared.used, 0)
        self.assertFalse(shared.entries)

    def test_shared_query_prefetch_caps_compressed_lookahead_and_preserves_legacy(self):
        import cloud_index
        from unittest.mock import patch
        publisher = Publisher(self.store, self.root, 64 * 1024)
        publisher.publish('one-lookup', make_stream('server-44', [
            ('/private/history/synthetic-needle.txt',
             'historical synthetic prefetch concurrency fixture')]))
        shared = SharedShardCache(64 * MIB)
        reader = Reader(self.store, self.root, 'one-lookup', 64 * MIB,
                        allowed_host='server-44', allowed_root='/',
                        shared_cache=shared)
        reader.refresh()
        seen = []
        original = cloud_index.prefetched_shards

        def record_prefetch(*args, **kwargs):
            seen.append(kwargs.get('workers', 6))
            yield from original(*args, **kwargs)

        with patch.object(cloud_index, 'prefetched_shards', record_prefetch):
            result = reader.query('prefetch concurrency fixture',
                                  authorized_roots=['/private'])
        self.assertEqual(len(result['results']), 1)
        self.assertEqual(seen, [2])
        self.assertLessEqual(shared.used, 64 * MIB)

        standalone_root = self.root / 'standalone'
        standalone_root.mkdir()
        standalone = Reader(self.store, standalone_root, 'one-lookup', 64 * MIB,
                            allowed_host='server-44', allowed_root='/')
        standalone.refresh()
        with patch.object(cloud_index, 'prefetched_shards', record_prefetch):
            old_result = standalone.query('prefetch concurrency fixture',
                                          authorized_roots=['/private'])
        self.assertEqual(len(old_result['results']), 1)
        self.assertEqual(seen, [2, 6])
        standalone.clear()
        reader.clear()
        self.assertEqual(shared.used, 0)

    def test_regex_absolute_deadline_is_shared_across_cloud_fetch(self):
        import cloud_index
        import time
        from unittest.mock import patch
        pub = Publisher(self.store, self.root, 64 * 1024)
        pub.publish('slow-regex', make_stream('server-44', [
            ('/private/history/slow-regex.txt', 'needle middle pattern')]))
        reader = Reader(self.store, self.root, 'slow-regex', 64 * MIB,
                        allowed_host='server-44', allowed_root='/')
        reader.refresh()
        real_get = self.store.get
        real_regex = cloud_index.regex_match_positions
        seen = []

        def delayed_get(*args, **kwargs):
            time.sleep(0.12)
            return real_get(*args, **kwargs)

        def record_regex(compiled, hard, body, absolute_deadline):
            seen.append((time.monotonic(), absolute_deadline))
            return real_regex(compiled, hard, body, absolute_deadline)

        began = time.monotonic()
        with patch.object(self.store, 'get', side_effect=delayed_get), patch.object(
                cloud_index, 'regex_match_positions', side_effect=record_regex):
            result = reader.query('needle.*pattern', regex=True,
                                  authorized_roots=['/private'], deadline_seconds=1.0)
        self.assertFalse(result['partial'], result['warnings'])
        self.assertEqual(len(result['results']), 1)
        self.assertEqual(len(seen), 1)
        self.assertGreaterEqual(seen[0][0] - began, 0.11)
        # The regex budget cannot restart after downloading a slow shard.
        self.assertLessEqual(seen[0][1], began + 1.04)

    def test_cloud_prefetch_scheduler_does_not_submit_more_than_worker_limit(self):
        import cloud_index
        import time
        from unittest.mock import patch

        class TrackingExecutor:
            instances = []
            def __init__(self, max_workers, thread_name_prefix=None):
                self.max_workers = max_workers
                self.pending = 0
                self.peak = 0
                self.instances.append(self)
            def submit(self, fn, *args):
                self.pending += 1
                self.peak = max(self.peak, self.pending)
                owner = self
                class Future:
                    def result(self, timeout=None):
                        owner.pending -= 1
                        return fn(*args)
                return Future()
            def shutdown(self, wait=False, cancel_futures=False):
                pass

        class FakeStore:
            def get(self, key, maximum):
                return key.encode('ascii')

        parts = [{'sha256': '%064x' % i,
                  'key': 'synthetic-%d' % i, 'packed_bytes': 20}
                 for i in range(7)]
        # A workers=1 reader must keep advancing after its first result:
        # the former scheduler silently stopped after one fetched part.
        for workers in (1, 2, 3):
            with self.subTest(workers=workers):
                with patch.object(cloud_index, 'ThreadPoolExecutor', TrackingExecutor):
                    rows = list(cloud_index.prefetched_shards(
                        FakeStore(), parts, {}, time.monotonic(), 5, workers=workers))
                self.assertEqual(len(rows), len(parts))
                self.assertEqual([value for _, value in rows],
                                 [b'synthetic-%d' % i for i in range(7)])
                executor = TrackingExecutor.instances[-1]
                self.assertEqual(executor.max_workers, workers)
                self.assertLessEqual(executor.peak, workers)
                self.assertEqual(executor.pending, 0)

    def test_legacy_small_scope_cache_keeps_existing_disjoint_quotas(self):
        # No manifests are fetched: only the constructor cache topology.
        scopes = [
            {'scope': 'old-one', 'source_host': 'server-44', 'source_root': '/a'},
            {'scope': 'old-two', 'source_host': 'server-44', 'source_root': '/b'}]
        reader = MultiReader(self.store, self.root, scopes, cache_mib=128)
        self.assertIsNone(reader.shared_cache)
        self.assertEqual([r.limit for r in reader.readers], [64*MIB, 64*MIB])


if __name__ == '__main__':
    unittest.main()
