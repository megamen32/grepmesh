"""Fail-closed native100 cloud checkpoint synthetic tests: never network/cloud runtime."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import tracemalloc
import unittest

from cloud_index import MemoryStore, Publisher, digest, json_bytes
from native100_part_checkpoint import DurableLedger, BatchSpool


SHA = hashlib.sha256(b'pinned-443parts-source-checkpoint').hexdigest()
SNAP = '20261031T233108Z-5732b979'


class PartitionFixture:
    def __init__(self):
        self.paths = []
        self.docs = []
        self.pending_bytes = 0
        self.count = 0
        self.parts = []
        self.logical = hashlib.sha256()

    def add(self, path):
        sha = digest(path.encode())
        self.paths.append((path, 0, 0, sha))
        self.count += 1
        self.logical.update(json_bytes([path, sha]))
        return sha


def shard(i, count):
    keysha = digest(f'rawshard-{i}-{count}'.encode())
    return {'key':'objects/'+keysha+'.sqlite.gz',
            'sha256':keysha, 'packed_sha256':digest(f'packed-{i}-{count}'.encode()),
            'packed_bytes':124,'documents':count}


def state(parts, ordinal, *, oversize=0, first_samples=None):
    c = sum(x.count for x in parts)
    return {'parts_done':ordinal,'source_rows':c+oversize,
            'oversize':oversize,'raw_body_bytes':c*18,
            'last_rowid':c+oversize, 'first_samples':first_samples or {},
            'partitions':[{'count':p.count,
                           'logical_sha256':p.logical.hexdigest(),
                           'parts':p.parts} for p in parts],
            'consistent_snapshot':False,'safe_for_deletion':False}


class DurableCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir='/run/user/1000',prefix='grepmesh-checkpoint-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.store = MemoryStore()
        self.ledger = DurableLedger(self.store,SHA,SNAP)
        self.ledger.init()

    def prepare_first(self):
        partitions = [PartitionFixture() for _ in range(16)]
        batch=BatchSpool(self.root, 16)
        for i,path in [(0,'/home/a'),(1,'/home/b'),(1,'/home/c')]:
            sha=partitions[i].add(path)
            batch.append(i,path,0,0,sha)
        for i,p in enumerate(partitions):
            if p.count:p.parts.append(shard(i,p.count))
        s=state(partitions,10)
        self.ledger.commit(batch,s)
        return partitions,s

    def fresh(self):
        return DurableLedger(self.store,SHA,SNAP)

    def test_kill_resume_after_committed_batch_no_original_source_needed(self):
        original,s=self.prepare_first()
        # Simulate kill after uncommitted next batch has generated data:
        partial=BatchSpool(self.root,16)
        partial.append(0,'/home/uncommitted',0,0,digest(b'uncommitted'))
        # Intentionally abandon without checkpoint; independent new process:
        p=[PartitionFixture() for _ in range(16)]
        restored=self.fresh().restore(p)
        self.assertEqual(restored['parts_done'],10)
        self.assertEqual(restored['source_rows'],3)
        self.assertEqual(p[0].paths,[('/home/a',0,0,digest(b'/home/a'))])
        self.assertEqual([r[0] for r in p[1].paths],['/home/b','/home/c'])
        self.assertNotIn('/home/uncommitted',[r[0] for x in p for r in x.paths])
        partial.cleanup()

    def test_resume_append_boundary_then_resume_exactly_once(self):
        self.prepare_first()
        recovered=self.fresh()
        p=[PartitionFixture() for _ in range(16)]
        recovered.restore(p)
        b=BatchSpool(self.root,16)
        sha=p[0].add('/home/d');b.append(0,'/home/d',0,0,sha)
        p[0].parts.append(shard(0,1))
        recovered.commit(b,state(p,20))
        p2=[PartitionFixture() for _ in range(16)]
        self.fresh().restore(p2)
        self.assertEqual([x[0] for x in p2[0].paths],['/home/a','/home/d'])
        self.assertEqual(sum(x.count for x in p2),4)
        self.assertEqual(sum(z['documents'] for x in p2 for z in x.parts),4)

    def test_reject_repeat_commit_without_duplicate_records(self):
        p,s=self.prepare_first()
        b=BatchSpool(self.root,16)
        b.append(0,'/home/a',0,0,digest(b'/home/a'))
        with self.assertRaisesRegex(ValueError,'Non-monotonic'):
            self.ledger.commit(b,s)
        b.cleanup()
        self.assertEqual(self.fresh().restore([PartitionFixture() for _ in range(16)])['source_rows'],3)

    def test_fail_closed_on_truncated_head(self):
        self.prepare_first()
        self.store.objects[self.ledger.head]=b'{"format":1'
        with self.assertRaises((ValueError,TypeError,json.JSONDecodeError)):
            self.fresh().restore([PartitionFixture() for _ in range(16)])

    def test_fail_closed_on_corrupted_manifest(self):
        self.prepare_first()
        head=json.loads(self.store.objects[self.ledger.head])
        self.store.objects[head['manifest']]=b'{"truncated":true}'
        with self.assertRaisesRegex(ValueError,'SHA'):
            self.fresh().restore([PartitionFixture() for _ in range(16)])

    def test_fail_closed_on_missing_segment(self):
        self.prepare_first()
        self.store.objects.pop(self.ledger.segments[0]['key'])
        with self.assertRaises(KeyError):
            self.fresh().restore([PartitionFixture() for _ in range(16)])

    def test_fail_closed_on_corrupt_or_incomplete_gzip(self):
        self.prepare_first()
        key=self.ledger.segments[0]['key']
        self.store.objects[key]=self.store.objects[key][:-5]
        with self.assertRaises(ValueError):
            self.fresh().restore([PartitionFixture() for _ in range(16)])

    def test_bind_source_sha_prevents_foreign_checkpoint(self):
        self.prepare_first()
        foreign=DurableLedger(self.store,digest(b'different'),SNAP)
        with self.assertRaisesRegex(ValueError,'head'):
            foreign.restore([PartitionFixture() for _ in range(16)])

    def test_unknown_prior_checkpoint_cannot_be_started_fresh(self):
        self.prepare_first()
        with self.assertRaisesRegex(RuntimeError,'Existing checkpoint'):
            self.fresh().init()

    def test_reject_gap_in_source_part_cursor(self):
        p,s=self.prepare_first()
        b=BatchSpool(self.root,16)
        with self.assertRaisesRegex(ValueError,'one exact pinned batch'):
            self.ledger.commit(b,state(p,30))
        b.cleanup()

    def test_abort_before_head_change_keeps_predecessor(self):
        self.prepare_first()
        oldhead=self.store.objects[self.ledger.head]
        p=[PartitionFixture() for _ in range(16)]
        recovered=self.fresh();recovered.restore(p)
        b=BatchSpool(self.root,16)
        sha=p[1].add('/home/next');b.append(1,'/home/next',0,0,sha)
        p[1].parts.append(shard(1,1))
        original_put=self.store.put
        def fail_at_head(k,v):
            if k.endswith('/head.json'):raise OSError('simulated crash before head update')
            original_put(k,v)
        self.store.put=fail_at_head
        with self.assertRaises(OSError):
            recovered.commit(b,state(p,20))
        self.store.put=original_put
        self.assertEqual(self.store.objects[self.ledger.head],oldhead)
        self.assertEqual(self.fresh().restore([PartitionFixture() for _ in range(16)])['parts_done'],10)
        b.cleanup()

    def test_head_update_uncertain_is_reconciled_not_replayed(self):
        self.prepare_first()
        p=[PartitionFixture() for _ in range(16)]
        recovered=self.fresh();recovered.restore(p)
        b=BatchSpool(self.root,16)
        sha=p[0].add('/home/next');b.append(0,'/home/next',0,0,sha)
        p[0].parts.append(shard(0,1))
        original_put=self.store.put
        def fail_after_head(k,v):
            original_put(k,v)
            if k.endswith('/head.json'):raise OSError('simulated ACK loss')
        self.store.put=fail_after_head
        with self.assertRaises(OSError):
            recovered.commit(b,state(p,20))
        self.store.put=original_put
        loaded=self.fresh().restore([PartitionFixture() for _ in range(16)])
        self.assertEqual(loaded['parts_done'],20)
        b.cleanup()

    def test_bounded_metadata_spooling_20k(self):
        batch=BatchSpool(self.root,16)
        sha=digest(b'constant body')
        tracemalloc.start()
        for i in range(20000):
            batch.append(i%16,'/home/testing/%09d'%i,0,0,sha)
        _,peak=tracemalloc.get_traced_memory()
        tracemalloc.stop()
        body=batch.seal()
        self.assertEqual(batch.count,20000)
        self.assertLess(peak,5*1024*1024)
        self.assertLess(len(body),2*1024*1024)
        batch.cleanup()

    def test_path_index_identity_after_checkpoint_restore(self):
        p,s=self.prepare_first()
        q=[PartitionFixture() for _ in range(16)]
        self.fresh().restore(q)
        from cloud_index import Publisher
        pub=Publisher(self.store,self.root)
        for i in (0,1):
            original=pub.path_index(p[i].paths)
            after=pub.path_index(q[i].paths)
            self.assertEqual(original['sha256'],after['sha256'])
            self.assertEqual(original['documents'],after['documents'])


if __name__=='__main__':
    unittest.main()
