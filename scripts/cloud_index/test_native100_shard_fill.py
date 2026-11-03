"""Packing, shared tmpfs budget and marked-queue integrity; synthetic only."""
import gzip
import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from dataclasses import asdict

from cloud_index import MIB, document
from native100_bounded_resume import Limits
from native100_shard_fill import FilledQueues, queue_name, PACKING_POLICY
from test_native100_bounded_resume import engine, make_fixture, verify_catalog


def route(path):
    return hashlib.sha256(path.encode('utf8')).digest()[0] % 16


class ShardFillTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir='/run/user/1000',prefix='grepmesh-fill-test-')
        self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        make_fixture(self.root,parts=4,rows=4)
        self.e,_,_=engine(self.root)

    def queue_one(self):
        path='/home/fixture/one';pid=route(path);body='data'
        frame=self.e.fill.encode(document(path,body))
        self.e.fill.append(pid,frame,len(body))
        return pid,frame

    def packing_counter(self,e):
        stats=[]
        def consume(docs):
            ids={route(d['path']) for d in docs}
            self.assertEqual(len(ids),1)
            self.assertLessEqual(len(docs),e.limits.pending_documents)
            self.assertLessEqual(sum(len(d['body'].encode()) for d in docs),e.limits.pending_body_bytes)
            stats.append({'pid':next(iter(ids)),'count':len(docs),
                'digest':hashlib.sha256(''.join(d['path'] for d in docs).encode()).hexdigest()})
            return []  # policy-only test; real SQLite is covered by runner/part425 tests
        e.publisher.shard=consume
        return stats

    def test_hash_route_preserved_and_full_512_batches(self):
        e=self.e;stats=self.packing_counter(e);seen=[[] for _ in range(16)]
        for i in range(10000):
            path='/home/fixture/fill-%08d'%i;pid=route(path);body='small text'
            seen[pid].append(path);e._add_doc(pid,path,body,len(body))
            self.assertLessEqual(e.scratch.reserved_bytes,e.fill.cap)
            self.assertEqual(e.pending_count,0)
        for pid in range(16):e._flush_docs(pid)
        self.assertEqual(sum(s['count'] for s in stats),10000)
        self.assertEqual(len(stats),sum((len(p)+511)//512 for p in seen))
        self.assertGreaterEqual(sum(s['count']==512 for s in stats),16)
        for pid in range(16):
            records=[x for x in stats if x['pid']==pid]
            for n,x in enumerate(records):
                self.assertEqual(x['digest'],hashlib.sha256(''.join(seen[pid][n*512:(n+1)*512]).encode()).hexdigest())
        self.assertLessEqual(e.peak_pending_count,512)
        self.assertLessEqual(e.peak_pending_bytes,8*MIB)
        self.assertEqual(e.fill.used,0)
        self.assertEqual(list(e.scratch.root.iterdir()),[e.scratch.marker])

    def test_empty_bodies_fill_by_count_not_bytes(self):
        e=self.e;stats=self.packing_counter(e)
        for i in range(1500):
            p='/home/empty/%08d'%i;e._add_doc(route(p),p,'',0)
        for pid in range(16):e._flush_docs(pid)
        self.assertEqual(sum(x['count'] for x in stats),1500)
        self.assertEqual(e.peak_pending_bytes,0)
        self.assertLessEqual(e.peak_pending_count,512)

    def test_same_reader_and_memory_limits(self):
        limits=Limits()
        self.assertEqual(limits.pending_body_bytes,8*MIB)
        self.assertEqual(limits.pending_documents,512)
        self.assertEqual(limits.max_sqlite_bytes,64*MIB)
        self.assertEqual(limits.max_shards_per_partition,500)
        self.assertEqual(limits.max_manifest_bytes,16*MIB)
        self.assertEqual(limits.page_raw_bytes,256*1024)
        self.assertEqual(limits.batch_parts,10)
        self.assertEqual(self.e.fill.cap,limits.max_sqlite_bytes//4)

    def test_sqlite_and_compressed_queues_share_one_budget(self):
        pid,_=self.queue_one();e=self.e
        c=e.scratch.connect(e.limits.max_sqlite_bytes)
        try:
            cap=c.execute('PRAGMA max_page_count').fetchone()[0]*4096
            self.assertLessEqual(cap+e.fill.used,e.limits.max_sqlite_bytes)
            c.execute('CREATE TABLE check_budget(x)');c.commit()
            e.scratch.measure_total(e.limits.max_sqlite_bytes)
            self.assertLessEqual(e.scratch.peak_bytes,e.limits.max_sqlite_bytes+1024)
        finally:c.close();e.scratch.discard_index()
        self.assertEqual(len(e.fill.take(pid)),1)

    def test_partial_queue_cannot_advance_checkpoint(self):
        self.queue_one()
        with self.assertRaisesRegex(RuntimeError,'unflushed'):
            self.e._publish_head(0)

    def test_partial_queue_is_not_final_catalog(self):
        self.e.cursor=len(self.e.parts);self.queue_one()
        with self.assertRaisesRegex(RuntimeError,'Incomplete'):
            self.e.finalize()

    def test_frame_corruption_fails_without_removing_queue(self):
        pid,_=self.queue_one();p=self.e.scratch.root/queue_name(pid)
        raw=bytearray(p.read_bytes());raw[-1]^=1;p.write_bytes(raw)
        with self.assertRaises(Exception):self.e.fill.take(pid)
        self.assertTrue(p.exists());self.assertEqual(self.e.fill.counts[pid],1)

    def test_frame_truncation_fails_without_removing_queue(self):
        pid,_=self.queue_one();p=self.e.scratch.root/queue_name(pid)
        p.write_bytes(p.read_bytes()[:-1])
        with self.assertRaises(RuntimeError):self.e.fill.take(pid)
        self.assertTrue(p.exists())

    def test_owned_marked_queue_reconciles_after_process_loss(self):
        pid,_=self.queue_one();p=self.e.scratch.root/queue_name(pid)
        other=self.root/'unrelated.partial';other.write_bytes(b'keep')
        q,_,_=engine(self.root)
        self.assertFalse(p.exists());self.assertEqual(other.read_bytes(),b'keep')
        self.assertEqual(q.fill.used,0)

    def test_unknown_artifact_prevents_all_cleanup(self):
        pid,_=self.queue_one();p=self.e.scratch.root/queue_name(pid)
        other=self.e.scratch.root/'foreign.partial';other.write_bytes(b'keep')
        with self.assertRaisesRegex(RuntimeError,'Unknown'):engine(self.root)
        self.assertTrue(p.exists());self.assertTrue(other.exists())

    def test_queue_symlink_not_followed_or_removed(self):
        pid,_=self.queue_one();p=self.e.scratch.root/queue_name(pid)
        outside=self.root/'keep';outside.write_bytes(b'keep');p.unlink();p.symlink_to(outside)
        with self.assertRaises(OSError):engine(self.root)
        self.assertTrue(p.is_symlink());self.assertEqual(outside.read_bytes(),b'keep')

    def test_queue_public_mode_fail_closed(self):
        pid,_=self.queue_one();p=self.e.scratch.root/queue_name(pid);p.chmod(0o644)
        with self.assertRaises(RuntimeError):self.e.fill.take(pid)
        self.assertTrue(p.exists())

    def test_queue_hardlink_fail_closed(self):
        pid,_=self.queue_one();p=self.e.scratch.root/queue_name(pid)
        os.link(p,self.root/'linked')
        with self.assertRaises(RuntimeError):engine(self.root)
        self.assertTrue(p.exists())

    def test_packing_policy_is_pinned_in_distinct_checkpoint_namespace(self):
        e,s,_=engine(self.root);e.build('init')
        import json
        commit=json.loads(s.get(e.commit_ref['key'],65536))
        self.assertEqual(commit['packing'],PACKING_POLICY)
        self.assertIn('/v3-fill1/',e.head_key)
        q,t,_=engine(self.root);q.build('resume')
        self.assertEqual(t.source_reads,[]);verify_catalog(self.root,q.finalize())

    def test_queue_pressure_never_raises_existing_total_budget(self):
        import random
        e,_,_=engine(self.root,limits=Limits(batch_parts=2,pending_documents=128,
            pending_body_bytes=256*1024,max_sqlite_bytes=4*MIB))
        stats=self.packing_counter(e);rng=random.Random(89)
        for i in range(100):
            path='/home/pressure/%08d'%i
            body=bytes(rng.randrange(33,127) for _ in range(60000)).decode('ascii')
            e._add_doc(route(path),path,body,len(body))
            self.assertLessEqual(e.fill.used,MIB)
            self.assertLessEqual(e.scratch.peak_bytes,4*MIB+1024)
        for pid in range(16):e._flush_docs(pid)
        self.assertEqual(sum(x['count'] for x in stats),100)
        self.assertGreater(e.fill_flushes['pressure'],0)
        self.assertLessEqual(e.peak_pending_bytes,256*1024)
        self.assertLessEqual(e.peak_pending_count,128)


if __name__=='__main__':unittest.main()
