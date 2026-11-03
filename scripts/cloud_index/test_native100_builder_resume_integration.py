"""One killed builder and its next process: genuine runner, in-memory cloud only.

All source rows and cloud objects are synthetic and in RAM. The test exercises
the actual Publisher/SQLite path index and the opted-in native100 run() flow.
"""
import contextlib
import gzip
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import publish_native100_rolling_private as target
from cloud_index import MemoryStore, digest, json_bytes
from native100_part_checkpoint import DurableLedger


class CloudFixture(MemoryStore):
    def __init__(self):
        super().__init__()
        self.reused = 0
        self.fail_at_part = None
        self.source_reads = []
        self.known = None

    def bind_verified_source_checkpoint(self, raw, receipt):
        return json.loads(raw)

    def get(self, key, limit=64*1024*1024):
        if key.startswith(target.PREFIX+'/parts/'):
            ordinal = int(key.split('/')[-1][:6])
            self.source_reads.append(ordinal)
            if ordinal == self.fail_at_part:
                raise RuntimeError('injected source failure at boundary')
        return super().get(key,limit)


class Native100RunnerKillResumeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir='/run/user/1000',prefix='grepmesh-runner-kill-')
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.store=CloudFixture()
        parts=[]
        for n in range(1,21):
            path='/home/fixture/document-%02d.txt'%n
            text='synthetic source body for %02d'%n
            raw=(json.dumps([n,path,text])+'\n').encode()
            packed=gzip.compress(raw,mtime=0)
            sha=digest(packed)
            key=target.PREFIX+'/parts/%06d-%s.jsonl.gz'%(n,sha)
            self.store.objects[key]=packed
            parts.append({'key':key,'sha256':sha,'compressed_bytes':len(packed),
                          'rows':1,'last_rowid':n})
        progress={'finished':True,'consistent_snapshot':False,
                  'safe_for_deletion':False,'rows':20,'last_rowid':20,'chunks':parts}
        self.store.objects[target.PREFIX+'/progress.json']=json_bytes(progress)
        self.audit=self.root/'audit.json'
        self.audit.write_text(json.dumps({
            'snapshot':target.SNAPSHOT,'historical_documents':20,
            'cloud_parts_fully_replayed':20,'duplicate_paths':0,
            'malformed_paths':0,'safe_for_deletion':False,
            'consistent_snapshot':False,'oversize_documents':0,
            'total_text_body_bytes':sum(len(('synthetic source body for %02d'%i).encode()) for i in range(1,21))
        }))
        self.source_receipt=self.root/'source_verified.json'
        self.source_receipt.write_text('{}')
        self.out_receipt=self.root/'candidate.json'
        self.progress=self.root/'builder.progress.json'
        self.patches=[
            patch.object(target,'ROOT',self.root),
            patch.object(target,'AUDIT',self.audit),
            patch.object(target,'SOURCE_VERIFICATION',self.source_receipt),
            patch.object(target,'RECEIPT',self.out_receipt),
            patch.object(target,'PROGRESS',self.progress),
            patch.object(target,'ARCHIVED',20),
            patch.object(target,'PARTS',20),
            patch.object(target,'VerifiedImmutableReuseStore',return_value=self.store)
        ]
        for item in self.patches:
            item.start();self.addCleanup(item.stop)

    def run_builder(self,mode):
        with patch.dict(os.environ,{
                'GREPMESH_REUSE_VERIFIED_SHARDS':'1',
                'GREPMESH_DURABLE_BUILD_MODE':mode,
                'GREPMESH_REQUIRE_CACHED_SOURCE_PART':''
            }),contextlib.redirect_stdout(io.StringIO()):
            target.run()

    def test_actual_publisher_kill_then_resume_no_replay_no_duplicates(self):
        self.store.fail_at_part=11
        with self.assertRaisesRegex(RuntimeError,'injected'):
            self.run_builder('init')
        self.assertFalse(self.out_receipt.exists())
        first_segment_gets=list(self.store.source_reads)
        self.assertEqual(first_segment_gets,list(range(1,12)))
        ledger=DurableLedger(self.store,digest(self.store.objects[target.PREFIX+'/progress.json']),
                             target.SNAPSHOT)
        head=json.loads(self.store.objects[ledger.head])
        self.assertEqual(head['parts_done'],10)
        # Kill between batches: the next process has no in-memory partitions.
        self.store.fail_at_part=None
        self.store.source_reads.clear()
        self.run_builder('resume')
        self.assertEqual(self.store.source_reads,list(range(11,21)))
        receipt=json.loads(self.out_receipt.read_text())
        self.assertEqual(receipt['historical_source_documents'],20)
        self.assertEqual(receipt['indexed_documents'],20)
        self.assertFalse(receipt['safe_for_deletion'])
        self.assertFalse(receipt['production_route_switched'])
        self.assertEqual(sum(p['documents'] for p in receipt['partitions']),20)
        self.assertEqual(json.loads(self.store.objects[ledger.head])['parts_done'],20)
        # All 20 unique synthetic paths are indexed, with no lost/duplicated rows.
        self.assertEqual(len(receipt['sample_path_sha256']),len([
            p for p in receipt['partitions'] if p['documents']>0
        ]))


if __name__=='__main__':
    unittest.main()
