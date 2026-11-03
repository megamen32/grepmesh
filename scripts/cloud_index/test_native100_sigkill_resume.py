"""Real SIGKILL of a synthetic child; new Python process, persistent fake cloud.
No existing service, builder PID or archive is stopped, read or restarted.
"""
import json
import os
from pathlib import Path
import resource
import selectors
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from native100_bounded_resume import Limits
from test_native100_bounded_resume import engine, make_fixture, verify_catalog

EVIDENCE=[]

def worker(base,mode,stop_stage,stop_cursor,finalize):
    # Tests cannot accidentally use a real transport.
    def forbidden(*args,**kwargs):raise AssertionError('network forbidden in synthetic worker')
    socket.socket.connect=forbidden
    base=Path(base);samples=[];holder={}
    def event(stage,info):
        if stage=='after_checkpoint':
            e=holder['engine']
            samples.append({'cursor':info['cursor'],'rss_peak_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                'scratch_current_bytes':sum(p.stat().st_size for p in e.scratch.root.iterdir()),
                'scratch_peak_bytes':e.scratch.peak_bytes,'pending_body_bytes':sum(e.pending_sizes),
                'pending_documents':e.pending_count,'metadata_records':len(e.records),
                'metadata_payload_bytes':e.record_bytes})
        if stage==stop_stage and info['cursor']==stop_cursor:
            print(json.dumps({'stopped_at':stage,'cursor':stop_cursor,'pid':os.getpid()}),flush=True)
            # Wait for the parent to SIGKILL THIS process, not an exception/fake restart.
            sys.stdin.buffer.read(1)
            raise AssertionError('Parent must SIGKILL, not release the barrier')
    e,store,_=engine(base,event);holder['engine']=e
    e.build(mode)
    result=e.finalize() if finalize else {'source_rows':e.rows,'source_parts':e.cursor,
        'peak_tmpfs_scratch_bytes':e.scratch.peak_bytes,'peak_pending_body_bytes':e.peak_pending_bytes,
        'peak_pending_documents':e.peak_pending_count,'peak_metadata_payload_bytes':e.peak_metadata_bytes}
    print(json.dumps({'pid':os.getpid(),'source_reads':store.source_reads,'result':result,
        'rss_peak_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'samples':samples}),flush=True)


class ActualSigkillResumeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir='/run/user/1000',prefix='grepmesh-real-sigkill-')
        self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        make_fixture(self.root,parts=6,rows=4)

    def command(self,mode,stage='',cursor=-1,finalize=True):
        return [sys.executable,'-B',str(Path(__file__).resolve()),'--worker',str(self.root),
                mode,stage,str(cursor),'1' if finalize else '0']

    def run_child(self,mode,finalize=True,timeout=90):
        r=subprocess.run(self.command(mode,finalize=finalize),text=True,capture_output=True,timeout=timeout)
        self.assertEqual(r.returncode,0,r.stderr[-4000:])
        return json.loads(r.stdout.strip().splitlines()[-1])

    def kill_at(self,stage,cursor):
        p=subprocess.Popen(self.command('init',stage,cursor),stdin=subprocess.PIPE,
                           stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(p.stdout,selectors.EVENT_READ)
                self.assertTrue(selector.select(45),'child did not reach requested commit barrier')
                line=p.stdout.readline()
            if not line:
                _,err=p.communicate(timeout=5);self.fail(err[-4000:])
            marker=json.loads(line)
            self.assertEqual(marker,{'stopped_at':stage,'cursor':cursor,'pid':p.pid})
            os.kill(p.pid,signal.SIGKILL)
            _,err=p.communicate(timeout=5)
            self.assertEqual(p.returncode,-signal.SIGKILL,err)
            return p.pid
        finally:
            if p.poll() is None:p.kill();p.wait(timeout=5)
            for stream in (p.stdin,p.stdout,p.stderr):
                if stream:stream.close()

    def prove(self,stage,confirmed):
        killed=self.kill_at(stage,4)
        old_objects={str(p.relative_to(self.root/'remote')) for p in (self.root/'remote').rglob('*') if p.is_file()}
        resumed=self.run_child('resume')
        self.assertNotEqual(killed,resumed['pid'])
        self.assertEqual(resumed['source_reads'],list(range(confirmed+1,7)))
        self.assertTrue(all(n>confirmed for n in resumed['source_reads']))
        check=verify_catalog(self.root,resumed['result'])
        self.assertEqual(check['exact_paths'],24);self.assertEqual(check['exact_bodies'],24)
        now={str(p.relative_to(self.root/'remote')) for p in (self.root/'remote').rglob('*') if p.is_file()}
        self.assertTrue(old_objects.issubset(now),'orphan/old immutable data was deleted')
        EVIDENCE.append({'test':self.id(),'stage':stage,'killed_pid':killed,'kill_returncode':-9,
            'resumed_pid':resumed['pid'],'confirmed_source_parts':confirmed,
            'resumed_source_reads':resumed['source_reads'],'exact_paths':24,'exact_bodies':24,
            'no_replay_confirmed':True,'immutable_keys_preserved':True,
            'peak_rss_kib':resumed['rss_peak_kib'],'peak_tmpfs_scratch_bytes':resumed['result']['peak_tmpfs_scratch_bytes']})

    def test_sigkill_after_shards_before_metadata(self):self.prove('after_shards',2)
    def test_sigkill_after_metadata_before_commit(self):self.prove('after_metadata',2)
    def test_sigkill_before_head_preserves_previous_batch(self):self.prove('before_head',2)
    def test_sigkill_after_head_write_before_ack_reconciles_without_replay(self):self.prove('after_head_write',4)
    def test_sigkill_after_confirmed_checkpoint_skips_confirmed_batches(self):self.prove('after_checkpoint',4)

    def test_new_process_finalization_is_idempotent(self):
        a=self.run_child('init');b=self.run_child('resume')
        self.assertNotEqual(a['pid'],b['pid']);self.assertEqual(b['source_reads'],[])
        self.assertEqual(a['result']['catalog'],b['result']['catalog'])
        verify_catalog(self.root,b['result'])
        EVIDENCE.append({'test':self.id(),'first_pid':a['pid'],'second_pid':b['pid'],
                         'resumed_source_reads':[],'same_catalog':True})

    def test_many_batches_total_worker_memory_and_tmpfs_do_not_grow_with_paths(self):
        # A distinct fixture; the remote folder represents external cloud, not
        # local builder cache. No expected full-body map is loaded by the worker.
        with tempfile.TemporaryDirectory(dir='/run/user/1000',prefix='grepmesh-scale-') as tmp:
            self.root=Path(tmp)
            limits=Limits(batch_parts=2,pending_body_bytes=256*1024,pending_documents=128,
                          page_raw_bytes=64*1024,max_sqlite_bytes=4*1024*1024)
            make_fixture(self.root,parts=60,rows=80,body_bytes=160,limits=limits)
            out=self.run_child('init',finalize=False,timeout=180)
            self.assertEqual(out['result']['source_rows'],4800)
            self.assertEqual(out['source_reads'],list(range(1,61)))
            samples=out['samples'];self.assertEqual(len(samples),31)
            self.assertLess(out['rss_peak_kib'],128*1024)
            early=next(s['rss_peak_kib'] for s in samples if s['cursor']==10)
            self.assertLessEqual(out['rss_peak_kib']-early,24*1024)
            self.assertLessEqual(out['result']['peak_tmpfs_scratch_bytes'],limits.max_sqlite_bytes+1024)
            self.assertLessEqual(out['result']['peak_pending_body_bytes'],limits.pending_body_bytes)
            self.assertLessEqual(out['result']['peak_pending_documents'],limits.pending_documents)
            self.assertLess(out['result']['peak_metadata_payload_bytes'],limits.page_raw_bytes)
            for s in samples:
                self.assertEqual(s['pending_body_bytes'],0);self.assertEqual(s['pending_documents'],0)
                self.assertEqual(s['metadata_records'],0);self.assertEqual(s['metadata_payload_bytes'],0)
                self.assertLess(s['scratch_current_bytes'],1024)
            EVIDENCE.append({'test':self.id(),'pid':out['pid'],'source_parts':60,'rows':4800,
                'batches':30,'worker_rss_peak_kib':out['rss_peak_kib'],
                'rss_growth_after_batch5_kib':out['rss_peak_kib']-early,
                'peak_tmpfs_scratch_bytes':out['result']['peak_tmpfs_scratch_bytes'],
                'peak_pending_body_bytes':out['result']['peak_pending_body_bytes'],
                'peak_pending_documents':out['result']['peak_pending_documents'],
                'peak_metadata_payload_bytes':out['result']['peak_metadata_payload_bytes'],
                'checkpoint_samples':samples,'remote_fixture_is_not_builder_cache':True})


def tearDownModule():
    dest=os.environ.get('GREPMESH_SYNTHETIC_EVIDENCE')
    if dest:
        group=next((line.split(':',2)[2] for line in Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::')),None)
        effective={}
        if group is not None:
            base=Path('/sys/fs/cgroup')/group.lstrip('/')
            for name in ('memory.high','memory.max','memory.swap.max','memory.peak','memory.events','cpu.max','pids.max','io.weight'):
                f=base/name
                if f.exists():effective[name]=f.read_text().strip()
        Path(dest).write_text(json.dumps({'evidence':EVIDENCE,'real_cloud_calls':0,
            'production_builder_touched':False,'test_only_process_kills':True,'effective_cgroup':effective},indent=2))

if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--worker':
        worker(sys.argv[2],sys.argv[3],sys.argv[4],int(sys.argv[5]),sys.argv[6]=='1')
    else:unittest.main()
