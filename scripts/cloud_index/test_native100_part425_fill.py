"""Explicit read-only source425 packing/SQLite canary, never a production build.

Requires GREPMESH_REAL425_FILL_CANARY=1. Reads only the previously verified
local cache, never the cloud or the rejected partial oversized archive. Before:
exact immutable eecb1d packing method with a counted flush, not an FTS build.
After: actual filled packing and SQLite FTS with full path/body comparisons.
Only aggregate counts/hashes/resources are emitted; all scratch is private RAM.
"""
import fcntl
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import resource
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import types
import unittest
import zlib

import native100_bounded_resume as current
from cloud_index import digest, json_bytes, MIB

BASE='eecb1d280dec42ef1c76d124b83ba93da81c2c57'
BASE_SOURCE_SHA='a23c2787a0953db0f64fac151737a7193ac37b6ae10f78f1589f073bf5ac27f7'
CACHE=Path('/run/user/1000/grepmesh-native100-private-build/verified-source-cache/000425-2afeae188593ca70e437df139a375ce8071c93eda504a0e9d1ebc8ef93db36dd.jsonl.gz')
SHA='2afeae188593ca70e437df139a375ce8071c93eda504a0e9d1ebc8ef93db36dd'
SIZE=5746993
ROWS=110282
SNAP='20261031T233108Z-5732b979'
EVIDENCE={}


def source425():
    fd=os.open(CACHE,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as f:
        st=os.fstat(f.fileno())
        if (not stat.S_ISREG(st.st_mode) or st.st_uid!=os.getuid() or
                st.st_nlink!=1 or stat.S_IMODE(st.st_mode)!=0o600 or st.st_size!=SIZE):
            raise RuntimeError('Source425 cache admission changed')
        h=hashlib.sha256()
        while True:
            b=f.read(256*1024)
            if not b:break
            h.update(b)
        if h.hexdigest()!=SHA:raise ValueError('Source425 immutable SHA mismatch')
        f.seek(0);n=0;last=0
        with gzip.GzipFile(fileobj=f,mode='rb') as src:
            while True:
                line=src.readline(64*MIB+1)
                if not line:break
                if len(line)>64*MIB or not line.endswith(b'\n'):
                    raise ValueError('Source425 frame bound failed')
                row=json.loads(line)
                if (not isinstance(row,list) or len(row)!=3 or type(row[0])is not int or
                        row[0]<=last or not current._path(row[1]) or not isinstance(row[2],str)):
                    raise ValueError('Source425 source record invalid')
                last=row[0];n+=1
                yield row[1],row[2]
        if n!=ROWS or last!=3266710:raise ValueError('Source425 row/EOF accounting failed')
        end=os.fstat(f.fileno())
        if (end.st_ino,end.st_size,end.st_mtime_ns)!=(st.st_ino,st.st_size,st.st_mtime_ns):
            raise RuntimeError('Read-only cache changed during canary')


class EchoSink:
    """One-object readback echo, NOT a durable cloud or runtime adapter."""
    def __init__(self):
        self.known={};self.last=None;self.puts=0;self.gets=0
    def exists(self,key):return key in self.known
    def put(self,key,body):
        self.last=(key,body);self.puts+=1
        self.known[key]=(len(body),hashlib.md5(body).hexdigest())
    def get(self,key,limit):
        if self.last is None or self.last[0]!=key or len(self.last[1])>limit:
            raise RuntimeError('Canary echo cannot simulate historical cloud reads')
        self.gets+=1;return self.last[1]


class VerifiedPublisher(current.BoundedPublisher):
    def __init__(self,*args):
        super().__init__(*args)
        self.input_docs=None;self.rows=0;self.logical=[hashlib.sha256() for _ in range(16)]
        self.partition_rows=[0]*16;self.real_shards=0
    def shard(self,docs):
        old=self.input_docs;self.input_docs=docs
        try:return super().shard(docs)
        finally:self.input_docs=old
    def _save_sqlite(self,kind,count,**extra):
        # Existing immutable scratch is already closed and quick_check verified.
        # Compare EVERY stored path and decompressed full body to admitted input.
        if kind:raise RuntimeError('Packing canary does not publish path catalogs')
        c=sqlite3.connect('file:'+str(self.scratch.index)+'?mode=ro&immutable=1',uri=True)
        try:
            n=0
            for n,(path,sha,compressed) in enumerate(c.execute(
                'SELECT d.path,d.sha,b.compressed FROM docs d JOIN bodies b ON d.sha=b.sha ORDER BY d.id'),1):
                if n>len(self.input_docs):raise ValueError('Unexpected SQLite row')
                expected=self.input_docs[n-1];body=zlib.decompress(compressed)
                if (path!=expected['path'] or sha!=expected['sha256'] or
                        body!=expected['body'].encode('utf8') or digest(body)!=sha):
                    raise ValueError('Full path/body parity failed')
                pid=hashlib.sha256(path.encode('utf8')).digest()[0]%16
                self.logical[pid].update(json_bytes([path,sha]));self.partition_rows[pid]+=1
            if n!=count or n!=len(self.input_docs):raise ValueError('SQLite row count mismatch')
        finally:c.close()
        self.rows+=n;self.real_shards+=1
        return super()._save_sqlite(kind,count,**extra)


def load_baseline():
    r=subprocess.run(['git','show',BASE+':scripts/cloud_index/native100_bounded_resume.py'],
        cwd=Path(__file__).resolve().parents[2],capture_output=True,timeout=10,check=True)
    if digest(r.stdout)!=BASE_SOURCE_SHA:raise ValueError('Before source hash mismatch')
    module=types.ModuleType('fill_immutable_baseline');sys.modules[module.__name__]=module
    exec(compile(r.stdout,'immutable-eecb1d/native100_bounded_resume.py','exec'),module.__dict__)
    return module


def worker(mode,root):
    socket.socket.connect=lambda *a,**kw:(_ for _ in ()).throw(AssertionError('network forbidden'))
    start=time.monotonic();root=Path(root)
    # Descriptor envelope exists ONLY to instantiate the packing components.
    # No source checkpoint is initialized, restored, written, or advanced here.
    key='archives/server-100/rolling/'+SNAP+'/parts/000001-'+SHA+'.jsonl.gz'
    progress={'finished':True,'consistent_snapshot':False,'safe_for_deletion':False,
        'rows':ROWS,'last_rowid':3266710,'chunks':[{'key':key,'sha256':SHA,
        'compressed_bytes':SIZE,'rows':ROWS,'last_rowid':3266710}]}
    audit={'snapshot':SNAP,'historical_documents':ROWS,'cloud_parts_fully_replayed':1,
        'duplicate_paths':0,'malformed_paths':0,'consistent_snapshot':False,'safe_for_deletion':False}
    mod=load_baseline() if mode=='before' else current
    sink=EchoSink();e=mod.BoundedBuilder(sink,root,SNAP,json_bytes(progress),progress,audit)
    flushes=[];actual=[hashlib.sha256() for _ in range(16)]
    expected=[hashlib.sha256() for _ in range(16)];counts=[0]*16
    observed=[0]*16
    if mode=='before':
        def counted_flush(pid):
            docs=e.pending[pid]
            if not docs:return
            flushes.append({'pid':pid,'documents':len(docs),'body_bytes':sum(len(d['body'].encode()) for d in docs)})
            for d in docs:actual[pid].update(json_bytes([d['path'],d['sha256']]));observed[pid]+=1
            e.pending_count-=len(docs);e.pending[pid]=[];e.pending_sizes[pid]=0
        e._flush_docs=counted_flush
    else:
        e.publisher=VerifiedPublisher(e.io,e.scratch,e.limits)
        def record(r):
            if r[0]!='shard':raise AssertionError('Only packing shard descriptors expected')
            pid,d=r[1:]
            flushes.append({'pid':pid,'documents':d['documents'],'raw_bytes':d['raw_bytes'],'packed_bytes':d['packed_bytes']})
        e._append=record
    n=body_bytes=0;max_pending_count=max_pending_bytes=0
    for path,body in source425():
        size=len(body.encode('utf8'));pid=hashlib.sha256(path.encode('utf8')).digest()[0]%16
        if size>MIB:raise ValueError('Unexpected source425 oversized row')
        expected[pid].update(json_bytes([path,digest(body.encode('utf8'))]));counts[pid]+=1
        e._add_doc(pid,path,body,size);n+=1;body_bytes+=size
        if mode=='before':
            max_pending_count=max(max_pending_count,e.pending_count)
            max_pending_bytes=max(max_pending_bytes,sum(e.pending_sizes))
    for pid in range(16):e._flush_docs(pid)
    if mode=='after':
        actual=e.publisher.logical;observed=e.publisher.partition_rows
        if e.publisher.rows!=ROWS:raise ValueError('Full SQLite verification incomplete')
        max_pending_count=e.peak_pending_count;max_pending_bytes=e.peak_pending_bytes
    if (counts!=observed or any(a.digest()!=b.digest() for a,b in zip(actual,expected)) or
            n!=ROWS or sum(x['documents'] for x in flushes)!=ROWS):
        raise ValueError('No-loss/no-duplicate/routing parity failed')
    per=[sum(x['pid']==pid for x in flushes) for pid in range(16)]
    out={'mode':mode,'rows':n,'body_bytes':body_bytes,'flushes_or_actual_shards':len(flushes),
        'per_partition':per,'documents_per_partition':counts,
        'min_documents':min(x['documents'] for x in flushes),'max_documents':max(x['documents'] for x in flushes),
        'mean_documents':n/len(flushes),'mean_fill_fraction':n/(len(flushes)*512),
        'full_512_shards':sum(x['documents']==512 for x in flushes),
        'peak_pending_documents':max_pending_count,'peak_pending_body_bytes':max_pending_bytes,
        'peak_tmpfs_scratch_bytes':e.scratch.peak_bytes,
        'rss_peak_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        'cpu_seconds':resource.getrusage(resource.RUSAGE_SELF).ru_utime+resource.getrusage(resource.RUSAGE_SELF).ru_stime,
        'elapsed_seconds':time.monotonic()-start,'worker_pid':os.getpid(),
        'exact_ordered_path_body_hashes_match':True,'path_hash_routing_preserved':True,
        'all_full_bodies_verified_in_sqlite':mode=='after','real_FTS_built':mode=='after',
        'cloud_calls':0,'user_content_emitted':False,'cache_modified':False,
        'part425_sha256':SHA,'limits':mod.asdict(e.limits),
        'source_metadata_envelope':'packing-only fixture; no source checkpoint or catalog publication'}
    if mode=='after':
        out.update({'fill_flushes':e.fill_flushes,'peak_fill_tmpfs_bytes':e.fill.peak_bytes,
            'max_raw_shard_bytes':max(x['raw_bytes'] for x in flushes),
            'total_raw_shard_bytes':sum(x['raw_bytes'] for x in flushes),
            'total_packed_shard_bytes':sum(x['packed_bytes'] for x in flushes),
            'local_queue_empty_after_final_flush':not e.fill.used and not any(e.fill.counts)})
    print(json.dumps(out),flush=True)


@unittest.skipUnless(os.environ.get('GREPMESH_REAL425_FILL_CANARY')=='1','explicit verified-local-cache canary only')
class ActualSource425FillTests(unittest.TestCase):
    def test_exact_before_after_from_verified_local_cache(self):
        env=dict(os.environ,XDG_RUNTIME_DIR='/run/user/1000',DBUS_SESSION_BUS_ADDRESS='unix:path=/run/user/1000/bus')
        def inactive():
            for name in ('grepmesh-native100-private-search-resume-20261101.service',
                         'grepmesh-remote44-oversize-offload-v3-20261103.service'):
                r=subprocess.run(['systemctl','--user','show',name,'-p','ActiveState','-p','MainPID','-p','ControlPID'],
                    env=env,text=True,capture_output=True,timeout=10,check=True)
                state=dict(x.split('=',1) for x in r.stdout.splitlines() if '=' in x)
                if (state.get('ActiveState')not in ('failed','inactive') or
                        state.get('MainPID')!='0' or state.get('ControlPID')!='0'):
                    raise RuntimeError('Original builder/transfer active or unknown')
        inactive()
        lock=Path('/run/user/1000/grepmesh-native100-private-build/private-build.lock')
        fd=os.open(lock,os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK)
        try:
            st=os.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_uid!=os.getuid() or st.st_nlink!=1:
                raise RuntimeError('Original lock provenance changed')
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);inactive()
            with tempfile.TemporaryDirectory(dir='/run/user/1000',prefix='grepmesh-source425-fill-') as tmp:
                results={}
                for mode in ('before','after'):
                    root=Path(tmp)/mode;root.mkdir(mode=0o700)
                    r=subprocess.run([sys.executable,'-B',str(Path(__file__).resolve()),'--worker',mode,str(root)],
                        env=env,text=True,capture_output=True,timeout=720)
                    self.assertEqual(r.returncode,0,r.stderr[-2000:])
                    results[mode]=json.loads(r.stdout.strip())
                before,after=results['before'],results['after']
                self.assertEqual(before['flushes_or_actual_shards'],1833)
                self.assertEqual(before['per_partition'],[116,116,112,117,113,114,116,114,114,115,116,112,115,114,114,115])
                self.assertEqual(after['flushes_or_actual_shards'],224)
                self.assertEqual(after['per_partition'],[14]*16)
                self.assertEqual(after['full_512_shards'],208)
                self.assertEqual(before['documents_per_partition'],after['documents_per_partition'])
                self.assertEqual(before['body_bytes'],after['body_bytes'])
                self.assertEqual(after['rows'],ROWS)
                self.assertLessEqual(after['peak_pending_documents'],512)
                self.assertLessEqual(after['peak_pending_body_bytes'],8*MIB)
                self.assertLessEqual(after['peak_tmpfs_scratch_bytes'],64*MIB+1024)
                self.assertLessEqual(after['max_raw_shard_bytes'],64*MIB)
                self.assertLess(after['rss_peak_kib'],256*1024)
                self.assertTrue(after['all_full_bodies_verified_in_sqlite'])
                EVIDENCE.update({'before':before,'after':after,
                    'reduction_fraction':1-after['flushes_or_actual_shards']/before['flushes_or_actual_shards'],
                    'original_lock_exclusive':True,'real_cloud_calls':0,'production_builder_started':False,
                    'full_corpus_admission_accepted':False})
        finally:os.close(fd)


def tearDownModule():
    path=os.environ.get('GREPMESH_FILL425_EVIDENCE')
    if path:
        group=next((l.split(':',2)[2] for l in Path('/proc/self/cgroup').read_text().splitlines() if l.startswith('0::')),None)
        if group:
            c=Path('/sys/fs/cgroup')/group.lstrip('/')
            EVIDENCE['effective_cgroup']={n:(c/n).read_text().strip() for n in
                ('memory.high','memory.max','memory.peak','memory.events','memory.swap.max','cpu.max','pids.max') if (c/n).exists()}
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'w') as f:json.dump(EVIDENCE,f,indent=2)


if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--worker':worker(sys.argv[2],sys.argv[3])
    else:unittest.main()
