"""Opt-in bounded builder/checkpoint v3; no service or production routing changes.

Paths/shard descriptors live in immutable cloud metadata pages. A private head
advances only after object, metadata and commit readback. A fresh process
verifies the chain before reading ANY uncommitted original source part.
"""
from __future__ import annotations
import base64
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import zlib
from dataclasses import asdict, dataclass
from typing import Callable
from cloud_index import (BLOOM_BYTES, INDEX_LAYOUT, MAX_DOCUMENT, MIB,
                         bloom_for, digest, document, json_bytes, valid_key)

from native100_shard_fill import FilledQueues, PACKING_POLICY, queue_name

FORMAT = 3
HEX = re.compile(r'[a-f0-9]{64}\Z')
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_-]{0,90}\Z')

@dataclass(frozen=True)
class Limits:
    """Admission caps, not targets. Runtime cgroup limits never change."""
    batch_parts: int = 10
    pending_body_bytes: int = 8*MIB  # across all sixteen partitions
    pending_documents: int = 512
    page_raw_bytes: int = 256*1024
    page_records: int = 4096
    max_pages: int = 16384
    max_source_packed: int = 64*MIB
    max_source_line: int = 64*MIB
    max_sqlite_bytes: int = 64*MIB
    max_manifest_bytes: int = 16*MIB
    max_shards_per_partition: int = 500
    max_inventory_objects: int = 32768

    def validate(self):
        if any(type(v) is not int or v <= 0 for v in asdict(self).values()):
            raise ValueError('Invalid bounded builder limit')
        if (self.batch_parts > 10 or self.pending_body_bytes > 16*MIB
                or self.pending_documents > 512 or self.page_raw_bytes > MIB
                or self.max_pages > 16384 or self.max_source_packed > 64*MIB
                or self.max_source_line > 64*MIB or self.max_sqlite_bytes > 64*MIB
                or self.max_sqlite_bytes % 4096):
            raise ValueError('Limits exceed source-reviewed envelope')

def _sha(v):
    return isinstance(v,str) and HEX.fullmatch(v) is not None

def _integer(v, low=0, high=2**63-1):
    return type(v) is int and low <= v <= high

def _path(v):
    return (isinstance(v,str) and v.startswith('/')
            and len(v.encode('utf8')) <= 4096 and '\x00' not in v
            and '..' not in Path(v).parts and os.path.normpath(v) == v)

class Scratch:
    """One marked bounded SQLite scratch; never traverse old/partial archives."""
    def __init__(self, root, source_sha):
        root=Path(root); info=root.lstat()
        runtime=Path('/run/user')/str(os.getuid())
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o077 or info.st_dev != runtime.stat().st_dev):
            raise RuntimeError('Bounded scratch requires private tmpfs')
        self.root=root/'bounded-resume-v3-fill1-work'
        try:
            self.root.mkdir(mode=0o700); created=True
        except FileExistsError:
            created=False
        s=self.root.lstat()
        if (not stat.S_ISDIR(s.st_mode) or s.st_uid != os.getuid()
                or s.st_mode & 0o077 or s.st_dev != info.st_dev):
            raise RuntimeError('Unsafe bounded scratch directory')
        self.marker=self.root/'scope.json'
        expected=json_bytes({'format':FORMAT,'source_checkpoint_sha256':source_sha,'packing':PACKING_POLICY})
        if created:
            fd=os.open(self.marker,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
            with os.fdopen(fd,'wb') as f:
                f.write(expected); f.flush(); os.fsync(f.fileno())
        else:
            fd=self._open_regular(self.marker)
            with os.fdopen(fd,'rb') as f:
                if f.read(1024) != expected:
                    raise RuntimeError('Scratch provenance mismatch; no cleanup')
        entries=list(self.root.iterdir())
        allowed={'scope.json','index.sqlite'}|{queue_name(i) for i in range(16)}
        if any(p.name not in allowed for p in entries):
            raise RuntimeError('Unknown scratch artifact; no cleanup')
        # Validate EVERY inode first. Never partially clean an unknown directory.
        for p in entries:
            fd=self._open_regular(p); os.close(fd)
        self.index=self.root/'index.sqlite'
        for p in entries:
            if p != self.marker:p.unlink()  # only this marked reproducible scratch
        self.reserved_bytes=0
        self.peak_bytes=self.marker.stat().st_size

    @staticmethod
    def _open_regular(path):
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        try:Scratch.check_regular_fd(fd)
        except BaseException:
            os.close(fd);raise
        return fd

    @staticmethod
    def check_regular_fd(fd):
        s=os.fstat(fd)
        if (not stat.S_ISREG(s.st_mode) or s.st_uid != os.getuid()
                or s.st_mode & 0o077 or s.st_nlink != 1):
            raise RuntimeError('Unsafe scratch inode')

    def measure_total(self,cap):
        size=self.index.stat().st_size if self.index.exists() else 0
        if size+self.reserved_bytes > cap:
            raise RuntimeError('Combined fill/SQLite scratch exceeds original admission')
        total=size+self.reserved_bytes+self.marker.stat().st_size
        self.peak_bytes=max(self.peak_bytes,total)
        return total

    def connect(self,cap):
        cap=((cap-self.reserved_bytes)//4096)*4096
        if cap<16384:raise RuntimeError('Insufficient remaining SQLite scratch budget')
        fd=os.open(self.index,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
        os.close(fd); c=sqlite3.connect(self.index)
        c.executescript('PRAGMA page_size=4096; PRAGMA journal_mode=OFF; '
                       'PRAGMA temp_store=MEMORY; PRAGMA cache_size=-2048; PRAGMA mmap_size=0;')
        actual=c.execute('PRAGMA max_page_count=%d'%(cap//4096)).fetchone()[0]
        if actual*4096 > cap:
            c.close(); raise RuntimeError('SQLite scratch cap not applied')
        return c

    def measure(self,cap):
        size=self.index.stat().st_size
        if size > cap:raise RuntimeError('SQLite scratch exceeds admission')
        self.measure_total(cap)
        return size

    def discard_index(self):
        if self.index.exists():
            fd=self._open_regular(self.index);os.close(fd);self.index.unlink()

class ObjectIO:
    """Same authorized CloudStore; no credentials or alternate transport."""
    def __init__(self,store):self.store=store

    def exists(self,key):
        valid_key(key)
        if hasattr(self.store,'exists'):
            return self.store.exists(key)  # explicit synthetic/offline adapter
        cmd=self.store.command(['lsjson',self.store.prefix+'/'+key,'--stat'])
        r=subprocess.run(cmd,capture_output=True,timeout=30)
        if r.returncode == 0:
            info=json.loads(r.stdout)
            if not isinstance(info,dict) or info.get('IsDir') is not False:
                raise RuntimeError('Unexpected object metadata')
            return True
        message=r.stderr.decode('utf8','replace').lower()
        if r.returncode in (3,4) and any(x in message for x in
                ('directory not found','object not found','file not found')):
            return False
        raise RuntimeError('Object existence unresolved')

    def immutable(self,key,body):
        valid_key(key)
        if self.exists(key):
            if self.store.get(key,len(body)) != body:
                raise RuntimeError('Immutable object collision; refusing overwrite')
        else:
            # Uncertain put is NOT retried; the next process reconciles it.
            self.store.put(key,body)
        readback=self.store.get(key,len(body))
        if len(readback)!=len(body) or digest(readback)!=digest(body):
            raise RuntimeError('Immutable object readback failed')
        return {'key':key,'bytes':len(body),'sha256':digest(body)}

    def checked(self,ref,prefix,cap):
        if (not isinstance(ref,dict) or set(ref)!={'key','bytes','sha256'}
                or not _sha(ref.get('sha256')) or not _integer(ref.get('bytes'),1,cap)
                or not isinstance(ref.get('key'),str) or not ref['key'].startswith(prefix)):
            raise ValueError('Invalid immutable reference')
        key=valid_key(ref['key'])
        suffix='.json.gz' if prefix.endswith('/pages/') else '.json'
        if key!=prefix+ref['sha256']+suffix:
            raise ValueError('Non-content-addressed immutable reference')
        body=self.store.get(key,ref['bytes'])
        if len(body)!=ref['bytes'] or digest(body)!=ref['sha256']:
            raise ValueError('Immutable object missing, short or corrupt')
        return body

class BoundedPublisher:
    """Original SQLite schemas; enforce scratch cap BEFORE inserts, not after."""
    def __init__(self,io_store,scratch,limits):
        self.io=io_store;self.scratch=scratch;self.limits=limits

    def _save_sqlite(self,kind,count,**extra):
        n=self.scratch.measure(self.limits.max_sqlite_bytes)
        fd=self.scratch._open_regular(self.scratch.index)
        with os.fdopen(fd,'rb') as f:raw=f.read(self.limits.max_sqlite_bytes+1)
        if len(raw)!=n:raise RuntimeError('Scratch changed during finalization')
        sha=digest(raw);packed=gzip.compress(raw,compresslevel=3,mtime=0);del raw
        if len(packed)>64*MIB:raise ValueError('Packed index exceeds bound')
        key='objects/'+sha+'.sqlite.gz';self.io.immutable(key,packed)
        return {'key':key,'sha256':sha,'packed_sha256':digest(packed),
                'packed_md5':hashlib.md5(packed).hexdigest(),'raw_bytes':n,
                'packed_bytes':len(packed),'documents':count,
                **({'kind':kind} if kind else {}),**extra}

    def shard(self,docs):
        c=self.scratch.connect(self.limits.max_sqlite_bytes);split=False
        try:
            c.executescript('CREATE TABLE bodies(sha TEXT PRIMARY KEY,compressed BLOB NOT NULL);'
                'CREATE TABLE docs(id INTEGER PRIMARY KEY,path TEXT UNIQUE NOT NULL,'
                'size INTEGER,mtime_ns INTEGER,sha TEXT NOT NULL);'
                "CREATE VIRTUAL TABLE terms USING fts5(path,body,content='',"
                "tokenize='trigram',detail=none,columnsize=0);")
            expanding=False
            for i,d in enumerate(docs,1):
                raw=d['body'].encode('utf8')
                if len(raw)>MAX_DOCUMENT or digest(raw)!=d['sha256']:
                    raise ValueError('Invalid shard input')
                c.execute('INSERT OR IGNORE INTO bodies VALUES(?,?)',(d['sha256'],zlib.compress(raw,3)))
                c.execute('INSERT INTO docs VALUES(?,?,?,?,?)',(i,d['path'],d['size'],d['mtime_ns'],d['sha256']))
                c.execute('INSERT INTO terms(rowid,path,body) VALUES(?,?,?)',(i,d['path'],d['body']))
                expanding|=any(s.casefold()!=s.lower() for s in (d['body'],d['path']))
            c.commit();c.execute("INSERT INTO terms(terms) VALUES('optimize')");c.commit()
            if c.execute('PRAGMA quick_check').fetchone()[0]!='ok':
                raise RuntimeError('Shard integrity check failed')
            c.close()
            return [self._save_sqlite('',len(docs),
                bloom=bloom_for([d['body']+'\n'+d['path'] for d in docs]),expanding_casefold=expanding)]
        except sqlite3.OperationalError as exc:
            if (getattr(exc,'sqlite_errorcode',None)==13 or str(exc)=='database or disk is full') and len(docs)>1:
                split=True
            else:raise
        finally:
            c.close();self.scratch.discard_index()
        if split:
            mid=len(docs)//2
            return self.shard(docs[:mid])+self.shard(docs[mid:])
        raise AssertionError('unreachable')

    def path_index(self,entries,expected_count):
        c=self.scratch.connect(self.limits.max_sqlite_bytes)
        try:
            c.executescript('CREATE TABLE paths(path TEXT PRIMARY KEY NOT NULL,'
                'size INTEGER,mtime_ns INTEGER,sha TEXT,path_fold TEXT NOT NULL);'
                'CREATE INDEX paths_prefix ON paths(path_fold);')
            c.executemany('INSERT INTO paths VALUES(?,?,?,?,?)',
                          ((path,0,0,sha,path.casefold()) for path,sha in entries))
            c.commit()
            if (c.execute('SELECT COUNT(*) FROM paths').fetchone()[0]!=expected_count
                    or c.execute('PRAGMA quick_check').fetchone()[0]!='ok'):
                raise RuntimeError('Path index count/integrity mismatch')
            c.close();return self._save_sqlite('path_index',expected_count)
        finally:
            c.close();self.scratch.discard_index()

class BoundedBuilder:
    """Only called under the original publisher flock. No corpus-sized lists.

    Retains 16 counters/hashes, one source object, bounded pending docs and one
    metadata page. Restore/finalization accumulate bounded SMALL references,
    not all paths/bodies/Bloom descriptors or a tmpfs corpus spool.
    """
    def __init__(self,store,root,snapshot,checkpoint,progress,audit,*,limits=None,on_event:Callable|None=None):
        self.limits=limits or Limits();self.limits.validate()
        if not NAME.fullmatch(snapshot):raise ValueError('Invalid source snapshot')
        if json.loads(checkpoint)!=progress:raise ValueError('Pinned checkpoint/parsed source mismatch')
        self.source_sha=digest(checkpoint);self.snapshot=snapshot
        self.progress=progress;self.audit=audit;self.parts=progress.get('chunks')
        if (not isinstance(self.parts,list) or not 1<=len(self.parts)<=443
                or progress.get('finished') is not True or progress.get('consistent_snapshot') is not False
                or progress.get('safe_for_deletion') is not False):
            raise ValueError('Unaccepted pinned source')
        last=0;total=0
        self.source_prefix='archives/server-100/rolling/'+snapshot+'/parts/'
        for number,part in enumerate(self.parts):
            if (not isinstance(part,dict) or not _sha(part.get('sha256'))
                    or part.get('key')!=self.source_prefix+'%06d-'%number+part['sha256']+'.jsonl.gz'
                    or not _integer(part.get('compressed_bytes'),1,self.limits.max_source_packed)
                    or not _integer(part.get('rows'),1,3187870)
                    or not _integer(part.get('last_rowid'),last+1)):
                raise ValueError('Invalid pinned source part descriptor')
            last=part['last_rowid'];total+=part['rows']
        if (total!=progress.get('rows') or last!=progress.get('last_rowid') or total>3187870
                or audit.get('snapshot')!=snapshot or audit.get('historical_documents')!=total
                or audit.get('cloud_parts_fully_replayed')!=len(self.parts)
                or audit.get('duplicate_paths')!=0 or audit.get('malformed_paths')!=0
                or audit.get('consistent_snapshot') is not False or audit.get('safe_for_deletion') is not False):
            raise ValueError('Independent source audit mismatch')
        self.store=store;self.io=ObjectIO(store)
        inventory=getattr(store,'known',None)
        if not isinstance(inventory,dict) or len(inventory)>self.limits.max_inventory_objects:
            raise ValueError('Bounded verified immutable inventory required')
        self.scratch=Scratch(root,self.source_sha);self.publisher=BoundedPublisher(self.io,self.scratch,self.limits)
        self.prefix='private-source-checkpoints/native100/v3-fill1/'+snapshot+'/'+self.source_sha
        self.head_key=self.prefix+'/head.json';self.on_event=on_event or (lambda *a:None)
        self.head_bytes=None;self.commit_ref=None;self.tip=None;self.pages=0;self.cursor=0
        self.records=[];self.record_bytes=0
        self.pending=[[] for _ in range(16)];self.pending_sizes=[0]*16;self.pending_count=0
        self.counts=[0]*16;self.shard_counts=[0]*16;self.logical=[hashlib.sha256() for _ in range(16)]
        self.rows=0;self.oversize=0;self.raw_body_bytes=0;self.last_rowid=0
        self.peak_pending_bytes=0;self.peak_pending_count=0;self.peak_metadata_bytes=0
        self.fill=FilledQueues(self.scratch,self.limits)
        self.fill_flushes={'full':0,'bytes':0,'pressure':0,'batch_tail':0}

    def _state(self):
        return {'rows':self.rows,'oversize':self.oversize,'raw_body_bytes':self.raw_body_bytes,
                'last_rowid':self.last_rowid,'counts':list(self.counts),'shard_counts':list(self.shard_counts),
                'logical_sha256':[h.hexdigest() for h in self.logical]}

    def _page(self,ref):
        packed=self.io.checked(ref,self.prefix+'/pages/',self.limits.page_raw_bytes+65536)
        with gzip.GzipFile(fileobj=io.BytesIO(packed)) as f:
            raw=f.read(self.limits.page_raw_bytes+1)
            if len(raw)>self.limits.page_raw_bytes:raise ValueError('Decoded metadata page exceeds bound')
        page=json.loads(raw)
        if (not isinstance(page,dict) or page.get('format')!=FORMAT or page.get('source')!=self.source_sha
                or not _integer(page.get('sequence'),1,self.limits.max_pages)
                or not isinstance(page.get('records'),list) or not 1<=len(page['records'])<=self.limits.page_records):
            raise ValueError('Malformed metadata page')
        return page

    def _references(self):
        ref=self.tip;expected=self.pages;refs=[]
        while ref is not None:
            if len(refs)>=self.limits.max_pages:raise ValueError('Metadata chain exceeds bound')
            page=self._page(ref)
            if page['sequence']!=expected:raise ValueError('Metadata chain sequence gap')
            refs.append(ref);ref=page['previous'];expected-=1
        if expected!=0:raise ValueError('Incomplete metadata chain')
        return list(reversed(refs))

    def _records(self,refs):
        for ref in refs:yield from self._page(ref)['records']

    def _append(self,record):
        encoded=json_bytes(record)
        if len(encoded)+1024>self.limits.page_raw_bytes:raise ValueError('Single metadata record exceeds page admission')
        if (self.record_bytes+len(encoded)+1024>self.limits.page_raw_bytes
                or len(self.records)>=self.limits.page_records):self._flush_page()
        self.records.append(record);self.record_bytes+=len(encoded)+1
        self.peak_metadata_bytes=max(self.peak_metadata_bytes,self.record_bytes)

    def _flush_page(self):
        if not self.records:return
        if self.pages>=self.limits.max_pages:raise ValueError('Metadata page count cap reached')
        page={'format':FORMAT,'source':self.source_sha,'sequence':self.pages+1,'previous':self.tip,'records':self.records}
        raw=json_bytes(page)
        if len(raw)>self.limits.page_raw_bytes:raise ValueError('Metadata page exceeds hard bound')
        packed=gzip.compress(raw,compresslevel=1,mtime=0)
        self.tip=self.io.immutable(self.prefix+'/pages/'+digest(packed)+'.json.gz',packed)
        self.pages+=1;self.records=[];self.record_bytes=0

    def _shard_valid(self,d):
        if (not isinstance(d,dict) or not _sha(d.get('sha256'))
                or d.get('key')!='objects/'+d['sha256']+'.sqlite.gz' or not _sha(d.get('packed_sha256'))
                or not isinstance(d.get('packed_md5'),str) or re.fullmatch('[a-f0-9]{32}',d['packed_md5']) is None
                or not _integer(d.get('packed_bytes'),1,64*MIB) or not _integer(d.get('raw_bytes'),1,self.limits.max_sqlite_bytes)
                or not _integer(d.get('documents'),1,512) or not isinstance(d.get('bloom'),str) or len(d['bloom'])>24000):
            raise ValueError('Malformed immutable shard descriptor')
        if len(base64.b64decode(d['bloom'],validate=True))!=BLOOM_BYTES:raise ValueError('Shard Bloom descriptor mismatch')
        actual=self.store.known.get(d['key'])
        if actual is None or tuple(actual)!=(d['packed_bytes'],d['packed_md5']):
            raise ValueError('Committed shard absent/changed in verified inventory')

    def _verify_records(self,refs,expected):
        rows=oversized=body_bytes=last=0
        counts=[0]*16;shard_counts=[0]*16;shard_docs=[0]*16;logical=[hashlib.sha256() for _ in range(16)]
        for r in self._records(refs):
            if not isinstance(r,list) or not r:raise ValueError('Invalid journal record')
            if r[0]=='row':
                if len(r)!=7:raise ValueError('Invalid row metadata frame')
                _,pid,rowid,path,sha,size,searchable=r
                if (not _integer(pid,0,15) or not _integer(rowid,last+1) or not _path(path) or not _sha(sha)
                        or not _integer(size,0,self.limits.max_source_line) or type(searchable) is not bool
                        or searchable!=(size<=MAX_DOCUMENT) or hashlib.sha256(path.encode()).digest()[0]%16!=pid):
                    raise ValueError('Journal row identity/order/accounting failed')
                last=rowid;rows+=1;body_bytes+=size
                if searchable:counts[pid]+=1;logical[pid].update(json_bytes([path,sha]))
                else:oversized+=1
            elif r[0]=='shard':
                if len(r)!=3 or not _integer(r[1],0,15):raise ValueError('Malformed shard journal frame')
                _,pid,d=r;self._shard_valid(d);shard_counts[pid]+=1;shard_docs[pid]+=d['documents']
                if shard_counts[pid]>self.limits.max_shards_per_partition:raise ValueError('Partition shard count cap exceeded')
            else:raise ValueError('Unknown metadata record')
        actual={'rows':rows,'oversize':oversized,'raw_body_bytes':body_bytes,'last_rowid':last,
                'counts':counts,'shard_counts':shard_counts,'logical_sha256':[h.hexdigest() for h in logical]}
        if actual!=expected or shard_docs!=counts:raise ValueError('Checkpoint catalog/count/digest mismatch')
        return actual,logical

    def _commit_body(self,cursor):
        return {'format':FORMAT,'source':self.source_sha,'snapshot':self.snapshot,'limits':asdict(self.limits),'packing':PACKING_POLICY,
                'cursor':cursor,'previous':self.commit_ref,'journal':self.tip,'pages':self.pages,'state':self._state(),
                'consistent_snapshot':False,'safe_for_deletion':False}

    def _publish_head(self,cursor):
        if self.pending_count or self.fill.used or any(self.fill.counts):
            raise RuntimeError('Cannot checkpoint unflushed fill queues')
        body=json_bytes(self._commit_body(cursor))
        ref=self.io.immutable(self.prefix+'/commits/'+digest(body)+'.json',body)
        new=json_bytes({'format':FORMAT,'source':self.source_sha,'cursor':cursor,'commit':ref})
        self.on_event('before_head',{'cursor':cursor})
        if self.head_bytes is None:
            if self.io.exists(self.head_key):raise RuntimeError('Unknown concurrent head; refusing initialization')
        elif self.store.get(self.head_key,2048)!=self.head_bytes:
            raise RuntimeError('Checkpoint head changed; no writer takeover')
        self.store.put(self.head_key,new)
        self.on_event('after_head_write',{'cursor':cursor})
        if self.store.get(self.head_key,2048)!=new:raise RuntimeError('Checkpoint head readback unresolved')
        self.head_bytes=new;self.commit_ref=ref;self.cursor=cursor
        self.on_event('after_checkpoint',{'cursor':cursor})

    def initialize(self):
        if self.io.exists(self.head_key):raise RuntimeError('Existing checkpoint; explicit resume required')
        self._publish_head(0)

    def restore(self):
        raw=self.store.get(self.head_key,2048);head=json.loads(raw)
        if (not isinstance(head,dict) or head.get('format')!=FORMAT or head.get('source')!=self.source_sha
                or not _integer(head.get('cursor'),0,len(self.parts))):raise ValueError('Invalid checkpoint head')
        ref=head.get('commit');cursor=head['cursor'];manifests=[]
        if cursor!=len(self.parts) and cursor%self.limits.batch_parts:
            raise ValueError('Checkpoint cursor is not a confirmed batch boundary')
        while ref is not None:
            if len(manifests)>len(self.parts):raise ValueError('Checkpoint chain exceeds bound')
            commit=json.loads(self.io.checked(ref,self.prefix+'/commits/',65536))
            if (not isinstance(commit,dict) or commit.get('format')!=FORMAT or commit.get('source')!=self.source_sha
                    or commit.get('snapshot')!=self.snapshot or commit.get('limits')!=asdict(self.limits)
                    or commit.get('packing')!=PACKING_POLICY
                    or commit.get('cursor')!=cursor or commit.get('consistent_snapshot') is not False
                    or commit.get('safe_for_deletion') is not False or not _integer(commit.get('pages'),0,self.limits.max_pages)):
                raise ValueError('Checkpoint identity/batch boundary mismatch')
            expected_rows=sum(p['rows'] for p in self.parts[:cursor])
            expected_last=self.parts[cursor-1]['last_rowid'] if cursor else 0
            state=commit.get('state')
            if not isinstance(state,dict) or state.get('rows')!=expected_rows or state.get('last_rowid')!=expected_last:
                raise ValueError('Checkpoint cursor disagrees with pinned source')
            manifests.append(commit)
            if cursor==0:
                if commit['previous'] is not None or commit['pages']!=0 or commit['journal'] is not None:
                    raise ValueError('Invalid checkpoint genesis')
                break
            cursor=((cursor-1)//self.limits.batch_parts)*self.limits.batch_parts;ref=commit['previous']
            if ref is None:raise ValueError('Incomplete checkpoint predecessor chain')
        if not manifests or cursor!=0:raise ValueError('Checkpoint genesis missing')
        latest=manifests[0];self.tip=latest['journal'];self.pages=latest['pages'];refs=self._references()
        for commit in manifests:
            n=commit['pages']
            if n>len(refs) or (refs[n-1] if n else None)!=commit['journal']:
                raise ValueError('Checkpoint journal prefix changed')
        actual,logical=self._verify_records(refs,latest['state'])
        self.logical=logical;self.counts=actual['counts'];self.shard_counts=actual['shard_counts']
        self.rows=actual['rows'];self.oversize=actual['oversize'];self.raw_body_bytes=actual['raw_body_bytes']
        self.last_rowid=actual['last_rowid'];self.cursor=head['cursor'];self.head_bytes=raw;self.commit_ref=head['commit']
        return self.cursor

    def _flush_docs(self,pid,reason='batch_tail'):
        if not self.fill.counts[pid]:return
        docs=self.fill.take(pid)  # at most the unchanged global Python batch caps
        self.pending[pid]=docs
        self.pending_sizes[pid]=sum(len(d['body'].encode('utf8')) for d in docs)
        self.pending_count=len(docs)
        self.peak_pending_bytes=max(self.peak_pending_bytes,sum(self.pending_sizes))
        self.peak_pending_count=max(self.peak_pending_count,self.pending_count)
        self.fill_flushes[reason]+=1
        self.on_event('before_filled_shard',{'cursor':self.cursor})
        try:
            for d in self.publisher.shard(docs):
                self._shard_valid(d);self.shard_counts[pid]+=1
                if self.shard_counts[pid]>self.limits.max_shards_per_partition:
                    raise ValueError('Partition needs a separately reviewed split')
                self._append(['shard',pid,d])
        finally:
            self.pending_count=0;self.pending[pid]=[];self.pending_sizes[pid]=0

    def _add_doc(self,pid,path,body,size):
        if (self.pending_count or type(size) is not int or size<0
                or size>self.limits.pending_body_bytes):
            raise ValueError('Single document exceeds unchanged pending budget')
        if self.fill.needs_flush(pid,size):
            self._flush_docs(pid,'bytes')
        frame=self.fill.encode(document(path,body,0,0))
        if len(frame)>self.fill.cap:
            # A single encoded record larger than the staging quarter is
            # processed directly after draining queues; never raise a budget.
            for i in range(16):self._flush_docs(i,'pressure')
            raise ValueError('Fill record exceeds bounded staging admission')
        while self.fill.used+len(frame)>self.fill.cap:
            self._flush_docs(self.fill.fullest(),'pressure')
        self.fill.append(pid,frame,size)
        self.on_event('after_spool_append',{'cursor':self.cursor})
        if self.fill.counts[pid]==self.limits.pending_documents:
            self._flush_docs(pid,'full')

    def _part(self,index):
        part=self.parts[index];packed=self.store.get(part['key'],part['compressed_bytes'])
        if len(packed)!=part['compressed_bytes'] or digest(packed)!=part['sha256']:
            raise ValueError('Source part complete SHA/length mismatch')
        n=0
        with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
            while True:
                line=stream.readline(self.limits.max_source_line+1)
                if not line:break
                if len(line)>self.limits.max_source_line or not line.endswith(b'\n'):
                    raise ValueError('Source JSONL record exceeds reviewed admission; no skip')
                row=json.loads(line)
                if not isinstance(row,list) or len(row)!=3:raise ValueError('Invalid immutable source row')
                rowid,path,body=row
                if not _integer(rowid,self.last_rowid+1) or not _path(path) or not isinstance(body,str):
                    raise ValueError('Source identity/path/body invalid')
                data=body.encode('utf8');size=len(data);sha=digest(data)
                pid=hashlib.sha256(path.encode('utf8')).digest()[0]%16;searchable=size<=MAX_DOCUMENT
                self._append(['row',pid,rowid,path,sha,size,searchable])
                self.rows+=1;self.last_rowid=rowid;self.raw_body_bytes+=size;n+=1
                if searchable:
                    self.counts[pid]+=1;self.logical[pid].update(json_bytes([path,sha]));self._add_doc(pid,path,body,size)
                else:self.oversize+=1  # exact body remains in pinned archive
                del data,line,row,body
        if n!=part['rows'] or self.last_rowid!=part['last_rowid']:
            raise ValueError('Source part row/EOF accounting mismatch')

    def build(self,mode,before_source=None):
        if mode=='init':
            if before_source is not None:before_source(0)
            self.initialize()
        elif mode=='resume':
            self.restore()
            if before_source is not None:before_source(self.cursor)
        else:raise ValueError('Explicit bounded init/resume mode required')
        while self.cursor<len(self.parts):
            end=min(self.cursor+self.limits.batch_parts,len(self.parts))
            for index in range(self.cursor,end):self._part(index)
            for pid in range(16):self._flush_docs(pid)
            self.on_event('after_shards',{'cursor':end});self._flush_page()
            self.on_event('after_metadata',{'cursor':end});self._publish_head(end)
        if (self.rows!=self.progress['rows'] or self.oversize!=self.audit.get('oversize_documents')
                or self.raw_body_bytes!=self.audit.get('total_text_body_bytes')
                or sum(self.counts)+self.oversize!=self.rows):raise ValueError('Full source accounting failed')
        return self.cursor

    def finalize(self):
        """All private manifests first, one private catalog head last.

        Never changes scopes/*/latest.json or any production pointer. Final
        indexes stream cloud metadata one partition at a time into one capped
        SQLite scratch. Extra metadata scans trade latency for bounded memory.
        """
        if self.cursor!=len(self.parts) or self.pending_count or self.records or self.fill.used:
            raise RuntimeError('Incomplete batch state cannot publish a catalog')
        refs=self._references();self._verify_records(refs,self._state());candidates=[]
        for pid in range(16):
            logical=hashlib.sha256();count=0
            def entries():
                nonlocal count
                for r in self._records(refs):
                    if r[0]=='row' and r[1]==pid and r[6]:
                        count+=1;logical.update(json_bytes([r[3],r[4]]));yield r[3],r[4]
                if count!=self.counts[pid] or logical.digest()!=self.logical[pid].digest():
                    raise ValueError('Final path metadata digest mismatch')
            path_index=self.publisher.path_index(entries(),self.counts[pid]);shards=[]
            for r in self._records(refs):
                if r[0]=='shard' and r[1]==pid:
                    self._shard_valid(r[2]);shards.append(r[2])
                    if len(shards)>self.limits.max_shards_per_partition:raise ValueError('Final manifest shard cap exceeded')
            scope='rolling100-'+self.snapshot.lower()+'-%02d'%pid
            manifest={'format':1,'scope':scope,'source_host':'server-100','source_root':'/',
                'index_layout':INDEX_LAYOUT,'policy':'PROVISIONAL pinned archived history; not live; generated_at unknown (0)',
                'documents':self.counts[pid],'logical_sha256':self.logical[pid].hexdigest(),
                'path_index':path_index,'parts':shards,'skipped':{'oversized_historical_body':self.oversize},
                'generated_at':0,'complete_for_policy':True,'storage':'immutable cloud segments / bounded scratch',
                'source_checkpoint_sha256':self.source_sha}
            raw=json_bytes(manifest)
            if len(raw)>self.limits.max_manifest_bytes:raise ValueError('Final manifest exceeds admission')
            ref=self.io.immutable('manifests/'+scope+'/'+digest(raw)+'.json',raw)
            candidates.append({'scope':scope,'manifest':ref,'documents':self.counts[pid],'parts':len(shards)})
        catalog=json_bytes({'format':FORMAT,'source':self.source_sha,'checkpoint':self.commit_ref,
            'source_rows':self.rows,'indexed_documents':sum(self.counts),'oversize_archive_only':self.oversize,
            'candidates':candidates,'consistent_snapshot':False,'safe_for_deletion':False,
            'production_route_switched':False,'verified_fs_acl_accepted':False})
        ref=self.io.immutable(self.prefix+'/catalogs/'+digest(catalog)+'.json',catalog)
        head=json_bytes({'format':FORMAT,'source':self.source_sha,'catalog':ref});pointer=self.prefix+'/catalog-head.json'
        if self.io.exists(pointer):
            if self.store.get(pointer,2048)!=head:raise RuntimeError('Different private catalog already exists; no overwrite')
        else:self.store.put(pointer,head)
        if self.store.get(pointer,2048)!=head:raise RuntimeError('Private catalog publication unresolved')
        return {'event':'bounded_private_candidate_published','catalog':ref,'source_parts':self.cursor,
            'source_rows':self.rows,'indexed_documents':sum(self.counts),'oversize_archive_only':self.oversize,
            'peak_pending_body_bytes':self.peak_pending_bytes,'peak_pending_documents':self.peak_pending_count,
            'peak_metadata_payload_bytes':self.peak_metadata_bytes,'peak_tmpfs_scratch_bytes':self.scratch.peak_bytes,
            'packing':PACKING_POLICY,'peak_fill_tmpfs_bytes':self.fill.peak_bytes,
            'fill_flushes':dict(self.fill_flushes),
            'consistent_snapshot':False,'safe_for_deletion':False,'production_route_switched':False,'verified_fs_acl_accepted':False}
