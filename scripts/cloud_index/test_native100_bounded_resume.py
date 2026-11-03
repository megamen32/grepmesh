"""Synthetic tmpfs object store. No Yandex, credentials, host files or ACL fixtures.

The file store survives *worker process death*, unlike MemoryStore. Its remote
folder simulates external cloud storage and is NOT builder scratch/cache.
"""
import contextlib
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
import zlib
from dataclasses import asdict, replace
from unittest.mock import patch

from cloud_index import digest, json_bytes, valid_key, MIB, Reader
from native100_bounded_resume import BoundedBuilder, Limits, Scratch, ObjectIO

SNAP='20261031T233108Z-5732b979'

class FileCloud:
    def __init__(self,root,log=None):
        self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.log=Path(log) if log else None
        self.known={};self.gets=0;self.bytes_in=0;self.puts=0;self.reused=0
        self.source_reads=[];self.write_keys=[];self.fail_after_head_cursor=None
        objects=self.root/'objects'
        if objects.exists():
            for path in objects.iterdir():
                if path.is_file():
                    raw=path.read_bytes()
                    self.known['objects/'+path.name]=(len(raw),hashlib.md5(raw).hexdigest())

    def path(self,key):return self.root.joinpath(*valid_key(key).split('/'))
    def exists(self,key):return self.path(key).exists()

    def get(self,key,limit=64*MIB):
        if '/parts/' in key and key.startswith('archives/'):
            ordinal=int(key.rsplit('/',1)[-1][:6]);self.source_reads.append(ordinal)
            if self.log:
                with self.log.open('a') as f:
                    f.write(json.dumps({'pid':os.getpid(),'part':ordinal})+'\n');f.flush();os.fsync(f.fileno())
        with self.path(key).open('rb') as f:raw=f.read(limit+1)
        if len(raw)>limit:raise ValueError('Synthetic object exceeds bound')
        self.gets+=1;self.bytes_in+=len(raw);return raw

    def put(self,key,data):
        p=self.path(key);p.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        tmp=p.with_name(p.name+'.new-'+str(os.getpid()))
        fd=os.open(tmp,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'wb') as f:f.write(data);f.flush();os.fsync(f.fileno())
        os.replace(tmp,p)
        fd=os.open(p.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(fd)
        finally:os.close(fd)
        self.puts+=1;self.write_keys.append(key)
        if key.startswith('objects/'):
            self.known[key]=(len(data),hashlib.md5(data).hexdigest())
        if key.endswith('/head.json') and self.fail_after_head_cursor is not None:
            if json.loads(data)['cursor']==self.fail_after_head_cursor:
                raise OSError('synthetic acknowledged-write response lost')

    def bind_verified_source_checkpoint(self,raw,receipt):
        if receipt.get('checkpoint_sha256')!=digest(raw):raise ValueError('synthetic pinned receipt mismatch')
        return json.loads(raw)


def source_row(number,*,body_bytes=80,duplicates=False,oversized=False):
    path='/home/fixture/document-%06d.txt'%number
    if duplicates and number==5:path='/home/fixture/document-000001.txt'
    if oversized and number==3:
        body='O'*(MIB+1)
    elif number%9==0:body=''
    else:
        body=('needle %06d\nOCR/STT Привет\n'%number)+'x'*body_bytes
    return path,body


def make_fixture(base,parts=6,rows=4,body_bytes=80,duplicates=False,oversized=False,limits=None):
    base=Path(base);base.mkdir(parents=True,exist_ok=True,mode=0o700)
    runtime=base/'runtime';runtime.mkdir(mode=0o700)
    store=FileCloud(base/'remote');chunks=[];rowid=0;raw_bytes=0;over=0
    for n in range(1,parts+1):
        lines=[]
        for _ in range(rows):
            rowid+=1;path,body=source_row(rowid,body_bytes=body_bytes,duplicates=duplicates,oversized=oversized)
            size=len(body.encode());raw_bytes+=size;over+=size>MIB
            lines.append(json_bytes([rowid,path,body])+b'\n')
        packed=gzip.compress(b''.join(lines),mtime=0);sha=digest(packed)
        key='archives/server-100/rolling/'+SNAP+'/parts/%06d-'%n+sha+'.jsonl.gz'
        store.put(key,packed)
        chunks.append({'key':key,'sha256':sha,'compressed_bytes':len(packed),'rows':rows,'last_rowid':rowid})
    progress={'finished':True,'consistent_snapshot':False,'safe_for_deletion':False,
              'rows':rowid,'last_rowid':rowid,'chunks':chunks}
    checkpoint=json_bytes(progress)
    store.put('archives/server-100/rolling/'+SNAP+'/progress.json',checkpoint)
    audit={'snapshot':SNAP,'historical_documents':rowid,'cloud_parts_fully_replayed':parts,
           'duplicate_paths':0,'malformed_paths':0,'consistent_snapshot':False,'safe_for_deletion':False,
           'oversize_documents':over,'total_text_body_bytes':raw_bytes}
    params={'parts':parts,'rows_per_part':rows,'body_bytes':body_bytes,'duplicates':duplicates,
            'oversized':oversized,'progress':progress,'audit':audit,
            'limits':asdict(limits or Limits(batch_parts=2))}
    (base/'fixture.json').write_text(json.dumps(params))
    (base/'audit.json').write_text(json.dumps(audit))
    (base/'source-receipt.json').write_text(json.dumps({'checkpoint_sha256':digest(checkpoint)}))
    return params


def engine(base,event=None,limits=None):
    base=Path(base);f=json.loads((base/'fixture.json').read_text())
    store=FileCloud(base/'remote',base/('reads-%d.jsonl'%os.getpid()))
    e=BoundedBuilder(store,base/'runtime',SNAP,json_bytes(f['progress']),f['progress'],f['audit'],
                     limits=limits or Limits(**f['limits']),on_event=event)
    return e,store,f


def verify_catalog(base,summary):
    """Compare every path AND recovered full body, not just row counts."""
    base=Path(base);store=FileCloud(base/'remote');f=json.loads((base/'fixture.json').read_text())
    catalog=json.loads(store.get(summary['catalog']['key'],summary['catalog']['bytes']))
    all_paths={};all_bodies={}
    with tempfile.TemporaryDirectory(dir=base,prefix='verify-') as tmp:
        tmp=Path(tmp)
        def db(descriptor):
            packed=store.get(descriptor['key'],descriptor['packed_bytes'])
            assert digest(packed)==descriptor['packed_sha256']
            raw=gzip.decompress(packed);assert digest(raw)==descriptor['sha256']
            p=tmp/'verify.sqlite';p.write_bytes(raw)
            return sqlite3.connect('file:'+str(p)+'?mode=ro&immutable=1',uri=True)
        for entry in catalog['candidates']:
            ref=entry['manifest'];raw=store.get(ref['key'],ref['bytes']);assert digest(raw)==ref['sha256']
            manifest=json.loads(raw)
            with db(manifest['path_index']) as c:
                rows=c.execute('SELECT path,sha FROM paths').fetchall()
            assert len(rows)==entry['documents']
            for path,sha in rows:
                assert path not in all_paths;all_paths[path]=sha
            for shard in manifest['parts']:
                with db(shard) as c:
                    records=c.execute('SELECT d.path,d.sha,b.compressed FROM docs d JOIN bodies b ON d.sha=b.sha').fetchall()
                for path,sha,compressed in records:
                    body=zlib.decompress(compressed).decode()
                    assert digest(body.encode())==sha
                    assert path not in all_bodies;all_bodies[path]=body
    expected={}
    for n in range(1,f['parts']*f['rows_per_part']+1):
        path,body=source_row(n,body_bytes=f['body_bytes'],duplicates=f['duplicates'],oversized=f['oversized'])
        if len(body.encode())<=MIB:expected[path]=body
    assert all_bodies==expected
    assert all_paths=={p:digest(b.encode()) for p,b in expected.items()}
    assert catalog['source_rows']==f['progress']['rows']
    return {'exact_paths':len(all_paths),'exact_bodies':len(all_bodies),'catalog':catalog}


class BoundedResumeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir='/run/user/1000',prefix='grepmesh-bounded-unit-')
        self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        make_fixture(self.root,parts=4,rows=4)

    def completed(self):
        e,s,f=engine(self.root);e.build('init');return e,s,f

    def reject_corrupt_restore(self,mutator):
        e,s,_=self.completed();mutator(e,s)
        q,t,_=engine(self.root)
        with self.assertRaises((ValueError,RuntimeError,FileNotFoundError,KeyError,EOFError)):
            q.build('resume')
        self.assertEqual(t.source_reads,[])

    def test_full_catalog_paths_and_bodies_exact(self):
        e,_,_=self.completed();result=e.finalize()
        checked=verify_catalog(self.root,result)
        self.assertEqual(checked['exact_paths'],16)
        self.assertFalse(result['production_route_switched'])
        self.assertFalse(result['verified_fs_acl_accepted'])

    def test_no_full_path_spool_or_retained_path_list(self):
        e,_,_=self.completed()
        self.assertEqual(list(e.scratch.root.iterdir()),[e.scratch.marker])
        self.assertFalse(hasattr(e,'paths'));self.assertFalse(hasattr(e,'partitions'))
        self.assertEqual(e.records,[]);self.assertEqual(e.pending_count,0)

    def test_lost_head_ack_resumes_actual_remote_commit(self):
        e,s,_=engine(self.root);s.fail_after_head_cursor=2
        with self.assertRaises(OSError):e.build('init')
        q,t,_=engine(self.root);q.build('resume')
        self.assertEqual(t.source_reads,[3,4]);verify_catalog(self.root,q.finalize())

    def test_repeat_resume_no_source_replay_and_catalog_idempotent(self):
        e,s,_=self.completed();a=e.finalize()
        old={str(p.relative_to(s.root)):digest(p.read_bytes()) for p in s.root.rglob('*') if p.is_file()}
        q,t,_=engine(self.root);q.build('resume');b=q.finalize()
        new={str(p.relative_to(t.root)):digest(p.read_bytes()) for p in t.root.rglob('*') if p.is_file()}
        self.assertEqual(t.source_reads,[]);self.assertEqual(t.puts,0)
        self.assertEqual(a['catalog'],b['catalog']);self.assertEqual(old,new)

    def test_existing_head_cannot_be_initialized_again(self):
        self.completed();q,s,_=engine(self.root)
        with self.assertRaisesRegex(RuntimeError,'Existing checkpoint'):q.build('init')
        self.assertEqual(s.source_reads,[])

    def test_torn_head_fails_before_source(self):
        self.reject_corrupt_restore(lambda e,s:s.path(e.head_key).write_bytes(b'{'))

    def test_corrupted_commit_fails_before_source(self):
        self.reject_corrupt_restore(lambda e,s:s.path(e.commit_ref['key']).write_bytes(b'corrupted'))

    def test_missing_predecessor_fails_before_source(self):
        def bad(e,s):
            commit=json.loads(s.get(e.commit_ref['key'],65536))
            s.path(commit['previous']['key']).unlink()
        self.reject_corrupt_restore(bad)

    def test_corrupt_metadata_page_fails_before_source(self):
        self.reject_corrupt_restore(lambda e,s:s.path(e.tip['key']).write_bytes(b'broken'))

    def test_missing_page_fails_before_source(self):
        self.reject_corrupt_restore(lambda e,s:s.path(e.tip['key']).unlink())

    def test_missing_shard_inventory_fails_before_source(self):
        def bad(e,s):next((s.root/'objects').iterdir()).unlink()
        self.reject_corrupt_restore(bad)

    def test_altered_shard_inventory_fails_before_source(self):
        def bad(e,s):next((s.root/'objects').iterdir()).write_bytes(b'bad same account object')
        self.reject_corrupt_restore(bad)

    def test_cursor_cannot_skip_unconfirmed_source_parts(self):
        def bad(e,s):
            head=json.loads(s.get(e.head_key,2048));head['cursor']=3;s.path(e.head_key).write_bytes(json_bytes(head))
        self.reject_corrupt_restore(bad)

    def test_source_row_count_in_commit_is_cross_checked(self):
        def bad(e,s):
            head=json.loads(s.get(e.head_key,2048));commit=json.loads(s.get(head['commit']['key'],65536))
            commit['state']['rows']-=1;raw=json_bytes(commit)
            ref={'key':e.prefix+'/commits/'+digest(raw)+'.json','bytes':len(raw),'sha256':digest(raw)}
            s.put(ref['key'],raw);head['commit']=ref;s.path(e.head_key).write_bytes(json_bytes(head))
        self.reject_corrupt_restore(bad)

    def test_foreign_checkpoint_identity_refused(self):
        def bad(e,s):
            h=json.loads(s.get(e.head_key,2048));h['source']='f'*64;s.path(e.head_key).write_bytes(json_bytes(h))
        self.reject_corrupt_restore(bad)

    def test_truncated_source_keeps_last_confirmed_batch(self):
        e,s,f=engine(self.root);key=f['progress']['chunks'][2]['key'];s.path(key).write_bytes(b'torn')
        with self.assertRaises(ValueError):e.build('init')
        self.assertEqual(json.loads(s.get(e.head_key,2048))['cursor'],2)

    def test_concurrent_head_change_is_not_overwritten(self):
        e,s,_=engine(self.root)
        def event(stage,info):
            if stage=='before_head' and info['cursor']==2:s.put(e.head_key,b'foreign writer')
        e.on_event=event
        with self.assertRaisesRegex(RuntimeError,'head changed'):e.build('init')
        self.assertEqual(s.get(e.head_key,2048),b'foreign writer')

    def test_unknown_existence_is_not_absence(self):
        e,s,_=engine(self.root)
        with patch.object(s,'exists',side_effect=PermissionError('synthetic denial')):
            with self.assertRaises(PermissionError):e.build('init')
        self.assertEqual(s.puts,0);self.assertEqual(s.source_reads,[])

    def test_unknown_scratch_artifact_not_deleted(self):
        e,_,_=engine(self.root);unknown=e.scratch.root/'foreign.partial'
        unknown.write_bytes(b'preserve')
        with self.assertRaisesRegex(RuntimeError,'Unknown scratch'):engine(self.root)
        self.assertEqual(unknown.read_bytes(),b'preserve')

    def test_symlink_scratch_not_followed(self):
        e,_,_=engine(self.root);outside=self.root/'keep';outside.write_bytes(b'keep')
        e.scratch.index.symlink_to(outside)
        with self.assertRaises(OSError):engine(self.root)
        self.assertEqual(outside.read_bytes(),b'keep')

    def test_marked_crash_scratch_reconciled_only_in_owned_directory(self):
        e,_,_=engine(self.root);e.scratch.index.write_bytes(b'disposable');e.scratch.index.chmod(0o600)
        outside=self.root/'unrelated.partial';outside.write_bytes(b'keep')
        q,_,_=engine(self.root)
        self.assertFalse(q.scratch.index.exists());self.assertEqual(outside.read_bytes(),b'keep')

    def test_sqlite_cap_applied_before_large_path_insert(self):
        e,_,_=engine(self.root,limits=Limits(batch_parts=2,max_sqlite_bytes=64*1024))
        observed=[];original=e.scratch.discard_index
        def discard():
            if e.scratch.index.exists():observed.append(e.scratch.index.stat().st_size)
            original()
        with patch.object(e.scratch,'discard_index',side_effect=discard):
            with self.assertRaises(sqlite3.OperationalError):
                e.publisher.path_index((('/home/'+'x'*3000+str(i),'a'*64) for i in range(100)),100)
        self.assertLessEqual(max(observed),64*1024)
        self.assertFalse(e.scratch.index.exists())

    def test_global_pending_and_metadata_are_bounded(self):
        e,_,_=engine(self.root,limits=Limits(batch_parts=2,pending_documents=3,pending_body_bytes=4096,page_raw_bytes=32768))
        e.build('init');e.finalize()
        self.assertLessEqual(e.peak_pending_count,3)
        self.assertLessEqual(e.peak_pending_bytes,4096)
        self.assertLess(e.peak_metadata_bytes,32768)
        self.assertLessEqual(e.scratch.peak_bytes,e.limits.max_sqlite_bytes+1024)

    def test_actual_publisher_entrypoint_uses_new_mode_under_same_lock(self):
        import publish_native100_rolling_private as target
        f=json.loads((self.root/'fixture.json').read_text());s=FileCloud(self.root/'remote')
        with contextlib.ExitStack() as stack:
            for name,value in {'ROOT':self.root/'runtime','AUDIT':self.root/'audit.json',
                'SOURCE_VERIFICATION':self.root/'source-receipt.json','RECEIPT':self.root/'candidate.json',
                'ARCHIVED':16,'PARTS':4}.items():stack.enter_context(patch.object(target,name,value))
            stack.enter_context(patch.object(target,'VerifiedImmutableReuseStore',return_value=s))
            stack.enter_context(patch.dict(os.environ,{'GREPMESH_REUSE_VERIFIED_SHARDS':'1',
                'GREPMESH_BOUNDED_RESUME_MODE':'init','GREPMESH_DURABLE_BUILD_MODE':'',
                'GREPMESH_REQUIRE_CACHED_SOURCE_PART':''}))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            result=target.run()
        self.assertEqual(result['source_rows'],16);verify_catalog(self.root,result)
        self.assertTrue((self.root/'runtime/private-build.lock').exists())

    def test_oversize_is_accounted_not_silently_claimed_searchable(self):
        with tempfile.TemporaryDirectory(dir='/run/user/1000',prefix='grepmesh-over-synthetic-') as tmp:
            make_fixture(tmp,parts=2,rows=3,oversized=True)
            e,_,_=engine(tmp);e.build('init');result=e.finalize()
            self.assertEqual(result['source_rows'],6);self.assertEqual(result['indexed_documents'],5)
            self.assertEqual(result['oversize_archive_only'],1);verify_catalog(tmp,result)

    def test_duplicate_paths_cannot_publish_final_catalog(self):
        with tempfile.TemporaryDirectory(dir='/run/user/1000',prefix='grepmesh-dupe-synthetic-') as tmp:
            make_fixture(tmp,parts=4,rows=2,duplicates=True)
            e,s,_=engine(tmp);e.build('init')
            with self.assertRaises(sqlite3.IntegrityError):e.finalize()
            self.assertFalse(s.exists(e.prefix+'/catalog-head.json'))

    def test_literal_regex_read_text_and_root_filter_on_generated_shards(self):
        e,s,_=self.completed();summary=e.finalize()
        catalog=json.loads(s.get(summary['catalog']['key'],summary['catalog']['bytes']))
        target='/home/fixture/document-000001.txt';pid=hashlib.sha256(target.encode()).digest()[0]%16
        entry=catalog['candidates'][pid];ref=entry['manifest'];m=json.loads(s.get(ref['key'],ref['bytes']))
        # Synthetic reader admission ONLY. This is NOT a real signed Hub ACL test.
        s.put('scopes/'+entry['scope']+'/latest.json',json_bytes({'manifest':ref['key'],'sha256':ref['sha256'],
            'documents':m['documents'],'source_host':'server-100','source_root':'/'}))
        cache=self.root/'reader-cache';cache.mkdir(mode=0o700)
        reader=Reader(s,cache,entry['scope'],64*MIB,'server-100','/')
        reader.refresh()
        out=reader.query('needle 000001',authorized_roots=['/home'])
        self.assertIn(target,[h['path'] for h in out['results']])
        out=reader.query(r'needle 00000[1]',regex=True,authorized_roots=['/home'])
        self.assertIn(target,[h['path'] for h in out['results']])
        text=reader.read_text(target,authorized_roots=['/home'])
        self.assertEqual(text['body_sha256'],digest(source_row(1)[1].encode()))
        with self.assertRaises(PermissionError):reader.read_text('/dev/null',authorized_roots=['/home'])
        with self.assertRaises(PermissionError):reader.read_text(target,authorized_roots=['/opt'])
        self.assertLessEqual(reader.used,64*MIB)

if __name__=='__main__':unittest.main()
