#!/usr/bin/env python3
"""Independently verify every private server-44 cloud FTS shard and path index.

Reads only immutable Yandex.Disk blobs into bounded tmpfs SQLite files.
Checks hashes of packed/raw files, every compressed text body, FTS trigram
probes, every document/path mapping, logical order and all path indexes.
The historical source archive is separately audited. This is NOT admission
for production ACL, source updates, OCR refresh, or SQLite deletion.
"""
import fcntl
import gzip
import hashlib
import json
import os
import pathlib
import re
import sqlite3
import tempfile
import time
import zlib
from cloud_index import CloudStore, MAX_DOCUMENT, MAX_MANIFEST, MAX_RAW_SHARD, digest, ensure_tmpfs, json_bytes

HOST='server-44'
SNAPSHOT='20261031T214414Z-ae84d605'
ROOT=pathlib.Path('/run/user/1000/grepmesh-private44-payload-audit')
SOURCE_RECEIPT=pathlib.Path('/home/roomhacker/.local/state/grepmesh-native44-logical-parity.json')
RECEIPT=pathlib.Path('/home/roomhacker/.local/state/grepmesh-native44-payload-parity.json')

def get_retried(store,key,expected):
    if expected<=0 or expected>MAX_RAW_SHARD:
        raise RuntimeError('Invalid immutable cloud object byte limit')
    for attempt in range(4):
        try:
            blob=store.get(key,expected)
            if len(blob)!=expected:
                raise RuntimeError('Cloud object size mismatch')
            return blob
        except RuntimeError as exc:
            if 'Cloud read failed:' not in str(exc) or attempt==3:
                raise
            print(json.dumps({'event':'transient_cloud_read_retry','attempt':attempt+1}),flush=True)
            time.sleep((attempt+1)*6)
    raise AssertionError('unreachable')

def checked_raw(store,desc):
    packed=get_retried(store,desc['key'],int(desc['packed_bytes']))
    if digest(packed)!=desc['packed_sha256']:
        raise RuntimeError('Compressed FTS object checksum mismatch')
    expected=int(desc['raw_bytes'])
    if not 0<expected<=MAX_RAW_SHARD:
        raise RuntimeError('FTS object exceeds RAM shard bound')
    decoder=zlib.decompressobj(16+zlib.MAX_WBITS)
    raw=decoder.decompress(packed,expected+1)
    if (not decoder.eof or decoder.unused_data or decoder.unconsumed_tail
            or len(raw)!=expected or digest(raw)!=desc['sha256']):
        raise RuntimeError('FTS SQLite object uncompressed checksum mismatch')
    return raw

def verify_sqlite_bytes(raw,callback):
    fd,name=tempfile.mkstemp(suffix='.sqlite',dir=ROOT)
    try:
        with os.fdopen(fd,'wb') as f:
            f.write(raw)
        c=sqlite3.connect('file:'+name+'?mode=ro',uri=True,timeout=10)
        try:
            c.execute('PRAGMA query_only=ON')
            c.execute('PRAGMA cache_size=-2048')
            c.execute('PRAGMA mmap_size=0')
            if c.execute('PRAGMA quick_check').fetchone()[0]!='ok':
                raise RuntimeError('Cloud-backed SQLite shard quick_check failed')
            return callback(c)
        finally:
            c.close()
    finally:
        pathlib.Path(name).unlink(missing_ok=True)

def verify_shard(c,desc,seen,logical):
    docs=0
    refs={}
    hashes=set()
    for docid,path,sha in c.execute('SELECT id,path,sha FROM docs ORDER BY id'):
        if type(docid) is not int or docid!=docs+1:
            raise RuntimeError('Cloud FTS doc rowid gap')
        if not isinstance(path,str) or not path.startswith('/') or '/../' in path:
            raise RuntimeError('Cloud FTS invalid document path')
        if path in seen:
            raise RuntimeError('Repeated document path across cloud FTS shards')
        if not isinstance(sha,str) or re.fullmatch(r'[a-f0-9]{64}',sha) is None:
            raise RuntimeError('Cloud FTS invalid document checksum')
        seen[path]=sha
        hashes.add(sha)
        refs.setdefault(sha,docid)
        logical.update(json_bytes([path,sha]))
        docs+=1
    if docs!=desc['documents']:
        raise RuntimeError('Cloud FTS shard document count mismatch')
    bodies=set()
    probe=None
    for sha,packed in c.execute('SELECT sha,compressed FROM bodies'):
        if sha in bodies:
            raise RuntimeError('Repeated FTS body SHA')
        decoded=zlib.decompressobj()
        raw=decoded.decompress(packed,MAX_DOCUMENT+1)
        if (len(raw)>MAX_DOCUMENT or not decoded.eof or decoded.unused_data
                or decoded.unconsumed_tail or digest(raw)!=sha):
            raise RuntimeError('Actual indexed text cannot be recovered by SHA')
        try: text=raw.decode('utf-8')
        except UnicodeDecodeError: raise RuntimeError('Cloud FTS stored text is not UTF-8')
        bodies.add(sha)
        if probe is None and sha in refs:
            m=re.search(r'[a-zA-Z0-9]{3}',text)
            if m is not None:
                probe=(refs[sha],m.group(0).lower())
    if bodies!=hashes:
        raise RuntimeError('FTS path-to-text body relation incomplete')
    if probe is not None:
        rid,gram=probe
        try:
            hit=c.execute('SELECT rowid FROM terms WHERE terms MATCH ? AND rowid=? LIMIT 1',
                          ('"'+gram+'"',rid)).fetchone()
        except sqlite3.DatabaseError as error:
            raise RuntimeError('Cloud FTS trigram search unavailable') from error
        if hit is None or hit[0]!=rid:
            raise RuntimeError('Cloud FTS trigram probe returned a false negative')
    return docs,int(probe is not None),len(bodies)

def verify_paths(c,seen):
    count=0
    for path,sha in c.execute('SELECT path,sha FROM paths ORDER BY path'):
        old=seen.pop(path,None)
        if old is None or old!=sha:
            raise RuntimeError('Path-only cloud index mismatches full-text shard')
        count+=1
    if seen:
        raise RuntimeError('Path-only cloud index omitted indexed documents')
    return count

def atomic_receipt(out):
    RECEIPT.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    name=RECEIPT.with_name(RECEIPT.name+'.tmp.'+str(os.getpid()))
    fd=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f:
        json.dump(out,f,sort_keys=True)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(name,RECEIPT)

def main():
    if os.getenv('GREPMESH_CLOUD_GATEWAY')!='local':
        raise RuntimeError('Only the already-authorized private Yandex.Disk gateway is supported')
    ensure_tmpfs(ROOT)
    lock=ROOT/'verify.lock'
    with lock.open('a+b') as f:
        fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
        earlier=json.loads(SOURCE_RECEIPT.read_text())
        if (earlier.get('archive_rows_verified')!=849796
                or earlier.get('private_fts_documents_logical_parity')!=849721
                or earlier.get('oversize_documents_logical_parity')!=75
                or earlier.get('content_sha_parity_against_archived_fts') is not True
                or earlier.get('safe_for_deletion') is not False):
            raise RuntimeError('Independent immutable source-archive parity gate missing')
        store=CloudStore()
        checked=0
        paths=0
        bodies=0
        probes=0
        started=time.monotonic()
        for i in range(8):
            scope=f'legacy-server44-{i}'
            receipt=json.loads(store.get('scopes/'+scope+'/latest.json',65536))
            blob=store.get(receipt['manifest'],MAX_MANIFEST)
            if digest(blob)!=receipt['sha256']:
                raise RuntimeError('FTS manifest changed since admission')
            m=json.loads(blob)
            if (m['scope']!=scope or m['source_host']!=HOST or m['source_root']!='/'
                    or not m.get('complete_for_policy') or len(m['parts'])>500):
                raise RuntimeError('FTS scope identity/size/policy mismatch')
            seen={}
            logical=hashlib.sha256()
            scope_count=0
            for desc in m['parts']:
                raw=checked_raw(store,desc)
                n,probe_count,b=verify_sqlite_bytes(raw,lambda c:verify_shard(c,desc,seen,logical))
                scope_count+=n
                probes+=probe_count
                bodies+=b
                checked+=1
            if scope_count!=m['documents'] or logical.hexdigest()!=m['logical_sha256']:
                raise RuntimeError('Cloud FTS stored text diverges from immutable historical archive')
            path_desc=m['path_index']
            path_raw=checked_raw(store,path_desc)
            n=verify_sqlite_bytes(path_raw,lambda c:verify_paths(c,seen))
            if n!=scope_count or n!=path_desc['documents']:
                raise RuntimeError('Cloud path index document count mismatch')
            paths+=n
            print(json.dumps({'event':'verified_private_fts_bucket','bucket':i,
                'shards_verified_total':checked,'documents_verified_total':paths,
                'elapsed_s':round(time.monotonic()-started,1)}),flush=True)
        if paths!=849721 or checked!=141 or probes<100:
            raise RuntimeError('Incomplete FTS body, shard or trigram probe coverage')
        result={'event':'verified_all_private44_fts_shard_payloads',
          'source':HOST,'snapshot':SNAPSHOT,
          'cloud_fts_documents_actual_text_verified':paths,
          'fts_shards_verified':checked,'path_indexes_verified':8,
          'verified_unique_content_bodies':bodies,
          'fts_live_trigram_probes_passed':probes,
          'full_archived_text_logical_sha_parity':True,
          'oversize_chunk_payloads_fully_verified':False,
          'production_verified_acl_accepted':False,
          'live_source_update_parity':False,'safe_for_deletion':False,
          'ssd_reclaimed_bytes':0,'elapsed_s':round(time.monotonic()-started,1)}
        atomic_receipt(result)
        print(json.dumps(result),flush=True)

if __name__=='__main__':
    main()
