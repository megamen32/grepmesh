#!/usr/bin/env python3
"""Independent, full logical parity audit of private server-44 native history.

This reads only the immutable, verified cloud archive, the 8 private FTS
manifests, and the oversized-text manifest. It neither reads production SQLite
nor changes access, active services, cloud pointers, or on-disk indices.
A successful receipt is NOT permission to delete the native database: shard
payloads, source updates, OCR, production ACL and rollback remain separate gates.
"""
import hashlib
import json
import os
import pathlib
import sys
import time

from cloud_index import CloudStore, MAX_DOCUMENT, MAX_MANIFEST, digest, json_bytes
from reindex_native_archive import ROOTS, BUCKETS, archive_rows, get_manifest
from oversize_support import OversizeIndex

HOST = 'server-44'
SNAPSHOT = '20261031T214414Z-ae84d605'
RECEIPT = pathlib.Path('/home/roomhacker/.local/state/grepmesh-native44-logical-parity.json')

def main():
    if os.getenv('GREPMESH_CLOUD_GATEWAY') != 'local':
        raise RuntimeError('Private, already-authorized local cloud gateway required')
    store = CloudStore()
    archive = get_manifest(store,HOST,SNAPSHOT)
    table = archive['tables']['grepmesh_documents_content']
    assert table['rows'] == 849796
    oversize = OversizeIndex(store,HOST,SNAPSHOT)
    oversize.refresh()
    assert oversize.manifest['documents']==75
    hashes = [hashlib.sha256() for _ in range(BUCKETS)]
    counts = [0] * BUCKETS
    seen = [set() for _ in range(BUCKETS)]
    oversized = {}
    scanned = 0
    unknown = 0
    last_rowid = -1
    started = time.monotonic()
    for record in archive_rows(store,table):
        if len(record)!=4:
            raise ValueError('Archive content record shape drift')
        rid,_,path,body=record
        if type(rid) is not int or rid<=last_rowid:
            raise RuntimeError('Non-increasing archived FTS row ID')
        last_rowid=rid
        if not isinstance(path,str) or not path.startswith('/') or not isinstance(body,str):
            raise ValueError('Invalid archived FTS content')
        scanned+=1
        if not any(path==r or path.startswith(r.rstrip('/')+'/') for r in ROOTS):
            unknown+=1
            continue
        raw=body.encode('utf-8')
        if len(raw)>MAX_DOCUMENT:
            if path in oversized:
                raise ValueError('Duplicate oversize historical path')
            oversized[path]=(len(raw),digest(raw))
        else:
            b=hashlib.sha256(path.encode()).digest()[0]%BUCKETS
            if path in seen[b]:
                raise ValueError('Duplicate searchable historical path')
            seen[b].add(path)
            counts[b]+=1
            hashes[b].update(json_bytes([path,digest(raw)]))
        if scanned%100000==0:
            print(json.dumps({'phase':'archived_texts','scanned':scanned,'elapsed_s':round(time.monotonic()-started,1)}),flush=True)
    if scanned!=849796 or unknown or sum(counts)!=849721 or len(oversized)!=75:
        raise RuntimeError('Archived source rows are incomplete or outside admitted policy')
    for i in range(BUCKETS):
        scope=f'legacy-{HOST.replace("-","")}-{i}'
        pointer=json.loads(store.get('scopes/'+scope+'/latest.json',65536))
        blob=store.get(pointer['manifest'],MAX_MANIFEST)
        if digest(blob)!=pointer['sha256']:
            raise RuntimeError('Private FTS manifest digest mismatch')
        manifest=json.loads(blob)
        if manifest['scope']!=scope or manifest['source_host']!=HOST or manifest['source_root']!='/':
            raise RuntimeError('Private FTS scope identity changed')
        if manifest['documents']!=counts[i] or manifest['logical_sha256']!=hashes[i].hexdigest():
            raise RuntimeError('Full-text corpus logical digest differs from original archive')
        if sum(x['documents'] for x in manifest['parts'])!=counts[i]:
            raise RuntimeError('FTS shard count differs from verified source')
    if set(oversized)!=set(oversize.by_path):
        raise RuntimeError('Oversize historical path set differs from original archive')
    for path,(size,sha) in oversized.items():
        item=oversize.by_path[path]
        if item['size_bytes']!=size or item['body_sha256']!=sha:
            raise RuntimeError('Oversize historical text SHA-256 mismatch')
    receipt={
        'source':HOST,'snapshot':SNAPSHOT,'archive_rows_verified':scanned,
        'private_fts_documents_logical_parity':sum(counts),
        'fts_bucket_digests_verified':BUCKETS,
        'oversize_documents_logical_parity':len(oversized),
        'archive_parts_streamed_and_sha_verified':len(table['chunks']),
        'content_sha_parity_against_archived_fts':True,
        'archive_snapshot_only':True,
        'consistent_live_source':False,'source_updates_accepted':False,
        'production_verified_acl_accepted':False,
        'shard_payloads_independently_verified':False,
        'safe_for_deletion':False,'ssd_reclaimed_bytes':0,
        'elapsed_s':round(time.monotonic()-started,1),
    }
    RECEIPT.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    temp=RECEIPT.with_name(RECEIPT.name+'.tmp.'+str(os.getpid()))
    fd=os.open(temp,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f:
        json.dump(receipt,f,sort_keys=True)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp,RECEIPT)
    print(json.dumps({'event':'verified_archive_to_cloud_index_full_logical_parity',**receipt}),flush=True)

if __name__=='__main__':
    main()
