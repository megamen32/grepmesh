#!/usr/bin/env python3
"""Build private, unexposed RAM-only fulltext scopes from a verified legacy archive.

The archive can include paths absent from the live filesystem. The resulting
searchable shards inherit the original source host and never consult source
files. This script does NOT install/register a peer or delete native indexes.
"""
from __future__ import annotations
import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import time

from cloud_index import (
    CloudStore, Publisher, digest, document, ensure_tmpfs, json_bytes, MAX_DOCUMENT, MIB,
)

ROOTS = ('/home/roomhacker', '/opt', '/etc')
BUCKETS = 8


def get_manifest(store,host,snapshot):
    base=f'archives/{host}/{snapshot}'
    receipt=json.loads(store.get(base+'/complete.json',256*1024))
    if not receipt.get('complete') or receipt['host_id']!=host or receipt['snapshot']!=snapshot:
        raise ValueError('No completed archive receipt')
    blob=store.get(receipt['manifest'],16*MIB)
    if digest(blob)!=receipt['sha256']:
        raise ValueError('Archive manifest hash mismatch')
    m=json.loads(blob)
    if m['snapshot_id']!=snapshot or m['host_id']!=host:
        raise ValueError('Archive source mismatch')
    return m


def archive_rows(store,table):
    for part in table['chunks']:
        packed=store.get(part['key'],part['compressed_bytes'])
        if len(packed)!=part['compressed_bytes'] or digest(packed)!=part['sha256']:
            raise ValueError('Archive content object corrupted')
        rows=0
        with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
            for line in stream:
                rows+=1
                yield json.loads(line)
        if rows!=part['rows']:
            raise ValueError('Archive object row count changed')


def prepare_sources(store,manifest,root,host,num=BUCKETS):
    ensure_tmpfs(root)
    if num!=BUCKETS:
        raise ValueError('Eight fixed partitions required to bound path index size')
    # Exact source fingerprints only; no filesystem walk or stale source loss.
    meta={}
    for row in archive_rows(store,manifest['tables']['grepmesh_extraction_cache']):
        _,path,size,mtime,_body=row
        meta[path]=(int(size),int(mtime))
    counts=[0]*num
    hashes=[hashlib.sha256() for _ in range(num)]
    skipped={'oversized_historical_body':0,'unknown_source_root':0}
    staged=[]
    outputs=[]
    start=time.monotonic()
    try:
        for i in range(num):
            p=root/f'legacy-{host}-{i}-{manifest["snapshot_id"]}.jsonl'
            if p.exists():
                raise FileExistsError('Existing staged data; refuse overwrite')
            f=p.open('x',encoding='utf-8')
            os.chmod(p,0o600)
            head={'kind':'begin','format':1,'source_host':host,
                  'source_root':'/',
                  'policy':'archived native FTS historical rows; immutable; source files are not read; private ACL required; bucket '+str(i)+'/'+str(num)}
            f.write(json.dumps(head,ensure_ascii=False)+'\n')
            staged.append((p,f))
        for row in archive_rows(store,manifest['tables']['grepmesh_documents_content']):
            rowid,_shadow_id,path,body=row
            if not isinstance(path,str) or not path.startswith('/'):
                raise ValueError('Invalid archived path')
            if not any(path==p or path.startswith(p.rstrip('/')+'/') for p in ROOTS):
                skipped['unknown_source_root']+=1
                continue
            if not isinstance(body,str):
                raise ValueError('Non-text archived FTS body')
            data=body.encode('utf-8')
            if len(data)>MAX_DOCUMENT:
                skipped['oversized_historical_body']+=1
                continue
            i=hashlib.sha256(path.encode('utf-8')).digest()[0]%num
            size,mtime=meta.get(path,(0,0))
            doc=document(path,body,size,mtime)
            hashes[i].update(json_bytes([path,doc['sha256']]))
            staged[i][1].write(json.dumps(doc,ensure_ascii=False)+'\n')
            counts[i]+=1
            if sum(counts)%100000==0:
                print(json.dumps({'event':'archive_reindex_scan','host':host,
                      'documents_processed':sum(counts),
                      'elapsed_s':round(time.monotonic()-start,1)}),flush=True)
        if sum(counts)+sum(skipped.values())!=manifest['tables']['grepmesh_documents_content']['rows']:
            raise RuntimeError('Archive rows were lost during reindex staging')
        for i,(path,f) in enumerate(staged):
            f.write(json.dumps({'kind':'end','count':counts[i],
                'logical_sha256':hashes[i].hexdigest(),'skipped':skipped,
                'complete_for_policy':True},ensure_ascii=False)+'\n')
            f.flush()
            f.close()
        print(json.dumps({'event':'staging_ready','documents':sum(counts),
            'counts':counts,'skipped':skipped,
            'staged_bytes':sum(p.stat().st_size for p,_ in staged),
            'elapsed_s':round(time.monotonic()-start,2)}),flush=True)
        return [(p,counts[i]) for i,(p,_) in enumerate(staged)],skipped
    except Exception:
        for p,f in staged:
            try:f.close()
            except OSError:pass
            p.unlink(missing_ok=True)
        raise


def publish(store,paths,host,snapshot,root):
    ensure_tmpfs(root)
    out=[]
    for i,(path,expected) in enumerate(paths):
        scope=f'legacy-{host.replace("-","")}-{i}'
        with path.open('r',encoding='utf-8') as stream:
            manifest=Publisher(store,root,shard_text_bytes=8*MIB).publish(scope,stream)
        if manifest['documents']!=expected:
            raise ValueError('Cloud publication document count mismatch')
        out.append({'scope':scope,'documents':expected,'parts':len(manifest['parts']),
                    'packed_bytes':sum(x['packed_bytes'] for x in manifest['parts'])})
        path.unlink() # tmpfs stage only; immutable cloud manifest is verified
        print(json.dumps({'event':'cloud_scope_published','host':host,**out[-1]}),flush=True)
    return out


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--host',default='server-44',choices=['server-44'])
    ap.add_argument('--snapshot',required=True)
    ap.add_argument('--root',type=Path,required=True)
    args=ap.parse_args()
    ensure_tmpfs(args.root)
    store=CloudStore()
    manifest=get_manifest(store,args.host,args.snapshot)
    paths,skipped=prepare_sources(store,manifest,args.root,args.host)
    result=publish(store,paths,args.host,args.snapshot,args.root)
    print(json.dumps({'event':'cloud_reindex_complete','host':args.host,
        'indexed_documents':sum(r['documents'] for r in result),
        'archived_documents':manifest['tables']['grepmesh_documents_content']['rows'],
        'skipped':skipped,'scopes':result}),flush=True)

if __name__=='__main__': main()
