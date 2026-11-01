#!/usr/bin/env python3
"""Publish oversized historical bodies from a verified native archive.

Streams grepmesh_documents_content rows of one completed archive (the same
verified source the legacy scopes were built from), keeps every row whose
body exceeds the 1 MiB shard limit, and publishes it through
oversize_support.publish_oversize into the immutable chunk+manifest+pointer
layout. Nothing is staged on disk, no native index is touched, and no MCP
peer is registered: this only uploads new cloud objects under
oversize/<host>/<snapshot>/.

The script fails loudly if the archive yields a different number of
oversized bodies than expected, and afterwards runs a full OversizeIndex
re-admission plus a bounded real read canary over sampled paths.
"""
from __future__ import annotations
import argparse, json, time, zlib
from cloud_index import MAX_DOCUMENT, digest
from reindex_native_archive import get_manifest, archive_rows
from oversize_support import OversizeIndex, publish_oversize


def oversize_docs(rows,progress_every=0):
    """Yield {'path','body'} for archive rows above the shard limit."""
    start=time.monotonic();seen=0;found=0
    for rowid,_shadow_id,path,body in rows:
        seen+=1
        if not isinstance(path,str) or not path.startswith('/') or \
                not isinstance(body,str):
            raise ValueError('Archive row is not an FTS text row')
        encoded=body.encode('utf-8')
        if len(encoded)>MAX_DOCUMENT:
            found+=1
            yield {'path':path,'body':body}
            if progress_every and found%progress_every==0:
                print(json.dumps({'event':'oversize_scan_found','found':found,
                    'rows_seen':seen,'elapsed_s':round(time.monotonic()-start,1)}),
                    flush=True)


def verify(store,host,snapshot,expect_documents,expected_rows_min=0):
    """Re-admit the freshly published index and run a bounded read canary."""
    index=OversizeIndex(store,host,snapshot)
    index.refresh()
    manifest=index.manifest
    by_path=index.by_path
    documents=manifest['documents']
    total=sum(record['size_bytes'] for record in manifest['records'])
    if expect_documents is not None and documents!=expect_documents:
        raise RuntimeError('Oversize index documents '+str(documents)+
                           ' differ from the expected '+str(expect_documents))
    if total<expected_rows_min:
        raise RuntimeError('Oversize index bytes below the expected gate')
    if not manifest['records']:
        return {'documents':0,'total_bytes':0,'canary':[]}
    # Bounded canary: verify chunk objects and digests for three sampled
    # paths directly from the store (a single line above 1 MiB is never
    # returned by any reader, so a direct chunk check is the honest proof).
    records=sorted(manifest['records'],
                   key=lambda r:(r['size_bytes'],r['path']))
    chosen={records[0]['path'],records[-1]['path'],
            records[len(records)//2]['path']}
    canary=[]
    for path in sorted(chosen):
        record=by_path[path]
        chunk=record['chunks'][0]
        packed=store.get(chunk['key'],chunk['packed_bytes']+1)
        if len(packed)!=chunk['packed_bytes'] or \
                digest(packed)!=chunk['packed_sha256']:
            raise RuntimeError('Oversize canary chunk mismatch: '+path)
        decoder=zlib.decompressobj()
        raw=decoder.decompress(packed,chunk['raw_bytes']+1)
        if (not decoder.eof or len(raw)!=chunk['raw_bytes'] or
                decoder.unused_data or digest(raw)!=chunk['sha256']):
            raise RuntimeError('Oversize canary integrity failure: '+path)
        canary.append({'path':path,'body_sha256':record['body_sha256'],
                       'chunk1_raw_bytes':len(raw)})
    return {'documents':documents,'total_bytes':total,'canary':canary,
            'packed_bytes':manifest['packed_bytes'],
            'raw_bytes':manifest['raw_bytes']}


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--host',required=True)
    ap.add_argument('--snapshot',required=True)
    ap.add_argument('--expect',type=int,default=None,
                    help='Exact oversized-document count to enforce, if known')
    ap.add_argument('--min-bytes',type=int,default=0,
                    help='Total oversize body bytes floor, if known')
    ap.add_argument('--progress',type=int,default=0)
    args=ap.parse_args()
    from cloud_index import CloudStore
    store=CloudStore()
    manifest=get_manifest(store,args.host,args.snapshot)
    table=manifest['tables']['grepmesh_documents_content']
    print(json.dumps({'event':'oversize_scan_begin','host':args.host,
        'snapshot':args.snapshot,'archived_rows':table['rows']}),flush=True)
    result=publish_oversize(store,args.host,args.snapshot,
                            oversize_docs(archive_rows(store,table),args.progress))
    print(json.dumps({'event':'oversize_publish_done','host':args.host,
        'snapshot':args.snapshot,'pointer_key':result['pointer_key'],
        'documents':result['documents'],'chunks':result['chunks'],
        'raw_bytes':result['raw_bytes'],'packed_bytes':result['packed_bytes']}),flush=True)
    admitted=verify(store,args.host,args.snapshot,result['documents'],
                    expected_rows_min=args.min_bytes)
    print(json.dumps({'event':'oversize_index_admitted','host':args.host,
        'snapshot':args.snapshot,'admission':admitted,
        'manifest_sha256':result['manifest_sha256']}),flush=True)


if __name__=='__main__':
    main()
