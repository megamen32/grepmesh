#!/usr/bin/env python3
"""Validate and optionally sample-restore an immutable GrepMesh native archive.

Checks its completion pointer, manifest SHA, compressed object SHA, every JSONL
row byte, full logical checksums and table row counts. Never writes source DB.
"""
from __future__ import annotations
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import time
from cloud_index import CloudStore, digest, json_bytes
from export_native_archive import TABLES, MIB


def verify(host: str, snapshot: str, cloud: CloudStore, validate_json=False):
    base=f'archives/{host}/{snapshot}'
    receipt=json.loads(cloud.get(base+'/complete.json',256*1024))
    if not receipt.get('complete') or receipt.get('host_id')!=host or receipt.get('snapshot')!=snapshot:
        raise ValueError('Archive completion receipt mismatches requested snapshot')
    raw=cloud.get(receipt['manifest'],16*MIB)
    if digest(raw)!=receipt['sha256']:
        raise ValueError('Manifest SHA-256 mismatch')
    m=json.loads(raw)
    if m.get('schema')!='grepmesh-native-logical-archive/v1' or m.get('host_id')!=host:
        raise ValueError('Unexpected archive format/source')
    if m.get('snapshot_id')!=snapshot or set(m.get('tables',{}))!=set(TABLES):
        raise ValueError('Missing source-data table or wrong archive')
    start=time.monotonic()
    totals={}
    first_paths=[]
    total_cloud_bytes=0
    for name,meta in m['tables'].items():
        written=0
        count=0
        rolling=hashlib.sha256()
        last_end=None
        if meta['rows']>0 and not meta['chunks']:
            raise ValueError('Nonempty table has no content')
        for idx,part in enumerate(meta['chunks']):
            if last_end is not None and part['first_rowid']<=last_end:
                raise ValueError('Rows overlap or are unordered')
            if part['rows']<=0:
                raise ValueError('Empty compressed archive object')
            compressed=cloud.get(part['key'],max(part['compressed_bytes'],1))
            if len(compressed)!=part['compressed_bytes'] or digest(compressed)!=part['sha256']:
                raise ValueError('Compressed archive checksum mismatch')
            total_cloud_bytes+=len(compressed)
            bytes_here=0
            rows_here=0
            first=None
            last=None
            with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as source:
                while True:
                    line=source.readline(193*MIB)
                    if not line:
                        break
                    if len(line)>=193*MIB or not line.endswith(b'\n'):
                        raise ValueError('Archive line exceeds record bound or lacks newline')
                    if validate_json:
                        data=json.loads(line)
                        if not isinstance(data,list) or len(data)!=len(meta['columns']):
                            raise ValueError('Malformed archive row')
                        rowid=data[0]
                    else:
                        # Even when JSON parsing is off, ensure the byte stream
                        # itself is checked and every record is newline-terminated.
                        rowid=None
                    bytes_here+=len(line)
                    rolling.update(line)
                    rows_here+=1
                    if name=='grepmesh_documents_content' and len(first_paths)<3:
                        # Sample from actual cloud bytes, not the original DB.
                        obj=json.loads(line)
                        first_paths.append({'rowid':obj[0], 'path_sha256':digest(obj[-2].encode()),
                                            'text_sha256':digest(obj[-1].encode())})
                    if rowid is not None:
                        if first is None: first=rowid
                        last=rowid
            if bytes_here!=part['raw_bytes'] or rows_here!=part['rows']:
                raise ValueError('Archive chunk row count/size differs from manifest')
            if validate_json and (first!=part['first_rowid'] or last!=part['last_rowid']):
                raise ValueError('Archive chunk row identifiers mismatch')
            count+=rows_here;written+=bytes_here
            last_end=part['last_rowid']
        if count!=meta['rows'] or written!=meta['raw_bytes']:
            raise ValueError('Archive table count/size mismatches manifest')
        if rolling.hexdigest()!=meta['logical_sha256']:
            raise ValueError('Archive logical table digest mismatch')
        totals[name]=count
        print(json.dumps({'event':'table_verified','host':host,'table':name,
                          'rows':count,'chunks':len(meta['chunks']),
                          'elapsed_s':round(time.monotonic()-start,2)}),flush=True)
    if totals['grepmesh_documents_content']!=totals['grepmesh_document_rows']:
        raise ValueError('Source content and row mapping counts differ')
    result={'verified':True,'host':host,'snapshot':snapshot,
            'documents':totals['grepmesh_documents_content'],
            'source_bytes':m['source_bytes'],
            'cloud_bytes_verified':total_cloud_bytes,
            'sampled_document_paths':first_paths,
            'table_counts':totals,'elapsed_s':round(time.monotonic()-start,2)}
    print(json.dumps({'event':'archive_verified',**result},ensure_ascii=False),flush=True)
    return result


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--host',required=True)
    ap.add_argument('--snapshot',required=True)
    ap.add_argument('--validate-json',action='store_true')
    args=ap.parse_args()
    verify(args.host,args.snapshot,CloudStore(),args.validate_json)

if __name__=='__main__':
    main()
