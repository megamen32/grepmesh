#!/usr/bin/env python3
"""Retrieve exact historical body by path from a verified cold native archive.

Does not need the original source file, does not create a persistent local
database, and does not publish an unauthenticated MCP endpoint.
"""
from __future__ import annotations
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
from cloud_index import CloudStore, MAX_DOCUMENT, digest, MIB


def objects(cloud, meta):
    for part in meta['chunks']:
        payload=cloud.get(part['key'],part['compressed_bytes'])
        if len(payload)!=part['compressed_bytes'] or digest(payload)!=part['sha256']:
            raise ValueError('Archived cloud object failed SHA-256')
        yield part, gzip.GzipFile(fileobj=io.BytesIO(payload))


def read_document(cloud, host, snapshot, path):
    if not path.startswith('/') or '/../' in path or path.endswith('/..'):
        raise ValueError('Absolute historical document path required')
    prefix=f'archives/{host}/{snapshot}'
    pointer=json.loads(cloud.get(prefix+'/complete.json',256*1024))
    if not pointer.get('complete') or pointer.get('host_id')!=host:
        raise ValueError('Cold archive is not complete for this host')
    raw=cloud.get(pointer['manifest'],16*MIB)
    if digest(raw)!=pointer['sha256']:
        raise ValueError('Cold archive manifest hash mismatch')
    m=json.loads(raw)
    if m['snapshot_id']!=snapshot or m['host_id']!=host:
        raise ValueError('Cold archive identity mismatch')
    key=None
    for part,stream in objects(cloud,m['tables']['grepmesh_document_rows']):
        with stream:
            for line in stream:
                row=json.loads(line)
                if row[1]==path:
                    key=int(row[2])
                    break
        if key is not None:break
    if key is None:return None
    for part in m['tables']['grepmesh_documents_content']['chunks']:
        if part['first_rowid']<=key<=part['last_rowid']:
            for _,stream in objects(cloud,{'chunks':[part]}):
                with stream:
                    for line in stream:
                        record=json.loads(line)
                        if record[0]==key:
                            if record[-2]!=path:
                                raise ValueError('Archive path/row mapping mismatch')
                            body=record[-1]
                            if not isinstance(body,str):
                                raise ValueError('Invalid historical body type')
                            return {'path':path,'rowid':key,'body':body,
                                    'body_sha256':digest(body.encode('utf-8')),
                                    'bytes':len(body.encode('utf-8'))}
            raise RuntimeError('Archive mapping references a missing document row')
    raise RuntimeError('Archive content chunk omitted requested row')


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--host',required=True)
    p.add_argument('--snapshot',required=True)
    p.add_argument('--path',required=True)
    p.add_argument('--sha-only',action='store_true')
    args=p.parse_args()
    result=read_document(CloudStore(),args.host,args.snapshot,args.path)
    if result is None:
        print(json.dumps({'found':False,'path_sha256':digest(args.path.encode())}))
        return
    if args.sha_only:
        result.pop('body')
        result['path_sha256']=digest(result.pop('path').encode())
    print(json.dumps({'found':True,**result},ensure_ascii=False))


if __name__=='__main__':main()
