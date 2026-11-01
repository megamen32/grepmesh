#!/usr/bin/env python3
"""Bounded-WAL, resumable *provisional* live export of legacy FTS source bodies.

Every SELECT is a short independent read transaction. Unlike one huge SQLite
read snapshot, this does not pin the writer WAL for hours. Because concurrent
updates may happen, this is NOT a consistent backup or permission to delete the
source database. The final cutover requires a separate frozen-writer reconcile.
"""
from __future__ import annotations
import argparse
import gzip
import hashlib
import io
import json
import fcntl
import os
from pathlib import Path
import sqlite3
import time

from cloud_index import CloudStore, digest, json_bytes, ensure_tmpfs, MIB

TABLE='grepmesh_documents_content'
RAW_TARGET=48*MIB
MAX_RECORD=192*MIB
MAX_WAL=2*1024*MIB
DEFAULT_ROWS=64


def dump(db_path:Path,store,snapshot:str,root:Path,max_chunks=0,batch_rows=DEFAULT_ROWS,chunk_raw=RAW_TARGET,create=False,lower_rowid=0,base_snapshot=None):
    ensure_tmpfs(root)
    if not db_path.is_file() or not db_path.name.endswith('.sqlite3'):
        raise ValueError('Explicit native SQLite source required')
    if not (1<=batch_rows<=128) or max_chunks<0 or not 64*1024<=chunk_raw<=RAW_TARGET:
        raise ValueError('Invalid bounded reader parameters')
    if store.prefix != 'yadisk:Backups/GrepMesh/cloud-index-v1':
        raise ValueError('Expected private GrepMesh cloud destination')
    if not snapshot or not snapshot.replace('-','').replace('T','').replace('Z','').isalnum() or len(snapshot)>64:
        raise ValueError('Invalid immutable run identifier')
    if (type(lower_rowid) is not int or lower_rowid<0 or
            (lower_rowid>0 and (not isinstance(base_snapshot,str)
             or not base_snapshot or base_snapshot==snapshot or
             not base_snapshot.replace('-','').replace('T','').replace('Z','').isalnum()
             or len(base_snapshot)>64)) or
            (lower_rowid==0 and base_snapshot is not None)):
        raise ValueError('Delta must have a valid completed base and a nonnegative lower row ID')
    prefix=f'archives/server-100/rolling/{snapshot}'
    state_key=prefix+'/progress.json'
    c=sqlite3.connect('file:'+str(db_path)+'?mode=ro',uri=True,timeout=4,isolation_level=None)
    c.execute('PRAGMA query_only=ON')
    c.execute('PRAGMA cache_size=-2048')
    c.execute('PRAGMA mmap_size=0')
    if c.execute('PRAGMA journal_mode').fetchone()[0]!='wal':
        raise RuntimeError('Expected WAL-mode source')
    try:
        if create:
            try:
                store.get(state_key,16*MIB)
            except (KeyError,RuntimeError):
                # A new unique run ID is required for creation. A caller
                # never silently resumes or reuses a different archive.
                pass
            else:
                raise ValueError('The rolling archive ID already exists')
            if lower_rowid:
                # A catch-up window may only follow an actually finished
                # private checkpoint for the SAME source. The base itself
                # remains provisional and cannot authorize any deletion.
                base=json.loads(store.get(
                    f'archives/server-100/rolling/{base_snapshot}/progress.json',16*MIB))
                if (base.get('format')!='grepmesh-rolling-text-v1' or
                    base.get('snapshot')!=base_snapshot or
                    base.get('source')!=str(db_path) or
                    base.get('finished') is not True or
                    base.get('upper_rowid')!=lower_rowid or
                    base.get('consistent_snapshot') is not False or
                    base.get('safe_for_deletion') is not False):
                    raise ValueError('Catch-up base is not a matching completed provisional checkpoint')
            state={'format':'grepmesh-rolling-text-v1','source':str(db_path),
                   'snapshot':snapshot,
                   'upper_rowid':c.execute(f'SELECT max(rowid) FROM {TABLE}').fetchone()[0],
                   'last_rowid':lower_rowid,'rows':0,'raw_bytes':0,'chunks':[],
                   'consistent_snapshot':False,'safe_for_deletion':False,
                   'finished':False}
            if (type(state['upper_rowid']) is not int or
                    state['upper_rowid']<=lower_rowid):
                raise RuntimeError('No new high-rowid range available to capture')
            if lower_rowid:
                state.update(lower_rowid=lower_rowid,base_snapshot=base_snapshot,
                             capture_mode='provisional_high_rowid_only')
            store.put(state_key,json_bytes(state))
        else:
            # Any unavailable cloud checkpoint is fatal on resume.
            state=json.loads(store.get(state_key,16*MIB))
            if (state.get('format')!='grepmesh-rolling-text-v1' or
                state.get('source')!=str(db_path) or
                state.get('snapshot')!=snapshot or state.get('finished') or
                state.get('lower_rowid',0)!=lower_rowid or
                state.get('base_snapshot')!=base_snapshot):
                raise ValueError('Progress state cannot be resumed safely')
        started=time.monotonic()
        raw=io.BytesIO()
        zipped=gzip.GzipFile(fileobj=raw,mode='wb',compresslevel=1,mtime=0)
        buffered=0;first_id=None;last_id=None;count=0;progress_count=0
        done=False
        next_cursor=int(state['last_rowid'])
        def publish_chunk():
            nonlocal raw,zipped,buffered,first_id,last_id,count,progress_count
            if count==0:return
            zipped.close()
            payload=raw.getvalue()
            sha=digest(payload)
            key=f'{prefix}/parts/{len(state["chunks"]):06d}-{sha}.jsonl.gz'
            store.put(key,payload)
            part={'key':key,'sha256':sha,'compressed_bytes':len(payload),
                  'raw_bytes':buffered,'rows':count,
                  'first_rowid':first_id,'last_rowid':last_id}
            state['chunks'].append(part)
            state['last_rowid']=last_id
            state['rows']+=count
            state['raw_bytes']+=buffered
            # Progress pointer is explicitly NON-authoritative. Its separate
            # immutable object has already been remotely hash-verified.
            store.put(state_key,json_bytes(state))
            progress_count+=1
            print(json.dumps({'event':'rolling_chunk','host':'server-100',
                'chunks':len(state['chunks']),'rows':state['rows'],
                'last_rowid':last_id,'cloud_mib':round(len(payload)/MIB,2),
                'elapsed_s':round(time.monotonic()-started,1),
                'consistent_snapshot':False}),flush=True)
            raw=io.BytesIO()
            zipped=gzip.GzipFile(fileobj=raw,mode='wb',compresslevel=1,mtime=0)
            buffered=0;count=0;first_id=last_id=None
        try:
            while next_cursor<=state['upper_rowid']:
                previous=next_cursor
                # The cursor is fully exhausted in each small page and
                # releases its WAL read mark BEFORE cloud network operations.
                rows=c.execute(
                    f'SELECT rowid,c0,c1 FROM {TABLE} '
                    'WHERE rowid>? AND rowid<=? ORDER BY rowid LIMIT ?',
                    (previous,int(state['upper_rowid']),batch_rows)
                ).fetchall()
                if not rows:
                    done=True;break
                for rowid,path,body in rows:
                    if not isinstance(path,str) or not isinstance(body,str):
                        raise ValueError('Native FTS source is not valid text')
                    line=json_bytes([rowid,path,body])+b'\n'
                    if len(line)>MAX_RECORD:
                        raise RuntimeError('Unbounded source body; retain existing database')
                    if count and buffered+len(line)>chunk_raw:
                        publish_chunk()
                        if max_chunks and progress_count>=max_chunks:
                            return state
                    zipped.write(line)
                    buffered+=len(line)
                    count+=1
                    if first_id is None:first_id=rowid
                    last_id=rowid
                # Advance the in-process cursor only after the page is read;
                # the durable cursor is separately committed per cloud chunk.
                next_cursor=int(rows[-1][0])
                # If interrupted mid-chunk, resume from the last uploaded rowid.
                wal=Path(str(db_path)+'-wal')
                if wal.exists() and wal.stat().st_size>MAX_WAL:
                    raise RuntimeError('Production WAL exceeded 2 GiB; abort safe')
            publish_chunk()
            if done and not (max_chunks and progress_count>=max_chunks):
                state['finished']=True
                state['consistent_snapshot']=False
                state['safe_for_deletion']=False
                store.put(state_key,json_bytes(state))
                print(json.dumps({'event':'rolling_provisional_finished',
                    'upper_rowid':state['upper_rowid'],
                    'archived_rows':state['rows'],'chunks':len(state['chunks']),
                    'consistent_snapshot':False,'safe_for_deletion':False}),flush=True)
            return state
        finally:
            zipped.close()
    finally:
        c.close()


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--database',type=Path,required=True)
    p.add_argument('--root',type=Path,required=True)
    p.add_argument('--snapshot',required=True)
    p.add_argument('--max-chunks',type=int,default=0)
    p.add_argument('--create',action='store_true')
    p.add_argument('--lower-rowid',type=int,default=0)
    p.add_argument('--base-snapshot')
    args=p.parse_args()
    lockpath=args.root/'rolling-export100.lock'
    ensure_tmpfs(args.root)
    with lockpath.open('a+b') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        dump(args.database,CloudStore(),args.snapshot,args.root,args.max_chunks,
             create=args.create,lower_rowid=args.lower_rowid,
             base_snapshot=args.base_snapshot)


if __name__=='__main__':
    main()
