#!/usr/bin/env python3
"""Non-destructive exact live-row parity for one immutable provisional GrepMesh tail.

A deleted source row is preserved by its archived body, never treated as
permission to delete the native SQLite. A row changed in place is a mismatch.
All paths are kept private; receipts contain only aggregate counts.
"""
import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sqlite3
import time

from cloud_index import CloudStore, digest

SOURCES = {
    'server-100': '/home/roomhacker/.cache/grepmesh/server-100-65415a79c0e91f4b.sqlite3',
    'server-44': '/home/roomhacker/.cache/grepmesh/server-44-65415a79c0e91f4b.sqlite3',
}
ID_RE = re.compile(r'^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}$')
MAX_RECORD = 192 * 1024 * 1024


def verify(host, snapshot, store=None, source=None):
    if host not in SOURCES or ID_RE.fullmatch(snapshot) is None:
        raise ValueError('Unrecognized immutable source identity')
    source = source or SOURCES[host]
    if source != SOURCES[host]:
        raise PermissionError('Native source substitution prohibited')
    store = store or CloudStore()
    prefix = f'archives/{host}/rolling/{snapshot}'
    cp = store.get(prefix+'/progress.json', 16*1024*1024)
    state = json.loads(cp)
    if (state.get('source') != source or state.get('snapshot') != snapshot
            or state.get('format') != 'grepmesh-rolling-text-v1'
            or state.get('finished') is not True
            or state.get('consistent_snapshot') is not False
            or state.get('safe_for_deletion') is not False
            or not isinstance(state.get('chunks'), list) or not state['chunks']):
        raise ValueError('Provisional checkpoint untrusted or unfinished')
    lower, upper = state.get('lower_rowid'), state.get('upper_rowid')
    if not (type(lower) is int and type(upper) is int and 0 < lower < upper):
        raise ValueError('Expected bounded append-only catch-up window')
    db=sqlite3.connect(f'file:{source}?mode=ro', uri=True, timeout=5, isolation_level=None)
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('PRAGMA mmap_size=0')
        db.execute('PRAGMA cache_size=-2048')
        if db.execute('PRAGMA journal_mode').fetchone()[0] != 'wal':
            raise RuntimeError('Live source WAL mode changed')
        seen = set()
        retained = modified = matches = 0
        previous = lower
        started = time.monotonic()
        for i, part in enumerate(state['chunks']):
            packed = store.get(part['key'], part['compressed_bytes'])
            if (part['key'] != f"{prefix}/parts/{i:06d}-{part['sha256']}.jsonl.gz"
                    or len(packed) != part['compressed_bytes']
                    or digest(packed) != part['sha256']):
                raise RuntimeError('Immutable cloud tail part mismatch')
            with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
                for line in stream:
                    if len(line) > MAX_RECORD or not line.endswith(b'\n'):
                        raise RuntimeError('Oversize or malformed archived row')
                    rid, path, body = json.loads(line)
                    if (type(rid) is not int or not previous < rid <= upper
                            or not isinstance(path, str) or not isinstance(body, str)
                            or rid in seen):
                        raise RuntimeError('Malformed or duplicate archived row')
                    previous=rid
                    seen.add(rid)
                    live=db.execute(
                        'SELECT c0,c1 FROM grepmesh_documents_content WHERE rowid=?', (rid,)
                    ).fetchone()
                    if live is None:retained+=1
                    elif live[0]==path and live[1]==body:matches+=1
                    else:modified+=1
        if len(seen)!=state['rows'] or previous!=state['last_rowid']:
            raise RuntimeError('Tail document count/cursor mismatch')
        now={row[0] for row in db.execute(
            'SELECT rowid FROM grepmesh_documents_content WHERE rowid>? AND rowid<=?',
            (lower,upper))}
        new_in_closed_window=len(now-seen)
        receipt={
            'host':host,'snapshot':snapshot,
            'source_window_lower_exclusive':lower,'source_window_upper_inclusive':upper,
            'archived_docs':len(seen),'source_rows_matching':matches,
            'source_rows_deleted_but_preserved':retained,
            'source_rows_changed_since_capture':modified,
            'source_rows_newly_inserted_in_closed_window':new_in_closed_window,
            'current_live_window_exact_parity':modified==0 and new_in_closed_window==0,
            'cloud_checkpoint_sha256':hashlib.sha256(cp).hexdigest(),
            'consistent_snapshot':False,'safe_for_deletion':False,'ssd_reclaimed_bytes':0,
            'elapsed_seconds':round(time.monotonic()-started,1),
        }
        return receipt
    finally:
        db.close()


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--host', choices=sorted(SOURCES), required=True)
    ap.add_argument('--snapshot', required=True)
    args=ap.parse_args()
    receipt=verify(args.host,args.snapshot)
    path=Path.home()/'.local/state'/f'grepmesh-{args.host}-tail-live-{args.snapshot}.json'
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    os.umask(0o077)
    temp=path.with_name(path.name+'.tmp.'+str(os.getpid()))
    fd=os.open(temp,os.O_WRONLY|os.O_EXCL|os.O_CREAT,0o600)
    with os.fdopen(fd,'w') as out:
        json.dump(receipt,out,sort_keys=True)
        out.write('\n')
        out.flush();os.fsync(out.fileno())
    os.replace(temp,path)
    print(json.dumps(receipt,sort_keys=True))


if __name__=='__main__':
    main()
