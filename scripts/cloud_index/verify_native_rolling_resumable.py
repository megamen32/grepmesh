#!/usr/bin/env python3
"""Resumable independent verifier for an explicitly PROVISIONAL GrepMesh rolling capture.

Uses the existing exhaustive per-chunk verifier; never exports, writes the source,
or grants deletion rights. A tiny local progress receipt avoids re-fetching
already verified immutable cloud parts after transient Yandex.Disk failures.
"""
from __future__ import annotations
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import time
from cloud_index import CloudStore
from verify_native_rolling import verify

SNAPSHOT='20261031T233108Z-5732b979'
KEY='archives/server-100/rolling/'+SNAPSHOT+'/progress.json'
PROGRESS=Path('/home/roomhacker/.local/state/grepmesh-native100-rolling-verify-progress.json')

class ImmutableCheckpointStore:
    def __init__(self,store,checkpoint):
        self.store=store
        self.checkpoint=checkpoint
    def get(self,key,limit):
        if key==KEY:
            if len(self.checkpoint)>limit:
                raise RuntimeError('Checkpoint exceeds verifier bound')
            return self.checkpoint
        return self.store.get(key,limit)

def persist(path,data):
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    temp=path.with_name(path.name+'.tmp.'+str(os.getpid()))
    with temp.open('x',encoding='utf-8') as f:
        os.chmod(temp,0o600)
        json.dump(data,f,ensure_ascii=False,sort_keys=True)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp,path)

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--seed-verified-prefix',type=int,default=None,
        help='Use ONLY after earlier sequential verification proves precisely this prefix')
    parser.add_argument('--max-new-chunks',type=int,default=0)
    parser.add_argument('--progress-path',type=Path,default=PROGRESS)
    args=parser.parse_args()
    if args.max_new_chunks<0:raise ValueError('Invalid bounded verification size')
    if os.getenv('GREPMESH_CLOUD_GATEWAY')!='local':
        raise RuntimeError('Requires existing authenticated local private cloud transport')
    lock=args.progress_path.with_suffix('.json.lock')
    lock.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    with lock.open('a+b') as fd:
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        store=CloudStore()
        checkpoint=store.get(KEY,16*1024*1024)
        checkpoint_sha=hashlib.sha256(checkpoint).hexdigest()
        state=json.loads(checkpoint)
        if (state.get('finished') is not True or
            state.get('safe_for_deletion') is not False or
            state.get('consistent_snapshot') is not False or
            state.get('snapshot')!=SNAPSHOT or
            state.get('rows')!=3187870 or
            len(state.get('chunks',[]))!=443):
            raise RuntimeError('Unexpected or unsafe source rolling checkpoint identity')
        total=len(state['chunks'])
        if args.progress_path.exists():
            progress=json.loads(args.progress_path.read_text(encoding='utf-8'))
            if (progress.get('checkpoint_sha256')!=checkpoint_sha or
                progress.get('snapshot')!=SNAPSHOT or
                progress.get('checkpoint_chunks')!=total or
                progress.get('consistent_snapshot') is not False or
                progress.get('safe_for_deletion') is not False):
                raise RuntimeError('Source checkpoint changed since the last verified part')
        else:
            seed=args.seed_verified_prefix
            if type(seed) is not int or not 0<=seed<=total:
                raise RuntimeError('Initial verified prefix required; never guess or replay silently')
            progress={'snapshot':SNAPSHOT,'checkpoint_sha256':checkpoint_sha,
                      'checkpoint_chunks':total,'verified_next_index':seed,
                      'confirmed_prior_source':'Sequential verifier stopped at chunk 119 after verifying 0..118; chunk 119 separately reverified',
                      'consistent_snapshot':False,'safe_for_deletion':False,
                      'source_sqlite_parity_verified':False}
            persist(args.progress_path,progress)
        if type(progress.get('verified_next_index')) is not int:
            raise RuntimeError('Invalid stored verified cursor type')
        start=progress['verified_next_index']
        if not 0<=start<=total:raise RuntimeError('Invalid stored verified cursor')
        view=ImmutableCheckpointStore(store,checkpoint)
        count=0
        for i in range(start,total):
            for attempt in range(4):
                try:
                    receipt=verify(view,SNAPSHOT,[i])
                    if receipt['verified_chunks']!=1 or receipt['all_chunks_replayed']:
                        raise RuntimeError('Unexpected per-chunk verifier receipt')
                    break
                except RuntimeError as e:
                    if 'Cloud read failed:' not in str(e) or attempt==3:
                        raise
                    print(json.dumps({'event':'cloud_transient_retry','part':i,'attempt':attempt+1}),flush=True)
                    time.sleep((attempt+1)*8)
            progress['verified_next_index']=i+1
            progress['last_verified_chunk_sha256']=state['chunks'][i]['sha256']
            progress['last_verified_utc_epoch']=int(time.time())
            persist(args.progress_path,progress)
            count+=1
            if (i+1)%20==0 or i+1==total:
                print(json.dumps({'event':'verified_progress','chunks':i+1,'total':total,
                                  'checkpoint_rows':state['rows'],'safe_for_deletion':False}),flush=True)
            if args.max_new_chunks and count>=args.max_new_chunks:
                break
        if progress['verified_next_index']==total:
            progress['all_chunks_replayed']=True
            persist(args.progress_path,progress)
            print(json.dumps({'event':'verified_all_provisional','checkpoint_chunks':total,
                              'checkpoint_rows':state['rows'],'consistent_snapshot':False,
                              'source_sqlite_parity_verified':False,'safe_for_deletion':False,
                              'ssd_reclaimed_bytes':0}),flush=True)

if __name__=='__main__':
    main()
