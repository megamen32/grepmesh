#!/usr/bin/env python3
"""Resumable full cold oversize payload SHA-256 audit for private server-44.

Reassembles and checks each byte of all 75 historical >1 MiB text bodies
from the immutable cloud archive; no native SQLite, no public MCP, no ACL
changes and never a deletion authorization. Completed document proofs are
checkpointed so later runs don't repeat already verified cloud downloads.
"""
import fcntl
import hashlib
import json
import os
import pathlib
import time
import zlib

from cloud_index import CloudStore, digest, json_bytes
from oversize_support import OversizeIndex, oversize_pointer_key

HOST='server-44'
SNAPSHOT='20261031T214414Z-ae84d605'
PROGRESS=pathlib.Path('/home/roomhacker/.local/state/grepmesh-native44-oversize-audit-progress.json')
RECEIPT=pathlib.Path('/home/roomhacker/.local/state/grepmesh-native44-oversize-payload-parity.json')

def persist(path,doc):
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    temp=path.with_name(path.name+'.tmp.'+str(os.getpid()))
    fd=os.open(temp,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f:
        json.dump(doc,f,sort_keys=True)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp,path)

def fetched(store,chunk):
    length=int(chunk['packed_bytes'])
    for attempt in range(4):
        try:
            data=store.get(chunk['key'],length)
            if len(data)!=length:
                raise RuntimeError('Cloud oversized chunk truncated')
            return data
        except RuntimeError as exc:
            if 'Cloud read failed:' not in str(exc) or attempt==3:
                raise
            print(json.dumps({'event':'transient_oversize_cloud_retry','attempt':attempt+1}),flush=True)
            time.sleep((attempt+1)*8)
    raise AssertionError('unreachable')

def main():
    if os.getenv('GREPMESH_CLOUD_GATEWAY')!='local':
        raise RuntimeError('Existing private Yandex.Disk authorization only')
    PROGRESS.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    with PROGRESS.with_suffix('.lock').open('a+b') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        store=CloudStore()
        index=OversizeIndex(store,HOST,SNAPSHOT)
        index.refresh()
        records=index.manifest['records']
        assert len(records)==75
        pointer=json.loads(store.get(oversize_pointer_key(HOST,SNAPSHOT),65536))
        source_sha=pointer['sha256']
        if RECEIPT.exists():
            done=json.loads(RECEIPT.read_text())
            if (done.get('manifest_sha256')==source_sha
                    and done.get('verified_documents')==75
                    and done.get('verified_chunks')==672
                    and done.get('verified_body_sha256_all') is True):
                print(json.dumps({'event':'already_fully_verified','documents':75,'chunks':672,'safe_for_deletion':False}))
                return
            raise RuntimeError('Existing oversize verification receipt conflicts with immutable archive')
        if PROGRESS.exists():
            progress=json.loads(PROGRESS.read_text())
            if (progress.get('manifest_sha256')!=source_sha
                or progress.get('snapshot')!=SNAPSHOT
                or progress.get('expected_documents')!=75):
                raise RuntimeError('Oversize audit checkpoint identity changed')
        else:
            progress={'snapshot':SNAPSHOT,'manifest_sha256':source_sha,
                'expected_documents':75,'verified_next_record':0,
                'verified_chunks':0,'verified_raw_bytes':0,
                'safe_for_deletion':False}
            persist(PROGRESS,progress)
        start=progress['verified_next_record']
        if type(start) is not int or not 0<=start<=75:
            raise RuntimeError('Invalid previous audit progress')
        started=time.monotonic()
        for i in range(start,len(records)):
            item=records[i]
            size=0
            h=hashlib.sha256()
            for chunk in item['chunks']:
                packed=fetched(store,chunk)
                if digest(packed)!=chunk['packed_sha256']:
                    raise RuntimeError('Oversize archived compressed payload SHA mismatch')
                decoder=zlib.decompressobj()
                raw=decoder.decompress(packed,int(chunk['raw_bytes'])+1)
                if (len(raw)!=chunk['raw_bytes'] or not decoder.eof
                        or decoder.unused_data or decoder.unconsumed_tail
                        or digest(raw)!=chunk['sha256']):
                    raise RuntimeError('Oversize archived raw payload SHA mismatch')
                h.update(raw)
                size+=len(raw)
            if h.hexdigest()!=item['body_sha256'] or size!=item['size_bytes']:
                raise RuntimeError('Oversize historical body could not be reconstructed')
            progress['verified_next_record']=i+1
            progress['verified_chunks']+=len(item['chunks'])
            progress['verified_raw_bytes']+=size
            persist(PROGRESS,progress)
            if (i+1)%10==0 or i+1==len(records):
                print(json.dumps({'event':'oversize_full_payload_audit_progress',
                    'verified_documents':i+1,
                    'verified_chunks':progress['verified_chunks'],
                    'elapsed_s':round(time.monotonic()-started,1)}),flush=True)
        if (progress['verified_next_record']!=75
                or progress['verified_chunks']!=672
                or progress['verified_raw_bytes']!=325853221):
            raise RuntimeError('Historical oversize data coverage incomplete')
        result={'snapshot':SNAPSHOT,'source':HOST,'manifest_sha256':source_sha,
            'verified_documents':75,'verified_chunks':672,
            'verified_raw_bytes':325853221,'verified_body_sha256_all':True,
            'safe_for_deletion':False,'live_source_update_parity':False,
            'production_verified_acl_accepted':False,'ssd_reclaimed_bytes':0}
        persist(RECEIPT,result)
        print(json.dumps({'event':'verified_all_historical_oversize_payloads',**result}),flush=True)

if __name__=='__main__':
    main()
