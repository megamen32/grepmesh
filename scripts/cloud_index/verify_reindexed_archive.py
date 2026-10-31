#!/usr/bin/env python3
"""Independent admission gate for private historical GrepMesh FTS snapshots.

Verify all bucket manifests, declared document counts, the archived original
count and exclusions before a private (never public) loopback search preview.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path

from cloud_index import CloudStore, MAX_MANIFEST, digest, MIB
from reindex_native_archive import BUCKETS, get_manifest


def check(store, host, snapshot):
    if host != 'server-44':
        raise ValueError('Only server44 private review is enabled')
    archived=get_manifest(store,host,snapshot)
    expected=archived['tables']['grepmesh_documents_content']['rows']
    if expected != 849796:
        raise ValueError('Native archive count drift')
    total=0
    buckets=[]
    misses=None
    for i in range(BUCKETS):
        scope=f'legacy-{host.replace("-","")}-{i}'
        raw=store.get('scopes/'+scope+'/latest.json',65536)
        receipt=json.loads(raw)
        blob=store.get(receipt['manifest'],MAX_MANIFEST)
        if digest(blob)!=receipt['sha256']:
            raise ValueError('Cloud index manifest checksum mismatch')
        m=json.loads(blob)
        if (m['scope']!=scope or m['source_host']!=host or m['source_root']!='/'
            or not m['complete_for_policy'] or m['index_layout']!='fts5-trigram-detail-none-v2'):
            raise ValueError('Cloud scope identity/policy mismatch')
        if m['path_index']['documents']!=m['documents']:
            raise ValueError('Path-only index accounting mismatch')
        if sum(p['documents'] for p in m['parts'])!=m['documents']:
            raise ValueError('Search shard accounting mismatch')
        if len(m['parts'])>500:
            raise ValueError('Cloud search scope exceeds query budget')
        if misses is None:misses=m['skipped']
        elif m['skipped']!=misses:
            raise ValueError('Cloud scopes disagree about skipped source')
        for part in m['parts']+[m['path_index']]:
            if part['raw_bytes']>64*MIB or part['packed_bytes']>64*MIB:
                raise ValueError('Cloud index shard exceeds cache limit')
        buckets.append({'scope':scope,'documents':m['documents'],'shards':len(m['parts']),
                        'cloud_bytes':sum(p['packed_bytes'] for p in m['parts'])})
        total+=m['documents']
        print(json.dumps({'event':'bucket_admitted',**buckets[-1]}),flush=True)
    if total != 849721 or total+sum(misses.values())!=expected:
        raise ValueError('Archived and searchable document counts do not reconcile')
    result={'admitted_private':True,'host':host,'source_archive':snapshot,
            'archived_documents':expected,'searchable_documents':total,
            'cold_only_oversize':misses.get('oversized_historical_body',0),
            'unadmitted_other':sum(misses.values())-misses.get('oversized_historical_body',0),
            'scopes':buckets,'cloud_reads':store.gets}
    print(json.dumps({'event':'historical_fulltext_manifest_verified',**result}),flush=True)
    return result


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--host',default='server-44')
    p.add_argument('--snapshot',required=True)
    args=p.parse_args()
    check(CloudStore(),args.host,args.snapshot)


if __name__=='__main__':main()
