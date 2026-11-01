#!/usr/bin/env python3
"""Reconcile the published server-44 oversize index against the live FTS DB.

Read-only on both sides: the live SQLite is opened immutable/ro on
server-44, and the published index is read from the cloud prefix. Prints
per-path digest/byte agreement and an honest summary of differences.
"""
import hashlib,json,subprocess,sys

HOST='server-44'
SNAPSHOT='20261031T214414Z-ae84d605'
DB='/home/roomhacker/.cache/grepmesh/server-44-65415a79c0e91f4b.sqlite3'

remote='''
import hashlib,json,sqlite3,sys
db=sys.argv[1]
c=sqlite3.connect('file:'+db+'?mode=ro&immutable=1',uri=True)
out=[]
for rid,path,body in c.execute('SELECT id,c0,c1 FROM grepmesh_documents_content WHERE LENGTH(CAST(c1 AS BLOB))>1048576'):
    raw=body.encode('utf-8')
    out.append({'id':rid,'path':path,'bytes':len(raw),
                'sha256':hashlib.sha256(raw).hexdigest()})
print(json.dumps(out))
'''
proc=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10',HOST,
                      'python3','-',DB],input=remote,text=True,capture_output=True)
if proc.returncode:
    print('remote read failed:',proc.stderr[-400:]);sys.exit(1)
live=json.loads(proc.stdout.strip().splitlines()[-1])

from oversize_support import OversizeIndex
from cloud_index import CloudStore
index=OversizeIndex(CloudStore(),HOST,SNAPSHOT)
index.refresh()
published={r['path']:r for r in index.manifest['records']}

live_by_path={r['path']:r for r in live}
only_published=sorted(set(published)-set(live_by_path))
only_live=sorted(set(live_by_path)-set(published))
both=sorted(set(published)&set(live_by_path))
digest_mismatch=[];byte_delta=0
for path in both:
    if live_by_path[path]['sha256']!=published[path]['body_sha256']:
        digest_mismatch.append({'path':path,
            'live_bytes':live_by_path[path]['bytes'],
            'published_bytes':published[path]['size_bytes'],
            'live_sha256':live_by_path[path]['sha256'],
            'published_sha256':published[path]['body_sha256']})
    byte_delta+=live_by_path[path]['bytes']-published[path]['size_bytes']
print(json.dumps({'live_oversized_rows':len(live),'published_records':len(published),
    'paths_in_both':len(both),'only_published':len(only_published),
    'only_live':len(only_live),'digest_mismatch':len(digest_mismatch),
    'live_bytes_total':sum(r['bytes'] for r in live),
    'published_bytes_total':sum(r['size_bytes'] for r in published.values()),
    'byte_delta_shared_paths':byte_delta,
    'mismatch_sample':digest_mismatch[:3]},ensure_ascii=False,indent=1))
