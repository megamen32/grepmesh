#!/usr/bin/env python3
"""Synthetic two-root federated cloud retrieval and ACL isolation tests."""
import io,json,os,pathlib,tempfile,threading
from unittest.mock import patch
from cloud_index import ensure_tmpfs,MemoryStore,Publisher,Reader,document,json_bytes
from multi_cloud import MultiReader
BASE=pathlib.Path('/run/user')/str(os.getuid())/'grepmesh-cloud-multi-tests'
ensure_tmpfs(BASE)
checks=[]
def check(label,ok):
    assert ok,label;checks.append(label)
def stream(root,host,paths):
    import hashlib
    rows=[{'kind':'begin','format':1,'source_host':host,'source_root':root,'policy':'fixture'}]
    digest=hashlib.sha256()
    for p,b in paths:
        d=document(root+'/'+p,b)
        rows.append(d);digest.update(json_bytes([d['path'],d['sha256']]))
    rows.append({'kind':'end','count':len(paths),'logical_sha256':digest.hexdigest(),'skipped':{},'complete_for_policy':True})
    return io.StringIO(''.join(json.dumps(x,ensure_ascii=False)+'\n' for x in rows))
with tempfile.TemporaryDirectory(dir=BASE) as tmp:
    st=MemoryStore();pub=Publisher(st,tmp,64*1024)
    pub.publish('code',stream('/src/code','server-100',[('alpha.md','alpha both shared'),('beta.rs','beta source')]))
    pub.publish('ocr',stream('/src/documents','server-100',[('report.pdf','бизнес OCR shared'),('document.docx','FINANCE confidential text')]))
    scopes=[{'scope':'code','source_root':'/src/code','source_host':'server-100'},
            {'scope':'ocr','source_root':'/src/documents','source_host':'server-100'}]
    a=MultiReader(st,tmp,scopes,cache_mib=128)
    a.refresh()
    check('two cloud locations appear',len(a.locations)==2)
    check('independent manifests counted',a.manifest['documents']==4)
    r=a.query('shared')
    check('cross-corpus text search',len(r['results'])==2 and not r['partial'])
    barrier=threading.Barrier(2)
    original_query=Reader.query
    def synchronized_query(self,*args,**kwargs):
        barrier.wait(timeout=2.0)
        return original_query(self,*args,**kwargs)
    with patch.object(Reader,'query',synchronized_query):
        concurrent=a.query('*.md',glob=True)
    check('independent path-scope queries actually run concurrently',
          len(concurrent['results'])==1 and not concurrent['partial'])
    check('RAM cache budget',a.used<=128*1024*1024)
    check('source-root narrowing',len(a.query('shared',roots=['/src/code'])['results'])==1)
    check('ACL isolates restricted source',len(a.query('shared',authorized_roots=['/src/documents'])['results'])==1)
    check('ACL denies everything',not a.query('shared',authorized_roots=[])['results'])
    paths=a.query('*.pdf',glob=True)
    check('filename glob across two scopes',len(paths['results'])==1)
    check('preloaded path indexes require zero cloud fetches',paths['cloud_fetches']==0)
    check('Cyrillic OCR retrieval',len(a.query('бизнес')['results'])==1)
    check('duplicate-free result paths',len({(x['source_host'],x['path']) for x in r['results']})==len(r['results']))
    try:a.query('shared',roots=['/src/nope'])
    except ValueError:check('unknown query root denied',True)
    else:raise AssertionError('Unknown root should not be silently accepted')
print(json.dumps({'passed':True,'tests':len(checks),'cases':checks},ensure_ascii=False))
