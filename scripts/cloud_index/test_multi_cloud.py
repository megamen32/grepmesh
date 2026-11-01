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
    historical=a.read_text('/src/code/alpha.md',start_line=1,end_line=1,authorized_roots=['/src/code'])
    check('multi-scope exact historical read',historical['chunks'][0]['lines'][0]['text']=='alpha both shared')
    try:a.read_text('/src/code/alpha.md',authorized_roots=['/src/documents'])
    except PermissionError:check('multi-scope historical read ACL deny',True)
    else:raise AssertionError('Multi-cloud ACL bypass')
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
    r=a.query(r'FINANCE (confidential|misc)',regex=True)
    check('regex across federated scopes',len(r['results'])==1 and r['results'][0]['path'].endswith('document.docx') and not r['partial'])
    r=a.query(r'(?i)finance (confidential|misc)',regex=True)
    check('regex inline case flag on federated scopes',len(r['results'])==1 and not r['partial'])
    try:a.query('financ(e',regex=True)
    except ValueError:check('invalid regex fails fast on federated reader',True)
    else:raise AssertionError('invalid regex silently passed federation')
    r=a.query(r'ZZZ-never[0-9]*present',regex=True)
    check('federated regex pruning touches no scope shards',not r['results'] and r['cloud_fetches']==0 and not r['partial'])
    paths=a.query('*.pdf',glob=True)
    check('filename glob across two scopes',len(paths['results'])==1)
    check('preloaded path indexes require zero cloud fetches',paths['cloud_fetches']==0)
    check('Cyrillic OCR retrieval',len(a.query('бизнес')['results'])==1)
    check('duplicate-free result paths',len({(x['source_host'],x['path']) for x in r['results']})==len(r['results']))
    try:a.query('shared',roots=['/src/nope'])
    except ValueError:check('unknown query root denied',True)
    else:raise AssertionError('Unknown root should not be silently accepted')

# Federated oversize support: bodies above the shard limit read through the
# index, scoped to the admitting source root, with the same ACL semantics.
from oversize_support import publish_oversize
with tempfile.TemporaryDirectory(dir=BASE) as tmp:
    import hashlib
    st=MemoryStore();pub=Publisher(st,tmp,64*1024)
    pub.publish('beta',stream('/src/beta','server-100',[('gamma.rs','beta tail text')]))
    pub.publish('alpha',stream('/src/alpha','server-100',[('small.md','alpha regular text')]))
    body=(''.join('line %d alpha\u00e9\n' % i for i in range(5000))+
          'M'*400000+'\n'+'X'*700000+'\ntail\n')
    assert len(body.encode('utf-8'))>1048576
    expected=body.splitlines()
    publish_oversize(st,'server-100','snap-f',
                     [{'path':'/src/alpha/big.log','body':body},
                      {'path':'/elsewhere/huge.log','body':body}])
    scopes=[{'scope':'beta','source_root':'/src/beta','source_host':'server-100'},
            {'scope':'alpha','source_root':'/src/alpha','source_host':'server-100',
             'oversize':{'host':'server-100','snapshot':'snap-f'}}]
    fed=MultiReader(st,tmp,scopes,cache_mib=128)
    fed.refresh()
    check('federated manifest lists oversize entries',
          fed.manifest['oversize_entries']==[{'scope':'alpha','host':'server-100',
              'snapshot':'snap-f','pointer_key':'oversize/server-100/snap-f/latest.json'}])
    result=fed.read_text('/src/alpha/big.log',start_line=4999,end_line=5001,
                         authorized_roots=['/src'],deadline_seconds=25)
    check('federated oversize read with line parity',
          result['chunks'][0]['lines']==[{'line_number':4999,'text':expected[4998]},
              {'line_number':5000,'text':expected[4999]},
              {'line_number':5001,'text':'M'*400000}] and
          len(result['chunks'][0]['lines'])==3)
    check('federated oversize body digest matches',
          result['body_sha256']==__import__('hashlib').sha256(
              body.encode('utf-8')).hexdigest())
    try:fed.read_text('/src/alpha/big.log',1,2,authorized_roots=['/src/beta'],
                      deadline_seconds=25)
    except PermissionError:check('federated oversize ACL deny',True)
    else:raise AssertionError('federated oversize ACL bypass')
    beyond_end=fed.read_text('/src/alpha/big.log',start_line=10**8,deadline_seconds=25)
    check('federated oversize read beyond end is empty',
          beyond_end['chunks'][0]['lines']==[])
    try:fed.read_text('/elsewhere/huge.log',start_line=1,deadline_seconds=25)
    except FileNotFoundError:check('oversize path outside admitted roots is denied',True)
    else:raise AssertionError('oversize index read escaped its scope root')
    late=fed.read_text('/src/alpha/big.log',start_line=len(expected)+5,
                       deadline_seconds=25)
    check('federated oversize read past the last line is empty',
          late['chunks'][0]['lines']==[])
    plain=fed.read_text('/src/beta/gamma.rs',start_line=1,end_line=1,
                        authorized_roots=['/src/beta'],deadline_seconds=25)
    check('plain scope historical read unchanged',
          plain['chunks'][0]['lines'][0]['text']=='beta tail text')
    hits=fed.query('tail')
    check('federated full text includes regular and oversized documents',
          not hits['partial'] and not hits['truncated'] and
          {x['path'] for x in hits['results']}==
          {'/src/beta/gamma.rs','/src/alpha/big.log'})
    beta_only=fed.query('tail',authorized_roots=['/src/beta'])
    check('federated full-text ACL excludes oversized other roots',
          not beta_only['partial'] and
          {x['path'] for x in beta_only['results']}=={'/src/beta/gamma.rs'})
    alpha_only=fed.query('tail',authorized_roots=['/src/alpha'])
    check('federated full-text ACL admits only authorized oversize',
          not alpha_only['partial'] and
          {x['path'] for x in alpha_only['results']}=={'/src/alpha/big.log'})
    check('empty verified ACL denies all federated text',
          not fed.query('tail',authorized_roots=[])['results'])
    regexp=fed.query('^tail$',regex=True)
    check('oversized historical body participates in bounded regex',
          not regexp['partial'] and
          {x['path'] for x in regexp['results']}=={'/src/alpha/big.log'})
    globbed=fed.query('*.log',glob=True)
    check('oversized historical path participates in glob',
          not globbed['partial'] and
          {x['path'] for x in globbed['results']}=={'/src/alpha/big.log'})
    # Oversized bodies are searchable through the same federation query.
    hit=fed.query('alpha')
    check('federated query finds a literal inside an oversized body',
          any(r['path']=='/src/alpha/big.log' and r['line_number']==1
              for r in hit['results']))
    check('oversize hit carries the body digest',
          any(r['body_sha256']==__import__('hashlib').sha256(
              body.encode('utf-8')).hexdigest()
              for r in hit['results']))
    miss=fed.query('nonexistent-token-xyz')
    check('federated query misses honestly in oversized bodies',
          not any(r['path']=='/src/alpha/big.log' for r in miss['results']))
    regex_hit=fed.query('line 4999',regex=True)
    check('federated regex search reaches oversized bodies',
          any(r['path']=='/src/alpha/big.log' and r['line_number']==5000
              for r in regex_hit['results']))
    anchored=fed.query('^line 4999 ',regex=True)
    check('federated regex anchors work inside oversized bodies',
          any(r['path']=='/src/alpha/big.log' and r['line_number']==5000
              for r in anchored['results']))
    denied=fed.query('alpha',authorized_roots=['/src/beta'])
    check('federated oversize search respects the read ACL',
          not any(r['path']=='/src/alpha/big.log' for r in denied['results']))
    globbed=fed.query('*big.log',glob=True)
    check('federated glob finds the oversized path',
          any(r['path']=='/src/alpha/big.log' for r in globbed['results']))
    try:MultiReader(st,tmp,[{'scope':'z','source_root':'/z','source_host':'h',
                             'oversize':{'snapshot':'s','host':'bad/host'}}],cache_mib=64)
    except ValueError:check('invalid oversize host rejected at init',True)
    else:raise AssertionError('bad oversize identity accepted')
    try:MultiReader(st,tmp,[{'scope':'z','source_root':'/z','source_host':'h',
                             'oversize':{'snapshot':'s','host':'server-100',
                                         'pointer_key':'oversize/other/latest.json'}}],
                    cache_mib=64)
    except ValueError:check('oversize pointer mismatch rejected at init',True)
    else:raise AssertionError('pointer mismatch accepted')
    try:MultiReader(st,tmp,[{'scope':'z','source_root':'/z','source_host':'h',
                             'oversize':{}}],cache_mib=64)
    except ValueError:check('oversize needs host and snapshot',True)
    else:raise AssertionError('empty oversize admission accepted')
print(json.dumps({'passed':True,'tests':len(checks),'cases':checks},ensure_ascii=False))
