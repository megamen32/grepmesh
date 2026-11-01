#!/usr/bin/env python3
"""Synthetic end-to-end test for the publish_native_oversize scan+verify flow."""
import os,pathlib
from cloud_index import MIB,MAX_DOCUMENT,MemoryStore,ensure_tmpfs
import publish_native_oversize
from oversize_support import publish_oversize
BASE=pathlib.Path('/run/user')/str(os.getuid())/'grepmesh-oversize-publish-tests'
ensure_tmpfs(BASE)
checks=[]
def check(label,ok):
    assert ok,label;checks.append(label)

bodies=['x'*(MAX_DOCUMENT+7)+'\n','y'*(MAX_DOCUMENT+11),
        '\n'.join(['a']*700000)+'\n','small one\n','two\n']
rows=[(i,None,'/src/legx/big%d.log'%i,body) for i,body in enumerate(bodies)]

docs=list(publish_native_oversize.oversize_docs(rows))
check('scan selects only oversize rows',
      [d['path'] for d in docs]==['/src/legx/big0.log','/src/legx/big1.log',
                                  '/src/legx/big2.log'])

try:list(publish_native_oversize.oversize_docs(
        [(1,None,'relative/path','body'*(MAX_DOCUMENT//12+1))]))
except ValueError:check('non-text or relative archive row refused',True)
else:raise AssertionError('malformed archive row accepted')
try:list(publish_native_oversize.oversize_docs(
        [(1,None,'/ok/path',None)]))
except ValueError:check('binary row refused',True)
else:raise AssertionError('non-text archive row accepted')
try:list(publish_native_oversize.oversize_docs(
        [(1,None,'/ok/path','x'*(MAX_DOCUMENT+3)) for _ in ()] +
        [(1,None,'/ok/path',123)]))
except ValueError:check('numeric body refused',True)
else:raise AssertionError('non-string body accepted')

st=MemoryStore()
result=publish_oversize(st,'server-99','snap-t',docs)
check('synthetic publish count matches scan',result['documents']==3)
admission=publish_native_oversize.verify(st,'server-99','snap-t',3,
                                         expected_rows_min=1)
check('admission re-reads the index and runs a canary',
      sorted(c['path'] for c in admission['canary'])==
      ['/src/legx/big0.log','/src/legx/big1.log','/src/legx/big2.log'] and
      admission['total_bytes']==sum(len(d['body'].encode('utf-8')) for d in docs) and
      all(0<c['chunk1_raw_bytes']<=MAX_DOCUMENT for c in admission['canary']) and
      all(c['body_sha256'] for c in admission['canary']))
try:publish_native_oversize.verify(st,'server-99','snap-t',4)
except RuntimeError:check('expectation mismatch enforced',True)
else:raise AssertionError('wrong expected count accepted')

print(str(len(checks))+' checks passed')
print('PASS')
