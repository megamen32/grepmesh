#!/usr/bin/env python3
"""Verify the frozen rebuild's exact-read routing and ACL over actual shards."""
import hashlib,io,json,os,tempfile
from pathlib import Path
from unittest.mock import patch
from cloud_index import ensure_tmpfs,MemoryStore,Publisher,Reader,document,json_bytes
from multi_cloud import MultiReader
root=Path('/run/user')/str(os.getuid())/'grepmesh-frozen-read-routing-tests'
ensure_tmpfs(root)
with tempfile.TemporaryDirectory(dir=root) as temp:
    store=MemoryStore(); publisher=Publisher(store,temp,65536); scopes=[]; paths={}
    for i in range(16):
        for n in range(10000):
            path=f'/private/snapshot/doc-{i}-{n}.txt'
            if int.from_bytes(hashlib.sha256(path.encode()).digest()[:8],'big')%16==i:break
        paths[i]=path; d=document(path,f'content-{i}')
        digest=hashlib.sha256(json_bytes([path,d['sha256']])).hexdigest()
        records=[{'kind':'begin','format':1,'source_host':'server-44','source_root':'/','policy':'frozen fixture'},d,{'kind':'end','count':1,'logical_sha256':digest,'skipped':{},'complete_for_policy':True}]
        scope=f'frozen44-20261105-b{i:02d}'
        publisher.publish(scope,io.StringIO(''.join(json.dumps(x)+'\n' for x in records)))
        scopes.append({'scope':scope,'source_host':'server-44','source_root':'/'})
    reader=MultiReader(store,temp,scopes,cache_mib=1024); reader.refresh()
    visited=[]; original=Reader.read_text
    def record(self,*a,**kw):
        visited.append(self.scope); return original(self,*a,**kw)
    with patch.object(Reader,'read_text',record):
        for i,path in paths.items():
            visited.clear(); got=reader.read_text(path,1,1,authorized_roots=['/private/snapshot'])
            assert got['chunks'][0]['lines'][0]['text']==f'content-{i}'
            assert visited==[f'frozen44-20261105-b{i:02d}'],visited
        try:reader.read_text(paths[5],authorized_roots=['/outside'])
        except PermissionError:pass
        else:raise AssertionError('ACL bypass')
    incomplete=MultiReader(store,temp,scopes[:-1],cache_mib=1024); incomplete.refresh()
    visited.clear()
    with patch.object(Reader,'read_text',record):
        got=incomplete.read_text(paths[14],1,1,authorized_roots=['/private/snapshot'])
    assert got['chunks'][0]['lines'][0]['text']=='content-14'
    assert len(visited)==15,visited  # No hash-routing claim for incomplete sets.
    print(json.dumps({'event':'frozen_route_and_acl_passed','documents':16}))
