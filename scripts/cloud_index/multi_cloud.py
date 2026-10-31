#!/usr/bin/env python3
"""Federated cloud text search over explicit, independently versioned scopes.

The same GrepMesh MCP host serves multiple roots, avoiding one discovery
peer per cloud corpus. Every scope's durable shards remain in Yandex.Disk,
while each reader uses only bounded tmpfs cache. No native index is changed.
"""
from __future__ import annotations
import argparse, json, os
from pathlib import Path
from cloud_index import CloudStore, Reader, ensure_tmpfs, safe_scope, serve, MIB, HOST

class MultiReader:
    def __init__(self, store, root, scopes, cache_mib=512):
        if not 1 <= len(scopes) <= 8:
            raise ValueError('An explicit list of 1..8 scopes is required')
        self.store=store
        self.scope='cloud-text-federation'
        self.readers=[]
        self.limit=cache_mib*MIB
        quota=self.limit//len(scopes)
        if quota < 64*MIB:
            raise ValueError('Give each scope at least 64 MiB of bounded cache')
        seen=set()
        for entry in scopes:
            scope=safe_scope(entry['scope'])
            source_host=entry['source_host']
            source_root=entry['source_root']
            if scope in seen:raise ValueError('Duplicate scope')
            if not source_host or not Path(source_root).is_absolute() or '..' in Path(source_root).parts:
                raise ValueError('Invalid cloud source admission')
            seen.add(scope)
            self.readers.append(Reader(store,root,scope,quota,source_host,source_root))
        self.manifest=None

    @property
    def used(self):return sum(x.used for x in self.readers)

    @property
    def locations(self):
        return [{'host':HOST,'name':r.scope,'path':r.manifest['source_root']}
                for r in self.readers]

    def refresh(self):
        manifests=[]
        for r in self.readers:
            m=r.refresh()
            # Keep tiny path-only indexes hot; glob lookups then avoid cold text shards.
            if m.get('path_index'):r.load(m['path_index'])
            manifests.append(m)
        self.manifest={
            'scope':self.scope,'source_root':'/',
            'source_host':'multiple','documents':sum(x['documents'] for x in manifests),
            'parts':[part for m in manifests for part in m['parts']],
            'policy':'explicit cloud-scoped text sources only; per-source coverage in locations',
            'generated_at':max(m['generated_at'] for m in manifests),
        }
        return self.manifest

    def query(self,query,limit=20,glob=False,case_sensitive=False,roots=None,
              path_globs=None,deadline_seconds=25,authorized_roots=None):
        if self.manifest is None:self.refresh()
        limit=max(1,min(int(limit),200))
        selected=[]
        for r in self.readers:
            source=r.manifest['source_root']
            if roots and not any(
                x in ('*','local',self.scope,r.scope,source)
                or (isinstance(x,str) and x.startswith(source.rstrip('/')+'/'))
                or (isinstance(x,str) and source.startswith(x.rstrip('/')+'/'))
                for x in roots):
                continue
            selected.append(r)
        if not selected and roots:
            raise ValueError('Requested root is outside the cloud corpus set')
        results=[];seen=set();warnings=[];truncated=False;cloud_fetches=0;elapsed_ms=0
        # Each scope has an independently verified immutable manifest.
        for reader in selected:
            try:
                out=reader.query(
                    query,limit=limit+1,glob=glob,case_sensitive=case_sensitive,
                    roots=None if not roots or any(x in ('local','*',self.scope) for x in roots) else roots,
                    path_globs=path_globs,deadline_seconds=deadline_seconds,
                    authorized_roots=authorized_roots)
                cloud_fetches+=out['cloud_fetches']
                elapsed_ms+=out['elapsed_ms']
                truncated |= bool(out['truncated'])
                if out['partial']:
                    warnings.extend(reader.scope+': '+str(s) for s in out.get('warnings',[]))
                for hit in out['results']:
                    ident=(hit['source_host'],hit['path'])
                    if ident not in seen:
                        seen.add(ident);results.append(hit)
            except Exception as error:
                warnings.append(reader.scope+': '+type(error).__name__+': '+str(error)[:160])
        truncated |= len(results)>limit
        return {
            'results':results[:limit],'partial':bool(warnings),'truncated':truncated,
            'warnings':warnings,'content_indexed':True,'scope':self.scope,
            'source_documents':self.manifest['documents'],
            'cache_bytes':self.used,'cache_limit_bytes':self.limit,
            'cloud_fetches':cloud_fetches,'elapsed_ms':round(elapsed_ms,2),
            'complete_for_policy':True,
        }

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('action',choices=['serve'])
    ap.add_argument('--config',type=Path,required=True)
    ap.add_argument('--root',type=Path,default=Path('/run/user')/str(os.getuid())/'grepmesh-cloud-index')
    ap.add_argument('--cache-mib',type=int,default=512)
    ap.add_argument('--port',type=int,default=19435)
    args=ap.parse_args()
    ensure_tmpfs(args.root)
    scopes=json.loads(args.config.read_text())
    if not isinstance(scopes,list):raise ValueError('Cloud scope configuration must be a list')
    reader=MultiReader(CloudStore(),args.root,scopes,args.cache_mib)
    serve(reader,args.port)

if __name__=='__main__':main()
