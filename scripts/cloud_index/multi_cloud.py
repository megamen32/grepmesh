#!/usr/bin/env python3
"""Federated cloud text search over explicit, independently versioned scopes.

The same GrepMesh MCP host serves multiple roots, avoiding one discovery
peer per cloud corpus. Every scope's durable shards remain in Yandex.Disk,
while each reader uses only bounded tmpfs cache. No native index is changed.
"""
from __future__ import annotations
import argparse, fnmatch, json, os, time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from cloud_index import CloudStore, Reader, ensure_tmpfs, safe_scope, serve, MIB, HOST, \
    compile_bounded_regex, regex_match_positions
from oversize_support import OversizeIndex, oversize_pointer_key

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
        self.oversize=[]
        for entry in scopes:
            scope=safe_scope(entry['scope'])
            source_host=entry['source_host']
            source_root=entry['source_root']
            if scope in seen:raise ValueError('Duplicate scope')
            if not source_host or not Path(source_root).is_absolute() or '..' in Path(source_root).parts:
                raise ValueError('Invalid cloud source admission')
            seen.add(scope)
            self.readers.append(Reader(store,root,scope,quota,source_host,source_root))
            spec=entry.get('oversize')
            if spec is not None:
                # Historical bodies above the 1 MiB shard limit live in a
                # separate per-snapshot oversize index; admissibility is the
                # scope's own source root, so federated reads stay scoped.
                try:pointer=oversize_pointer_key(spec['host'],spec['snapshot'])
                except (KeyError,TypeError,ValueError) as error:
                    raise ValueError('Invalid oversize admission') from error
                if spec.get('pointer_key') and spec['pointer_key']!=pointer:
                    raise ValueError('Oversize pointer mismatch')
                self.oversize.append({'scope':scope,'source_root':source_root,
                                      'source_host':source_host,
                                      'host':spec['host'],
                                      'snapshot':spec['snapshot'],
                                      'pointer_key':pointer,'index':None})
        self.manifest=None

    def _oversize_index(self,entry):
        """Lazy-load one oversize index; failures stay honest on first use."""
        if entry['index'] is None:
            entry['index']=OversizeIndex(self.store,entry['host'],entry['snapshot'])
        return entry['index']

    @property
    def used(self):return sum(x.used for x in self.readers)

    @property
    def locations(self):
        return [{'host':HOST,'name':r.scope,'path':r.manifest['source_root']}
                for r in self.readers]

    def refresh(self):
        def start_reader(reader):
            m=reader.refresh()
            # Hot path-only indexes are independent per scope and stay in tmpfs.
            if m.get('path_index'):reader.load(m['path_index'])
            return m
        # Initialize independent scopes concurrently before accepting requests.
        # A failed manifest or hash aborts startup rather than dropping coverage.
        with ThreadPoolExecutor(max_workers=min(4,len(self.readers))) as pool:
            manifests=list(pool.map(start_reader,self.readers))
        self.manifest={
            'scope':self.scope,'source_root':'/',
            'source_host':'multiple','documents':sum(x['documents'] for x in manifests),
            'parts':[part for m in manifests for part in m['parts']],
            'policy':'explicit cloud-scoped text sources only; per-source coverage in locations',
            'generated_at':max(m['generated_at'] for m in manifests),
            'oversize_entries':[{'scope':e['scope'],'host':e['host'],
                                 'snapshot':e['snapshot'],
                                 'pointer_key':e['pointer_key']}
                                for e in self.oversize],
        }
        return self.manifest

    def read_text(self,path,start_line=1,end_line=None,authorized_roots=None,deadline_seconds=25):
        """Exact historical read. Hash-route the original 8-way archive, never fan out."""
        if not isinstance(path,str) or not path.startswith('/') or '..' in Path(path).parts:
            raise ValueError('Absolute historical path required')
        if self.manifest is None:self.refresh()
        legacy={int(r.scope.rsplit('-',1)[-1]):r for r in self.readers
                if r.scope.startswith('legacy-server44-') and r.scope.rsplit('-',1)[-1].isdigit()}
        if len(legacy)==8 and set(legacy)==set(range(8)):
            import hashlib
            bucket=hashlib.sha256(path.encode('utf-8')).digest()[0]%8
            selected=[legacy[bucket]]
        else:
            selected=sorted((r for r in self.readers
                             if path==r.manifest['source_root'] or
                             path.startswith(r.manifest['source_root'].rstrip('/')+'/')),
                            key=lambda r:len(r.manifest['source_root']),reverse=True)
        started=time.monotonic()
        for reader in selected:
            remaining=deadline_seconds-(time.monotonic()-started)
            if remaining<=0:raise TimeoutError('Global historical read deadline')
            try:
                return reader.read_text(path,start_line,end_line,
                                        authorized_roots=authorized_roots,
                                        deadline_seconds=remaining)
            except FileNotFoundError:
                continue
        # Route miss: oversized historical bodies live outside the search
        # shards, so consult each scope's lazily-loaded oversize index. A
        # path stays scoped to the admitting source root; nothing fans out.
        for entry in self.oversize:
            root=entry['source_root']
            if not (path==root or path.startswith(root.rstrip('/')+'/')):continue
            remaining=deadline_seconds-(time.monotonic()-started)
            if remaining<=0:raise TimeoutError('Global historical read deadline')
            try:
                return self._oversize_index(entry).read_text(
                    path,start_line,end_line,authorized_roots=authorized_roots,
                    deadline_seconds=remaining)
            except FileNotFoundError:
                continue
        raise FileNotFoundError('Historical path is not in searchable cloud scopes')

    def _oversize_query_hits(self,entry,query,remaining_hits,glob,case_sensitive,
                             path_globs,authorized_roots,deadline,regex=False,
                             compiled=None):
        """Scan one scope's oversize index; returns (hits, warnings)."""
        warnings=[];hits=[]
        try:
            index=self._oversize_index(entry)
        except Exception as error:
            return [],[entry['scope']+': '+type(error).__name__+': '+str(error)[:140]]
        try:
            with index.lock:
                index._require()
                root=entry['source_root']
                needle='' if glob else (query if case_sensitive else query.casefold())
                records=sorted(index.by_path.values(),key=lambda r:r['path'])
                for record in records:
                    if time.monotonic()>=deadline:
                        warnings.append(entry['scope']+
                            ': oversize scan deadline; remaining documents not searched')
                        break
                    path=record['path']
                    if not (path==root or path.startswith(root.rstrip('/')+'/')):continue
                    if authorized_roots is not None and not any(
                            path==v or path.startswith(v.rstrip('/')+'/')
                            for v in authorized_roots):continue
                    if path_globs and not any(fnmatch.fnmatchcase(path,g)
                                              for g in path_globs):continue
                    if glob:
                        probe=query.casefold()
                        if not (fnmatch.fnmatchcase(path.casefold(),probe) or
                                fnmatch.fnmatchcase(Path(path).name.casefold(),probe)):
                            continue
                        line,column,text=0,0,''
                    else:
                        if regex:
                            if compiled is None:
                                compiled=compile_bounded_regex(query)
                                if compiled is None:
                                    return [],warnings
                            found=index.first_match(record,deadline,
                                regex_pair=compiled)
                        else:
                            found=index.first_match(record,deadline,
                                needle=needle,case_sensitive=case_sensitive)
                        if found is None:
                            probe=needle
                            python_path=path if case_sensitive else path.casefold()
                            if probe and probe in python_path:
                                line,column,text=0,0,'[path match] '+path
                            else:continue
                        else:
                            line,column,text=found
                    hits.append({'host_id':HOST,'source_host':entry['source_host'],
                                 'path':path,'line_number':line,'column':column,
                                 'context':[],'text':text[:3000],
                                 'size':record['size_bytes'],'modified_ms':None,
                                 'created_ms':None,'body_sha256':record['body_sha256'],
                                 'snapshot_at':index.manifest['generated_at']})
                    if len(hits)>remaining_hits:break
        except TimeoutError:
            warnings.append(entry['scope']+
                ': oversize scan deadline; remaining documents not searched')
        except Exception as error:
            warnings.append(entry['scope']+': '+type(error).__name__+': '+str(error)[:160])
        return hits,warnings

    def query(self,query,limit=20,glob=False,case_sensitive=False,roots=None,
              path_globs=None,deadline_seconds=25,authorized_roots=None,regex=False):
        if self.manifest is None:self.refresh()
        if regex and compile_bounded_regex(query) is None:
            raise ValueError('Invalid regular expression: not an empty result')
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
        results=[];seen=set();warnings=[];truncated=False;cloud_fetches=0
        started=time.monotonic()
        def read_scope(reader,budget):
            try:
                result=reader.query(
                    query,limit=limit+1,glob=glob,case_sensitive=case_sensitive,
                    roots=None if not roots or any(x in ('local','*',self.scope) for x in roots) else roots,
                    path_globs=path_globs,deadline_seconds=budget,
                    authorized_roots=authorized_roots,regex=regex)
                return reader.scope,result
            except Exception as error:
                return reader.scope,error
        def add_result(name,out):
            nonlocal cloud_fetches,truncated
            if isinstance(out,Exception):
                warnings.append(name+': '+type(out).__name__+': '+str(out)[:160])
                return
            cloud_fetches+=out['cloud_fetches']
            truncated |= bool(out['truncated'])
            if out['partial']:
                warnings.extend(name+': '+str(s) for s in out.get('warnings',[]))
            for hit in out['results']:
                ident=(hit['source_host'],hit['path'])
                if ident not in seen:
                    seen.add(ident);results.append(hit)
        if glob and len(selected)>1:
            # Path-only indexes are already hot in tmpfs, and independent
            # glob searches are inexpensive to process in parallel.
            with ThreadPoolExecutor(max_workers=min(4,len(selected))) as pool:
                for name,out in pool.map(lambda r:read_scope(r,deadline_seconds),selected):
                    add_result(name,out)
        else:
            # Cloud full-text downloads are expensive and rate-limited.
            # Never fan out 8 readers * 6 speculative rclone subprocesses:
            # process scopes in deterministic order, stop when the requested
            # limit is reached, and share ONE deadline across all scopes.
            for index,reader in enumerate(selected):
                budget=deadline_seconds-(time.monotonic()-started)
                if budget<=0:
                    warnings.append('Global cloud text deadline; remaining scopes not searched')
                    break
                name,out=read_scope(reader,budget)
                add_result(name,out)
                if len(results)>=limit:
                    # Unsearched scopes can contain additional results;
                    # signal truncation instead of claiming exhaustive search.
                    truncated |= index<len(selected)-1 or len(results)>limit
                    break
        truncated |= len(results)>limit
        if self.oversize:
            if len(results)>=limit:
                truncated=True
                warnings.append('Hit limit reached; oversized bodies not searched')
            else:
                budget=deadline_seconds-(time.monotonic()-started)
                if budget<=0:
                    truncated=True
                    warnings.append('Global cloud text deadline; oversized bodies not searched')
                else:
                    deadline=started+deadline_seconds
                    for entry in self.oversize:
                        if len(results)>=limit:
                            truncated=True
                            warnings.append('Hit limit reached; remaining oversized documents not searched')
                            break
                        if time.monotonic()>=deadline:
                            truncated=True
                            warnings.append('Global cloud text deadline; remaining oversized documents not searched')
                            break
                        source=entry['source_root']
                        if roots and not any(
                            x in ('*','local',self.scope,entry['scope'],source)
                            or (isinstance(x,str) and x.startswith(source.rstrip('/')+'/'))
                            or (isinstance(x,str) and source.startswith(x.rstrip('/')+'/'))
                            for x in roots):
                            continue
                        gets0=self.store.gets
                        hits,extra=self._oversize_query_hits(
                            entry,query,limit-len(results),glob,case_sensitive,
                            path_globs,authorized_roots,deadline,regex=regex)
                        cloud_fetches+=self.store.gets-gets0
                        warnings.extend(extra)
                        for hit in hits:
                            ident=(hit['source_host'],hit['path'])
                            if ident not in seen:
                                seen.add(ident);results.append(hit)
        elapsed_ms=round((time.monotonic()-started)*1000,2)
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
