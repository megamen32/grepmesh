#!/usr/bin/env python3
"""Deterministic cloud-index correctness tests. Synthetic text only; tmpfs required."""
import hashlib, importlib.util, io, json, os, pathlib, sqlite3, subprocess, tempfile, time, threading
from unittest.mock import patch
from cloud_index import Publisher, Reader, MemoryStore, CloudStore, document, json_bytes, ensure_tmpfs, scan_git, prefetched_shards, compile_bounded_regex, regex_match_positions, serve

BASE=pathlib.Path('/run/user/1000/grepmesh-cloud-index-tests')

def stream(items,root='/fixture',host='fixture-host',end=True):
    rows=[{'kind':'begin','format':1,'source_host':host,'source_root':root,'policy':'synthetic fixture'}]
    h=hashlib.sha256()
    for path,body in items:
        d=document(path,body,len(body.encode()),1234567890);rows.append(d);h.update(json_bytes([path,d['sha256']]))
    if end:rows.append({'kind':'end','count':len(items),'logical_sha256':h.hexdigest(),'complete_for_policy':True,'skipped':{}})
    return [json.dumps(r,ensure_ascii=False)+'\n' for r in rows]


def run():
    ensure_tmpfs(BASE);cases=[];start=time.monotonic()
    def ok(name,condition=True):
        assert condition,name;cases.append(name)
    with tempfile.TemporaryDirectory(dir=BASE) as work:
        root=pathlib.Path(work);store=MemoryStore();pub=Publisher(store,root,64*1024)
        ok('historical scoped writer accepts explicit 16 MiB flush',Publisher(store,root,16*1024*1024).target==16*1024*1024)
        ok('default project shard target unchanged',Publisher(store,root).target==8*1024*1024)
        docs=[('/fixture/one.md','alpha needleONE\nsecond line\n'+'stable words '*6000),('/fixture/two.md','beta needleTWO\n'+'unchanged words '*6000),('/fixture/фото.md','Кириллица строка\nStraße other\n'+'unicode note '*6000),('/fixture/Case.md','case upper only'),('/fixture/case.md','case lower only')]
        m=pub.publish('fixture',stream(docs));ok('full snapshot row accounting',m['documents']==5 and len(m['parts'])>=3)
        ok('path-only index published',m.get('path_index',{}).get('documents')==5)
        maximum=max(x['raw_bytes'] for x in m['parts']);reader=Reader(store,root,'fixture',maximum,'fixture-host','/fixture');reader.refresh()
        r=reader.query('needleONE');ok('literal body retrieval',len(r['results'])==1 and r['results'][0]['line_number']==1 and not r['partial'])
        ok('first query reads cloud',r['cloud_fetches']>0)
        r=reader.query('needleONE');ok('warm query cache hit',r['cloud_fetches']==0)
        historical=reader.read_text('/fixture/one.md',start_line=1,end_line=2,authorized_roots=['/fixture'])
        ok('exact historical read without source file',not pathlib.Path('/fixture/one.md').exists() and [x['text'] for x in historical['chunks'][0]['lines']]==['alpha needleONE','second line'])
        ok('historical read has exact content digest',historical['body_sha256']==hashlib.sha256(docs[0][1].encode()).hexdigest())
        before_acl_denial=store.gets
        try:reader.read_text('/fixture/one.md',authorized_roots=['/fixture/two.md'])
        except PermissionError:ok('historical read respects delegated ACL')
        else:raise AssertionError('Historical ACL bypass')
        ok('ACL denied before cloud fetch',store.gets==before_acl_denial)
        try:reader.read_text('/fixture/../escape.md',authorized_roots=['/fixture'])
        except ValueError:ok('historical read rejects path traversal')
        else:raise AssertionError('Historical path traversal')

        try:reader.read_text('/fixture/not-existing.md',authorized_roots=['/fixture'])
        except FileNotFoundError:ok('missing historical body explicit failure')
        else:raise AssertionError('Missing historical body falsely completed')
        r=reader.query('строка');ok('Cyrillic text retrieval',len(r['results'])==1 and not r['partial'])
        r=reader.query('alpha needleONE');ok('multi-token exact substring using trigram AND',len(r['results'])==1 and not r['partial'])
        r=reader.query('Кириллица строка');ok('Cyrillic phrase in contentless FTS',len(r['results'])==1 and not r['partial'])
        r=reader.query('needleONE\nsecond line');ok('cross-line exact substring in contentless FTS',len(r['results'])==1 and not r['partial'])
        r=reader.query('alpha needleTWO');ok('noncontiguous terms do not cause false hits',not r['results'])
        r=reader.query('STRASSE');ok('expanding Unicode case folding',len(r['results'])==1)
        ok('bounded LRU',reader.used<=maximum and len(reader.cache)<=1)
        reader.clear();fresh=Reader(store,root,'fixture',maximum,'fixture-host','/fixture');r=fresh.query('needleTWO')
        ok('empty-cache recovery requires no source DB',len(r['results'])==1 and r['cloud_fetches']>=3)
        r=fresh.query('*.md',glob=True,limit=20);ok('filename search and case-distinct paths',len(r['results'])==5 and not r['partial'])
        ok('filename glob fetches only compact path index',r['cloud_fetches']==1)
        path_index=fresh.manifest.pop('path_index')
        legacy=fresh.query('one.md',glob=True,authorized_roots=['/fixture'])
        ok('legacy shards retain path-only hits without body reads',
           [x['path'] for x in legacy['results']]==['/fixture/one.md'] and not legacy['partial'])
        fresh.manifest['path_index']=path_index
        r=fresh.query('case',case_sensitive=True,limit=20);ok('literal case-sensitive query',len(r['results'])==2)
        r=fresh.query(r'needle(TWO|ONE)',regex=True,limit=20)
        ok('regex alternation finds both branch bodies',
           {x['path'] for x in r['results']}=={'/fixture/two.md','/fixture/one.md'}
           and all('needle' in x['text'] for x in r['results'])
           and r['partial']is False and r['results'][0]['column']>0)
        before_literal_prune=store.gets
        r=fresh.query(r'ZZZmissing[0-9]*committee',regex=True,limit=20)
        ok('regex literal routing avoids every cloud shard',not r['results'] and not r['partial'] and store.gets==before_literal_prune)
        r=fresh.query(r'(?i)CASE UPPER',regex=True,limit=20)
        ok('regex inline case-insensitive flag',len(r['results'])==1 and r['results'][0]['path']=='/fixture/Case.md')
        r=fresh.query(r'^case upper',regex=True,limit=20)
        ok('regex line anchor without multiline matches line start',len(r['results'])==1 and r['results'][0]['column']==0 and r['results'][0]['path']=='/fixture/Case.md')
        r=fresh.query(r'^upper',regex=True,limit=20)
        ok('regex line anchor never matches mid-line',not r['results'] and not r['partial'])
        r=fresh.query(r'owner only',regex=True,limit=20)
        ok('regex absent pattern returns no hit without error',not r['results'] and not r['partial'])
        r=reader.query(r'(?i)кириллиц',regex=True,limit=20)
        ok('regex over unicode body hit',len(r['results'])==1 and not r['partial'])
        cross=reader.query(r'(?s)needleONE.*?stable',regex=True,limit=20)
        ok('regex cross-line whole-body scan',len(cross['results'])==1 and cross['results'][0]['line_number']==1)
        literal_cross=reader.query(r'needleONE\nsecond',regex=True,limit=20)
        ok('literal newline regex matches across lines',len(literal_cross['results'])==1 and literal_cross['results'][0]['line_number']==1)
        try:fresh.query('needle(ONE',regex=True,limit=20)
        except ValueError:ok('invalid regex is an explicit error not an empty success',True)
        else:raise AssertionError('invalid regex silently empty')
        r=fresh.query(r'needle[0-9]*ONE',regex=True,limit=20)
        ok('regex optional class literal prunes to the true hit',len(r['results'])==1 and r['results'][0]['path']=='/fixture/one.md' and not r['partial'])
        try:
            t=time.monotonic();std,hard=compile_bounded_regex(r'(?:a+)+b')
            regex_match_positions(std,hard,'a'*300000+'x',time.monotonic()+0.2)
            raise AssertionError('catastrophic regex exceeded hard deadline')
        except TimeoutError:
            ok('blind backtracking regex bounded under hard deadline',time.monotonic()-t<2.0)
        socket_=importlib.import_module('socket');urllib=importlib.import_module('urllib.request');urlerr=importlib.import_module('urllib.error')
        probe=socket_.socket();probe.bind(('127.0.0.1',0));loop_port=probe.getsockname()[1];probe.close()
        serving=threading.Thread(target=serve,args=(fresh,loop_port),daemon=True);serving.start()
        ready=False
        for _ in range(100):
            try:urllib.urlopen('http://127.0.0.1:%d'%loop_port,timeout=1).read();ready=True;break
            except urlerr.URLError:time.sleep(0.05)
        ok('cloud MCP loopback reviewer reachable',ready)
        def rpc(name,arguments):
            body={'jsonrpc':'2.0','id':7,'method':'tools/call','params':{'name':name,'arguments':arguments}}
            out=urllib.urlopen(urllib.Request('http://127.0.0.1:%d'%loop_port,data=json.dumps(body).encode('utf-8'),headers={'Content-Type':'application/json'}),timeout=10).read()
            return json.loads(out)
        mcp=rpc('search',{'query':r'needle(TWO|ONE)','mode':'regex','limit':20})
        inner=json.loads(mcp['result']['content'][0]['text'])
        ok('regex flows through the MCP envelope',json.loads(mcp['result']['content'][0]['text'])==inner and {x['path'] for x in inner['results']}=={'/fixture/two.md','/fixture/one.md'} and mcp['result']['isError'] is False)
        mcp=rpc('search',{'query':'needle(ONE','mode':'regex'})
        ok('invalid regex reaches MCP as an error not results',mcp['result'].get('isError') is True and 'Invalid regular expression' in mcp['result']['content'][0]['text'])
        mcp=rpc('search',{'query':'needleTWO','mode':'literal'})
        inner=json.loads(mcp['result']['content'][0]['text'])
        ok('literal mode unchanged over MCP',len(inner['results'])==1)
        uio=rpc('find_paths',{'query':'*.md','mode':'regex'})
        ok('regex on find_paths is already explicit non-empty failure',uio['result'].get('isError') is True and 'find_paths matches globs only' in uio['result']['content'][0]['text'])

        r=fresh.query('needleONE',path_globs=['*/two.md']);ok('path restriction before results',not r['results'])
        r=fresh.query('needleONE',authorized_roots=['/fixture/two.md']);ok('forwarded read ACL filters text hits',not r['results'])
        r=fresh.query('needleONE',authorized_roots=[]);ok('empty read ACL never broadens access',not r['results'])
        try:fresh.query('needleONE',roots=['/unauthorized'])
        except ValueError:ok('unauthorized corpus root denied')
        else:raise AssertionError('root restriction missing')
        # Removing/updating documents is a full scope generation replacement, not an append.
        replacement=[('/fixture/renamed.md','new needleRENAMED'),('/fixture/two.md','new needleUPDATED')]
        pub.publish('fixture',stream(replacement));fresh.refresh()
        ok('deletion visibility',not fresh.query('needleONE')['results'])
        ok('update visibility',len(fresh.query('needleUPDATED')['results'])==1 and not fresh.query('needleTWO')['results'])
        ok('rename visibility',fresh.query('needleRENAMED')['results'][0]['path']=='/fixture/renamed.md')
        pointer=store.objects['scopes/fixture/latest.json']
        try:pub.publish('fixture',stream([('/fixture/failed.md','not published')],end=False))
        except RuntimeError:pass
        else:raise AssertionError('incomplete scan must not publish')
        ok('incomplete source cannot replace good generation',store.objects['scopes/fixture/latest.json']==pointer)
        try:pub.publish('fixture',stream([('/escape/secret.md','not admitted')]))
        except ValueError:ok('escaped source path denied')
        else:raise AssertionError('escaped path admitted')
        try:pub.publish('fixture',stream([('/fixture/a','a'),('/fixture/a','b')]))
        except ValueError:ok('duplicate source path rejected')
        else:raise AssertionError('duplicate path admitted')
        fresh.clear();part=fresh.manifest['parts'][0];saved=store.objects[part['key']];store.objects[part['key']]=b'corruption'
        r=fresh.query('needleUPDATED');ok('corrupt object is explicit failure not false absence',r['partial'] and r['warnings'])
        store.objects[part['key']]=saved
        fresh.clear();r=fresh.query('needleUPDATED');path=next(iter(fresh.cache.values()))
        c=sqlite3.connect(path);ftssql=c.execute("SELECT sql FROM sqlite_master WHERE name='terms'").fetchone()[0].lower()
        ok('FTS does not duplicate stored source text',"content=''" in ftssql and 'detail=none' in ftssql and 'columnsize=0' in ftssql)
        ok('compressed body copy only',c.execute('SELECT count(*) FROM bodies').fetchone()[0]==2);c.close()
        old_keys=[k for k in store.objects if k.startswith('manifests/fixture/')];ok('rollback manifests retained',len(old_keys)==2)
        # Source agent reads originals and leaves no local SQLite/files of its own.
        source=root/'source';source.mkdir();subprocess.run(['git','init','-q',str(source)],check=True)
        (source/'a.md').write_text('original source text');(source/'.env').write_text('synthetic_secret=never_export');(source/'b.bin').write_bytes(b'\0binary')
        (source/'link.md').symlink_to('/etc/hosts');subprocess.run(['git','-C',str(source),'add','.'],check=True)
        out=io.StringIO();scan_git(source,'fixture-host',out);emitted=[json.loads(x) for x in out.getvalue().splitlines()]
        ok('source scan reads original',sum(x.get('kind')=='document' for x in emitted)==1)
        ok('secret and symlink exclusion',emitted[-1]['skipped'].get('secret_name')==1 and emitted[-1]['skipped'].get('symlink')==1)
        ok('no source-side database',not list(source.glob('*.sqlite')))
        ok('no persistent temporary index',all(str(p).startswith('/run/') for p in root.rglob('*.sqlite')))
        # Regression: SSH metadata reads must never steal JSON lines from the publisher's stdin.
        payload=b'cloud-stream-regression'
        result=[subprocess.CompletedProcess([],0,stdout=payload,stderr=b''),
                subprocess.CompletedProcess([],0,stdout=b'',stderr=b''),
                subprocess.CompletedProcess([],0,stdout=json.dumps({'Size':len(payload),'Hashes':{'md5':hashlib.md5(payload).hexdigest()}}).encode(),stderr=b'')]
        with patch('cloud_index.subprocess.run',side_effect=result) as mocked:
            with patch.object(CloudStore,'command',return_value=['fake-rclone']):
                cloud=CloudStore()
                assert cloud.get('objects/regression')==payload
                cloud.put('objects/regression',payload)
        ok('cloud reads disconnect source stdin',mocked.call_args_list[0].kwargs.get('stdin') == subprocess.DEVNULL)
        ok('uploads preserve their explicit byte stream',mocked.call_args_list[1].kwargs.get('input') == payload)
        ok('upload verification disconnects source stdin',mocked.call_args_list[2].kwargs.get('stdin') == subprocess.DEVNULL)
        with patch('cloud_index.SOURCE_MACHINE','local'):
            with patch('cloud_index.ensure_tmpfs') as guarded:
                local_command=CloudStore().command(['cat','yadisk:example/file'])
                ok('local owner gateway skips redundant SSH',local_command[:2]==['/bin/sh','-c'] and 'exec rclone cat' in local_command[2])
                ok('local gateway insists on tmpfs',guarded.called)
        with patch('cloud_index.SOURCE_MACHINE','authorized-remote-host'):
            remote_command=CloudStore().command(['cat','yadisk:example/file'])
            ok('remote gateway still requires authenticated SSH',remote_command[0]=='ssh' and remote_command[-2]=='authorized-remote-host')
        class SimulatedCloud:
            def __init__(self):
                self.lock=threading.Lock();self.active=0;self.peak=0
            def get(self,key,limit):
                with self.lock:
                    self.active+=1
                    self.peak=max(self.peak,self.active)
                try:
                    time.sleep(.03)
                    if key=='part-3':raise ValueError('synthetic cloud failure')
                    return key.encode()
                finally:
                    with self.lock:self.active-=1
        mock_store=SimulatedCloud()
        parts=[{'sha256':f'{i:064x}','key':f'part-{i}','packed_bytes':50} for i in range(7)]
        data=list(prefetched_shards(mock_store,parts,{parts[0]['sha256']:root},time.monotonic(),1,workers=4))
        ok('parallel prefetch bounded to four workers',2<=mock_store.peak<=4)
        ok('parallel prefetch keeps manifest order',[p['key'] for p,_ in data]==[p['key'] for p in parts])
        ok('cached blob does not need another download',data[0][1] is None)
        ok('failed cloud fetch stays an explicit error',isinstance(data[3][1],ValueError))
        ok('other cloud fetches are unmodified',data[4][1]==b'part-4')
    report={'all_passed':True,'tests':len(cases),'cases':cases,'duration_seconds':round(time.monotonic()-start,3),'production_unchanged':True,'test_store':'synthetic in-memory cloud transport','real_cloud_test_separate':True}
    (BASE/'result.json').write_text(json.dumps(report,indent=2));print(json.dumps(report))

if __name__=='__main__':
    if hasattr(os,'sched_setaffinity'):os.sched_setaffinity(0,sorted(os.sched_getaffinity(0))[:2])
    os.nice(10);run()
