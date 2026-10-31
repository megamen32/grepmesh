#!/usr/bin/env python3
"""Deterministic cloud-index correctness tests. Synthetic text only; tmpfs required."""
import hashlib, importlib.util, io, json, os, pathlib, sqlite3, subprocess, tempfile, time
from unittest.mock import patch
from cloud_index import Publisher, Reader, MemoryStore, CloudStore, document, json_bytes, ensure_tmpfs, scan_git

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
        docs=[('/fixture/one.md','alpha needleONE\nsecond line\n'+'stable words '*6000),('/fixture/two.md','beta needleTWO\n'+'unchanged words '*6000),('/fixture/фото.md','Кириллица строка\nStraße other\n'+'unicode note '*6000),('/fixture/Case.md','case upper only'),('/fixture/case.md','case lower only')]
        m=pub.publish('fixture',stream(docs));ok('full snapshot row accounting',m['documents']==5 and len(m['parts'])>=3)
        maximum=max(x['raw_bytes'] for x in m['parts']);reader=Reader(store,root,'fixture',maximum,'fixture-host','/fixture');reader.refresh()
        r=reader.query('needleONE');ok('literal body retrieval',len(r['results'])==1 and r['results'][0]['line_number']==1 and not r['partial'])
        ok('first query reads cloud',r['cloud_fetches']>0)
        r=reader.query('needleONE');ok('warm query cache hit',r['cloud_fetches']==0)
        r=reader.query('строка');ok('Cyrillic text retrieval',len(r['results'])==1 and not r['partial'])
        r=reader.query('STRASSE');ok('expanding Unicode case folding',len(r['results'])==1)
        ok('bounded LRU',reader.used<=maximum and len(reader.cache)<=1)
        reader.clear();fresh=Reader(store,root,'fixture',maximum,'fixture-host','/fixture');r=fresh.query('needleTWO')
        ok('empty-cache recovery requires no source DB',len(r['results'])==1 and r['cloud_fetches']>=3)
        r=fresh.query('*.md',glob=True,limit=20);ok('filename search and case-distinct paths',len(r['results'])==5 and not r['partial'])
        r=fresh.query('case',case_sensitive=True,limit=20);ok('literal case-sensitive query',len(r['results'])==2)
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
        c=sqlite3.connect(path);ok('FTS does not duplicate stored source text',c.execute('SELECT body FROM terms LIMIT 1').fetchone()[0] is None)
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
    report={'all_passed':True,'tests':len(cases),'cases':cases,'duration_seconds':round(time.monotonic()-start,3),'production_unchanged':True,'test_store':'synthetic in-memory cloud transport','real_cloud_test_separate':True}
    (BASE/'result.json').write_text(json.dumps(report,indent=2));print(json.dumps(report))

if __name__=='__main__':
    if hasattr(os,'sched_setaffinity'):os.sched_setaffinity(0,sorted(os.sched_getaffinity(0))[:2])
    os.nice(10);run()
