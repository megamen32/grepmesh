#!/usr/bin/env python3
"""Synthetic preservation/lookup tests for the compact cloud catalog."""
import json, os, sqlite3, tempfile
from pathlib import Path
from catalog_lean import compact_catalog, metadata_query


def run_tests(base: Path) -> dict:
    paths = ['root/Report.PDF', 'root/фото 2024.jpg', 'root/archive/receipt.json',
             'receipt.json', 'outside/receipt.json', 'root/[draft]/file?.txt',
             'root/[draft]/plain.txt', 'root/100%_done.txt', 'root/Readme.md',
             'README.md', 'root/sub/a.json', 'root/sub/b.json', 'root/a*b.txt',
             "root/quoted'file.txt", 'root/Case.txt', 'root/case.txt',
             'root/Straße.txt', 'root/STRASSE.txt', 'root/z', 'root/[a].txt',
             'root/receipt.dir/file.txt', 'root/имя\nфайла.txt']
    patterns = ['*receipt*','receipt.json','*.json','root/*','*фото*','*no-such-file*',
                '*','[a-z]*','README.md','*.PDF','root/Case.txt','*STRASSE*',
                '[^x]*.txt','[[]a].txt','file[?].txt','a[*]b.txt','root/sub/?[.]json','z']
    terms = ['receipt', 'фото', '100%_', 'a*b', 'file?', '[draft]', "quoted'", 'z',
             '.', 'straße', '_', '\n', 'root/sub', 'not-found']
    with tempfile.TemporaryDirectory(prefix='lean-fixture-', dir=base) as work:
        old, new = Path(work)/'old.sqlite', Path(work)/'new.sqlite'
        c=sqlite3.connect(old)
        c.executescript('''CREATE TABLE files(id INTEGER PRIMARY KEY,path TEXT UNIQUE,name TEXT,
            size INTEGER,modified TEXT,path_fold TEXT,name_fold TEXT);
            CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
            CREATE INDEX path_order ON files(path_fold);
            CREATE VIRTUAL TABLE names_fts USING fts5(path_fold,name_fold,
                content='files',content_rowid='id',tokenize='trigram');''')
        for i,p in enumerate(paths,1):
            n=p.rsplit('/',1)[-1]
            c.execute('INSERT INTO files VALUES(?,?,?,?,?,?,?)',(i,p,n,i,'2026-10-31',p.casefold(),n.casefold()))
        c.execute('INSERT INTO metadata VALUES(?,?)',('manifest',json.dumps({'files':len(paths),'metadata_complete':True})))
        c.execute("INSERT INTO names_fts(names_fts) VALUES('rebuild')");c.commit()
        conversion=compact_catalog(old,new)
        d=sqlite3.connect(new); checked=0
        for query in patterns:
            expected={r[0] for r in c.execute('SELECT id FROM files WHERE path_fold GLOB ? OR name_fold GLOB ?', (query.casefold(),query.casefold()))}
            for conn in (c,d):
                sql,args=metadata_query(conn,query,True)
                rows=conn.execute(sql,args).fetchall();got=[r[0] for r in rows]
                assert set(got)==expected, (query,got,expected)
                assert len(got)==len(set(got)), ('duplicate',query)
            checked+=1
        for query in terms:
            expected={i for i,p in enumerate(paths,1) if query.casefold() in p.casefold()}
            for conn in (c,d):
                sql,args=metadata_query(conn,query,False)
                got={r[0] for r in conn.execute(sql,args)}
                assert got==expected, (query,got,expected)
            checked+=1
        assert conversion['files']==len(paths) and conversion['all_records_identical']
        assert 'name_fold' not in {r[1] for r in d.execute('PRAGMA table_info(files)')}
        plan=[r[3] for r in d.execute("EXPLAIN QUERY PLAN SELECT f.* FROM names_fts n JOIN files f ON f.id=n.rowid WHERE n.path_fold GLOB '*receipt*'")]
        assert any('VIRTUAL TABLE INDEX' in p and 'G' in p for p in plan), plan
        c.close();d.close()
        return {'cases':checked,'both_schemas_passed':True,'case_distinct_paths_preserved':True,
                'all_fixture_records_identical':True,'trigram_plan':plan}

if __name__=='__main__':
    if hasattr(os,'sched_setaffinity'): os.sched_setaffinity(0,sorted(os.sched_getaffinity(0))[:2])
    os.nice(10)
    root=Path(os.environ.get('TMPDIR','/run/user/1000/grepmesh-yadisk'))
    result=run_tests(root);print(json.dumps(result))
    (root/'lean-tests.json').write_text(json.dumps(result,indent=2))
