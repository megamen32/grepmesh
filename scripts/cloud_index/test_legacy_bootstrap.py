#!/usr/bin/env python3
"""Narrow migration parity and fail-closed checks; synthetic data only."""
import hashlib, io, json, os, pathlib, sqlite3, subprocess, tempfile
from legacy_bootstrap import scan_legacy
from cloud_index import ensure_tmpfs, MemoryStore, Publisher, Reader, MAX_DOCUMENT

base=pathlib.Path('/run/user')/str(os.getuid())/'grepmesh-cloud-legacy-tests'
ensure_tmpfs(base)
checks=[]
def ok(name,value):
    if not value:raise AssertionError(name)
    checks.append(name)
with tempfile.TemporaryDirectory(dir=base) as temp:
    work=pathlib.Path(temp);root=work/'source'/'references';root.mkdir(parents=True)
    db=work/'legacy.sqlite3'
    conn=sqlite3.connect(db)
    conn.executescript("""
      CREATE TABLE grepmesh_extraction_cache(path TEXT PRIMARY KEY,size INTEGER,mtime_ns TEXT,body TEXT);
      CREATE TABLE grepmesh_document_rows(path TEXT PRIMARY KEY,fts_rowid INTEGER);
      CREATE VIRTUAL TABLE grepmesh_documents USING fts5(path UNINDEXED,body,tokenize='trigram');
    """)
    values=[
      ('a.md',b'real text body','first live text'),
      ('b.pdf',b'%PDF-synthetic','Кириллица OCR уникальная фраза'),
      ('c.docx',b'PK-synthetic','DOCX already extracted cached body'),
      ('empty_cache.pdf',b'%PDF with memo','this paragraph is only in the FTS body'),
      ('auth.json',b'private fixture','PRIVATE_SECRETS_NEVER_EXPORTED'),
    ]
    for path,data,body in values:
        file=root/path;file.write_bytes(data);stat=file.stat()
        cache=body if path!='empty_cache.pdf' else ''
        conn.execute('INSERT INTO grepmesh_extraction_cache VALUES (?,?,?,?)',(str(file),stat.st_size,str(stat.st_mtime_ns),cache))
        conn.execute('INSERT INTO grepmesh_documents(path,body) VALUES (?,?)',(str(file),body))
        conn.execute('INSERT INTO grepmesh_document_rows VALUES (?,?)',(str(file),conn.execute('SELECT last_insert_rowid()').fetchone()[0]))
    conn.commit()
    stream=io.StringIO()
    scan_legacy(root,db,'server-100',stream)
    lines=[json.loads(x) for x in stream.getvalue().splitlines()]
    ok('four supported documents including PDFs and DOCX',lines[-1]['count']==4)
    ok('secret filename excluded',lines[-1]['skipped'].get('secret_name')==1)
    ok('FTS fallback for compacted extraction cache',any(d.get('body','').startswith('this paragraph') for d in lines))
    store=MemoryStore()
    manifest=Publisher(store,work).publish('legacy-scope',io.StringIO(stream.getvalue()))
    ok('complete legacy source atomically published',manifest['documents']==4)
    reader=Reader(store,work,'legacy-scope',512*1024*1024,'server-100',str(root))
    result=reader.query('OCR')
    ok('PDF OCR found',len(result['results'])==1 and result['results'][0]['path'].endswith('b.pdf') and not result['partial'])
    result=reader.query('DOCX')
    ok('DOCX extracted content found',len(result['results'])==1 and not result['partial'])
    result=reader.query('paragraph')
    ok('compacted cache body found',len(result['results'])==1 and result['results'][0]['path'].endswith('empty_cache.pdf'))
    result=reader.query('PRIVATE_SECRETS_NEVER_EXPORTED')
    ok('excluded secrets are absent',len(result['results'])==0)
    (root/'a.md').write_text('changed')
    try:scan_legacy(root,db,'server-100',io.StringIO())
    except RuntimeError:checks.append('stale source prevents publication')
    else:raise AssertionError('Stale source incorrectly published')
    conn.close()
    ok('legacy database retained for rollback',db.exists())
print(json.dumps({'tests':len(checks),'passed':True,'cases':checks},ensure_ascii=False))
