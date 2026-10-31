#!/usr/bin/env python3
"""Synthetic lossless-cloud-archive checks; no production DB or remote writes."""
from __future__ import annotations
import gzip, hashlib, json, os, pathlib, sqlite3, sys, tempfile
from export_native_archive import export
from verify_native_archive import verify
from recover_native_archive import read_document
import reindex_native_archive as reindex

class FakeCloud:
    prefix='yadisk:Backups/GrepMesh/cloud-index-v1'
    def __init__(self):
        self.objects={}
        self.puts=0
    def get(self,key,limit):
        if key not in self.objects:
            raise RuntimeError('not found')
        data=self.objects[key]
        if len(data)>limit:
            raise ValueError('too large')
        return data
    def put(self,key,data):
        if key in self.objects:
            raise RuntimeError('immutable overwrite refused')
        self.objects[key]=bytes(data)
        self.puts+=1


def create_source(path):
    db=sqlite3.connect(path)
    db.executescript("""
    PRAGMA journal_mode=WAL;
    CREATE TABLE grepmesh_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
    CREATE TABLE grepmesh_document_rows(path TEXT PRIMARY KEY,fts_rowid INTEGER NOT NULL UNIQUE);
    CREATE VIRTUAL TABLE grepmesh_documents USING fts5(path UNINDEXED,body,tokenize='trigram');
    CREATE TABLE grepmesh_extraction_cache(path TEXT PRIMARY KEY,size INTEGER NOT NULL,mtime_ns TEXT NOT NULL,body TEXT NOT NULL);
    CREATE TABLE grepmesh_pending_ocr(path TEXT PRIMARY KEY);
    CREATE TABLE grepmesh_ocr_attempts(path TEXT PRIMARY KEY,size INTEGER NOT NULL,mtime_ns TEXT NOT NULL,failures INTEGER NOT NULL);
    CREATE TABLE grepmesh_ocr_empty_results(path TEXT PRIMARY KEY,size INTEGER NOT NULL,mtime_ns TEXT NOT NULL,disposition TEXT NOT NULL);
    """)
    docs=[
        ('/home/test/survives.md','Hello open files'),
        ('/home/test/deleted-since-index.txt','Historical text from missing file'),
        ('/home/test/old-scan.pdf','OCR machine-readable memories \nСТАРЫЙ ТЕКСТ'),
    ]
    for n,(p,b) in enumerate(docs,1):
        db.execute('INSERT INTO grepmesh_documents(rowid,path,body) VALUES(?,?,?)',(n,p,b))
        db.execute('INSERT INTO grepmesh_document_rows VALUES(?,?)',(p,n))
    db.execute("INSERT INTO grepmesh_extraction_cache VALUES(?,?,?,?)",(docs[2][0],122,'1726666666',docs[2][1]))
    db.execute("INSERT INTO grepmesh_metadata VALUES(?,?)",('rebuild_marker','opaque:kept'))
    db.execute("INSERT INTO grepmesh_pending_ocr VALUES(?)",('/home/test/missing-ocr.pdf',))
    db.execute("INSERT INTO grepmesh_ocr_attempts VALUES(?,?,?,?)",('/home/test/x.png',100,'1726',2))
    db.execute("INSERT INTO grepmesh_ocr_empty_results VALUES(?,?,?,?)",('/home/test/y.jpg',0,'1725','empty_output'))
    db.commit()
    db.close()


def run():
    root=pathlib.Path('/run/user') / str(os.getuid()) / 'grepmesh-native-backup-tests'
    root.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(dir=root) as temp:
        db=pathlib.Path(temp)/'native.sqlite3'
        create_source(db)
        store=FakeCloud()
        result=export(db,'server-100',root,'20261101T000000Z-01234567',store,4*1024*1024)
        assert verify('server-100','20261101T000000Z-01234567',store,validate_json=True)['documents']==3
        recovered=read_document(store,'server-100','20261101T000000Z-01234567','/home/test/deleted-since-index.txt')
        assert recovered['body']=='Historical text from missing file'
        assert read_document(store,'server-100','20261101T000000Z-01234567','/home/test/not-in-old-index') is None
        assert 'СТАРЫЙ ТЕКСТ' in read_document(store,'server-100','20261101T000000Z-01234567','/home/test/old-scan.pdf')['body']
        original_roots=reindex.ROOTS
        reindex.ROOTS=('/home/test',)
        try:
            selected,skips=reindex.prepare_sources(store,result,root/'synthetic-scope', 'server-100')
            assert sum(n for _,n in selected)==3
            assert sum(skips.values())==0
            for path,n in selected:
                records=[json.loads(line) for line in path.read_text().splitlines()]
                assert records[-1]['kind']=='end' and records[-1]['count']==n
                assert records[0]['source_root']=='/'
                path.unlink()
        finally:
            reindex.ROOTS=original_roots
        assert result['tables']['grepmesh_documents_content']['rows']==3
        assert result['tables']['grepmesh_document_rows']['rows']==3
        assert result['tables']['grepmesh_extraction_cache']['rows']==1
        assert result['tables']['grepmesh_pending_ocr']['rows']==1
        complete_key='archives/server-100/20261101T000000Z-01234567/complete.json'
        receipt=json.loads(store.objects[complete_key])
        m=store.objects[receipt['manifest']]
        assert hashlib.sha256(m).hexdigest()==receipt['sha256']
        for name,t in result['tables'].items():
            rows=[]
            for part in t['chunks']:
                compressed=store.objects[part['key']]
                assert hashlib.sha256(compressed).hexdigest()==part['sha256']
                rows.extend(json.loads(line) for line in gzip.decompress(compressed).splitlines())
            assert len(rows)==t['rows']
            if name=='grepmesh_documents_content':
                assert any('Historical text from missing file' in r[-1] for r in rows)
                assert any('СТАРЫЙ ТЕКСТ' in r[-1] for r in rows)
        db2=sqlite3.connect('file:'+str(db)+'?mode=ro',uri=True)
        assert db2.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
        db2.close()
        try:
            export(db,'server-100',root,'20261101T000000Z-01234567',store,4*1024*1024)
        except ValueError:
            pass
        else:
            raise AssertionError('immutable duplicate allowed')
        broken=pathlib.Path(temp)/'broken.sqlite3'
        c=sqlite3.connect(broken);c.execute('PRAGMA journal_mode=WAL');c.execute('CREATE TABLE grepmesh_metadata(key TEXT,value TEXT)');c.commit();c.close()
        other=FakeCloud()
        try:export(broken,'server-44',root,'20261101T000001Z-01234567',other,4*1024*1024)
        except RuntimeError:
            pass
        else:
            raise AssertionError('incomplete source archived')
        assert not any(x.endswith('complete.json') for x in other.objects)
        print(json.dumps({'passed':True,'tests':16,'archive_tables':list(result['tables']),'objects':len(store.objects)}))

if __name__=='__main__':run()
