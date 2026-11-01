#!/usr/bin/env python3
"""Bounded WAL-export regression using only synthetic tmpfs SQLite."""
import gzip,json,os,pathlib,sqlite3,tempfile

from export_native_rolling import dump

class MemoryCloud:
    prefix='yadisk:Backups/GrepMesh/cloud-index-v1'
    def __init__(self):self.data={}
    def get(self,key,limit):
        if key not in self.data:raise RuntimeError('not found')
        assert len(self.data[key])<=limit
        return self.data[key]
    def put(self,key,data):
        self.data[key]=bytes(data)

def run():
    root=pathlib.Path('/run/user')/str(os.getuid())/'grepmesh-rolling-archive-tests'
    root.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(dir=root) as folder:
        db=pathlib.Path(folder)/'source.sqlite3'
        connection=sqlite3.connect(db)
        connection.execute('PRAGMA journal_mode=WAL')
        connection.execute('CREATE TABLE grepmesh_documents_content (rowid INTEGER PRIMARY KEY, c0 TEXT, c1 TEXT)')
        data=[(i,'/home/synthetic/doc-'+str(i)+'.md','needle_'+str(i)+' '+('abcdef'*500)) for i in range(1,201)]
        connection.executemany('INSERT INTO grepmesh_documents_content VALUES(?,?,?)',data)
        connection.commit()
        connection.close()
        cloud=MemoryCloud()
        runid='20261101T023000Z-test0001'
        first=dump(db,cloud,runid,root,max_chunks=2,batch_rows=8,chunk_raw=96*1024,create=True)
        assert not first['finished'] and len(first['chunks'])==2
        assert first['rows']>0
        resume=dump(db,cloud,runid,root,batch_rows=8,chunk_raw=96*1024,create=False)
        assert resume['finished'] and resume['rows']==len(data)
        assert not resume['consistent_snapshot'] and not resume['safe_for_deletion']
        recovered=[]
        for part in resume['chunks']:
            payload=cloud.data[part['key']]
            import hashlib
            assert hashlib.sha256(payload).hexdigest()==part['sha256']
            rows=[json.loads(line) for line in gzip.decompress(payload).splitlines()]
            assert len(rows)==part['rows']
            recovered.extend(rows)
        assert recovered==[list(x) for x in data]
        try:dump(db,cloud,runid,root,max_chunks=1,batch_rows=8,chunk_raw=96*1024,create=True)
        except ValueError:pass
        else:raise AssertionError('Duplicate archive ID was allowed')
        # Only newer, high-rowid documents are captured: no second full scan.
        conn=sqlite3.connect(db)
        additions=[(i,'/home/synthetic/new-'+str(i)+'.md','new'+str(i))
                   for i in range(201,208)]
        conn.executemany('INSERT INTO grepmesh_documents_content VALUES (?,?,?)',additions)
        conn.commit()
        conn.close()
        nextid='20261101T043000Z-delta001'
        delta=dump(db,cloud,nextid,root,batch_rows=8,chunk_raw=96*1024,
                   create=True,lower_rowid=resume['upper_rowid'],base_snapshot=runid)
        assert delta['finished'] and delta['rows']==7
        assert delta['lower_rowid']==200 and delta['upper_rowid']==207
        assert delta['base_snapshot']==runid
        assert delta['consistent_snapshot'] is False and delta['safe_for_deletion'] is False
        packed=cloud.get(delta['chunks'][0]['key'],100000)
        assert [json.loads(x) for x in gzip.decompress(packed).splitlines()] == [list(x) for x in additions]
        try:dump(db,cloud,'20261101T044000Z-delta002',root,batch_rows=8,
                 create=True,lower_rowid=199,base_snapshot=runid)
        except ValueError:pass
        else:raise AssertionError('Accepted a delta with a gap/overlap against its base')
        try:dump(db,cloud,nextid,root,batch_rows=8,create=False)
        except ValueError:pass
        else:raise AssertionError('Resumed a delta without its immutable base reference')
        print(json.dumps({'passed':True,'checks':15,'documents':len(recovered),
                          'delta_documents':len(additions),'chunks':len(resume['chunks']),
                          'safe_for_deletion':False}))

if __name__=='__main__':run()
