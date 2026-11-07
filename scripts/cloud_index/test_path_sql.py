#!/usr/bin/env python3
"""Existing SQLite paths: exact fnmatch parity, ACL before LIMIT, no body reads."""
import fnmatch, pathlib, sqlite3, time
from cloud_index import sql_path_rows
paths=['/home/u/a.md','/home/u/A.MD','/home/u/sub/b.md','/home/u/straße.md',
       '/home/u/ДОГОВОР.md','/home/u/[x].md','/home/u/!.md','/home/u/^.md',
       '/home/unauthorized/a.md','/etc/a.md']
c=sqlite3.connect(':memory:');c.row_factory=sqlite3.Row
c.execute('CREATE TABLE paths(path TEXT,size INT,mtime_ns INT,sha TEXT)')
c.executemany('INSERT INTO paths VALUES(?,1,0,\'sha\')',[(p,) for p in paths])
for pattern in ['*.md','a.?d','STRASSE.md','договор.md','[!a]*.md','[[]x].md',
                '[]x].md','[^x].md','[z-a].md','[abc','/home/*/a.md','sub/*.md','*']:
 for roots in [None,[],['/home/u'],['/etc']]:
  expected=[p for p in paths if (roots is None or any(p==r or p.startswith(r+'/') for r in roots))
            and (fnmatch.fnmatchcase(p.casefold(),pattern.casefold()) or
                 fnmatch.fnmatchcase(pathlib.PurePosixPath(p).name.casefold(),pattern.casefold()))]
  got=[r['path'] for r in sql_path_rows(c,'paths',pattern,['/'],roots,[],time.monotonic()+5,100)]
  assert got==expected,(pattern,roots,got,expected)
assert [r['path'] for r in sql_path_rows(c,'paths','*',['/'],['/etc'],[],time.monotonic()+5,1)]==['/etc/a.md']
assert [r['path'] for r in sql_path_rows(c,'paths','*',['/home/u'],None,['*/sub/*'],time.monotonic()+5,10)]==['/home/u/sub/b.md']
print('54 SQL glob/ACL parity checks PASS')
