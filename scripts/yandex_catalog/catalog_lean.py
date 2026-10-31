#!/usr/bin/env python3
"""Compact read-only file catalog schema and substring queries.
No credentials, network calls, or production configuration. Build beside the
old database and publish only after independently verifying the result.
"""
from __future__ import annotations
import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 2
CORE_FIELDS = 'id,path,size,modified,path_fold'


def quote_glob(value: str) -> str:
    return value.replace('[', '[[]').replace('*', '[*]').replace('?', '[?]')


def metadata_query(connection: sqlite3.Connection, query: str, glob: bool) -> tuple[str, tuple[Any, ...]]:
    """Return unbounded SELECT + parameters; callers add permissions and LIMIT.

    Works with the old two-column FTS and compact single-column FTS. Basename
    matching is retained without storing/indexing the basename a second time.
    GLOB, rather than MATCH phrases, supports detail=none trigram indexes.
    """
    connection.create_function('gm_basename', 1, lambda p: p.rsplit('/', 1)[-1], deterministic=True)
    has_fts = connection.execute("SELECT 1 FROM sqlite_master WHERE name='names_fts'").fetchone() is not None
    if not glob:
        pattern = '*' + quote_glob(query.casefold()) + '*'
    else:
        pattern = query.removeprefix('yadisk:').lstrip('/').casefold()
    if glob and '/' in pattern and not any(c in pattern for c in '*?['):
        return 'SELECT * FROM files WHERE path_fold=?', (pattern,)
    head = 'SELECT f.* FROM names_fts n JOIN files f ON f.id=n.rowid' if has_fts else 'SELECT f.* FROM files f'
    field = 'n.path_fold' if has_fts else 'f.path_fold'
    sql = head + ' WHERE ' + field + ' GLOB ?'
    params: tuple[Any, ...] = (pattern,)
    # If pattern starts with *, a basename match is already a whole-path match.
    # A pattern containing / cannot match a basename. Other patterns need an
    # additional indexed candidate set plus an exact basename predicate.
    if glob and '/' not in pattern and not pattern.startswith('*'):
        sql += (' UNION ALL ' + head + ' WHERE ' + field + ' GLOB ?'
                ' AND NOT (f.path_fold GLOB ?) AND gm_basename(f.path_fold) GLOB ?')
        params += ('*/' + pattern, pattern, pattern)
    return sql, params


def logical_digest(connection: sqlite3.Connection) -> tuple[int, int, str]:
    """Hash every retained record in stable ID order, without loading it all."""
    digest = hashlib.sha256()
    count = total = 0
    for row in connection.execute('SELECT ' + CORE_FIELDS + ' FROM files ORDER BY id'):
        encoded = json.dumps(tuple(row), ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        digest.update(len(encoded).to_bytes(8, 'big'))
        digest.update(encoded)
        count += 1
        total += row[2]
    return count, total, digest.hexdigest()


def compact_catalog(source: Path, target: Path, max_bytes: int = 1536 * 1024 * 1024) -> dict[str, Any]:
    """Create a new compact database; never modify source or overwrite target.

    The ingestion/staging table remains responsible for deduplicating exact,
    case-sensitive paths. The published table is immutable: it needs only the
    folded path B-tree and one reduced-detail trigram index.
    """
    source, target = Path(source), Path(target)
    if not source.is_file() or source.resolve() == target.resolve():
        raise ValueError('Source must be an existing, distinct database')
    if target.exists():
        raise FileExistsError(str(target))
    started = time.monotonic()
    src = sqlite3.connect(source.resolve().as_uri() + '?mode=ro', uri=True)
    src.execute('PRAGMA cache_size=-8192')
    src.execute('BEGIN')
    cols = {r[1] for r in src.execute('PRAGMA table_info(files)')}
    if {'name', 'name_fold'} <= cols:
        src.create_function('gm_basename', 1, lambda p: p.rsplit('/', 1)[-1], deterministic=True)
        src.create_function('gm_casefold', 1, str.casefold, deterministic=True)
        inconsistent = src.execute("SELECT 1 FROM files WHERE name!=gm_basename(path) OR name_fold!=gm_casefold(gm_basename(path)) LIMIT 1").fetchone()
        if inconsistent:
            src.close()
            raise ValueError('Refusing to discard inconsistent basename fields')
    c = sqlite3.connect(target)
    try:
        c.executescript('''PRAGMA journal_mode=DELETE; PRAGMA synchronous=NORMAL;
            PRAGMA temp_store=MEMORY; PRAGMA cache_size=-16384;
            CREATE TABLE files(id INTEGER PRIMARY KEY,path TEXT NOT NULL,
                size INTEGER NOT NULL,modified TEXT NOT NULL,path_fold TEXT NOT NULL);
            CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);''')
        source_digest = hashlib.sha256()
        count = total = 0
        cursor = src.execute('SELECT ' + CORE_FIELDS + ' FROM files ORDER BY id')
        while True:
            rows = cursor.fetchmany(2000)
            if not rows:
                break
            c.executemany('INSERT INTO files VALUES(?,?,?,?,?)', rows)
            for row in rows:
                encoded = json.dumps(row, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
                source_digest.update(len(encoded).to_bytes(8, 'big')); source_digest.update(encoded)
                count += 1; total += row[2]
            if target.stat().st_size > max_bytes:
                raise RuntimeError('Compact database exceeds configured byte limit')
        c.executemany('INSERT INTO metadata VALUES(?,?)', src.execute('SELECT key,value FROM metadata'))
        src.close()
        c.executescript('''CREATE INDEX path_order ON files(path_fold);
            CREATE VIRTUAL TABLE names_fts USING fts5(path_fold,content='files',
                content_rowid='id',tokenize='trigram',detail=none,columnsize=0);
            INSERT INTO names_fts(names_fts) VALUES('rebuild');
            INSERT INTO names_fts(names_fts) VALUES('optimize');''')
        row = c.execute("SELECT value FROM metadata WHERE key='manifest'").fetchone()
        manifest = json.loads(row[0]) if row else {}
        manifest.update(schema_version=SCHEMA_VERSION, files=count, listed_bytes=total,
                        logical_rows_sha256=source_digest.hexdigest(), index_detail='none',
                        indexed_fields=['path_fold'])
        c.execute('INSERT OR REPLACE INTO metadata VALUES(?,?)', ('manifest', json.dumps(manifest)))
        c.execute('PRAGMA user_version=2')
        c.commit()
        # Drop merge freelist pages in the candidate, never VACUUM a live database.
        c.execute('VACUUM')
        actual_count, actual_total, actual_digest = logical_digest(c)
        if (actual_count, actual_total, actual_digest) != (count, total, source_digest.hexdigest()):
            raise RuntimeError('Record preservation check failed')
        if c.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise RuntimeError('Compact database integrity check failed')
        c.close()
        if target.stat().st_size > max_bytes:
            raise RuntimeError('Compact database exceeds configured byte limit')
        return {'schema_version':SCHEMA_VERSION, 'files':count, 'listed_bytes':total,
                'logical_rows_sha256':actual_digest, 'all_records_identical':True,
                'old_bytes':source.stat().st_size, 'new_bytes':target.stat().st_size,
                'build_seconds':round(time.monotonic()-started,3)}
    except BaseException:
        c.close(); src.close()
        # Candidate is disposable, while the original database remains intact.
        for suffix in ('', '-journal', '-wal', '-shm'):
            Path(str(target) + suffix).unlink(missing_ok=True)
        raise
