#!/usr/bin/env python3
"""Lossless, immutable cloud archive of GrepMesh's native source-data tables.

This is a BACKUP, not a replacement search index. In particular, it retains
bodies whose original paths have disappeared, all extracted OCR text and
metadata, and source rows independently from regeneratable FTS5 token tables.

A consistent SQLite WAL read snapshot spans all tables. Uploads are verified
before a final manifest/pointer appears. No persistent scratch is permitted.
"""
from __future__ import annotations
import argparse
import base64
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import time

from cloud_index import CloudStore, ensure_tmpfs, digest, json_bytes

TABLES = (
    'grepmesh_metadata', 'grepmesh_document_rows',
    'grepmesh_documents_content', 'grepmesh_extraction_cache',
    'grepmesh_pending_ocr', 'grepmesh_ocr_attempts',
    'grepmesh_ocr_empty_results',
)
MIB = 1024 * 1024
MAX_RECORD = 192 * MIB
WAL_LIMIT = 3 * 1024 * MIB
CHUNK_RAW = 96 * MIB
CHUNK_PACKED = 64 * MIB


def encode_row(row):
    values = []
    for field in row:
        if isinstance(field, bytes):
            values.append({'$base64': base64.b64encode(field).decode('ascii')})
        elif field is None or isinstance(field, (str, int, float)):
            values.append(field)
        else:
            raise TypeError('Unserializable SQLite column')
    line = json_bytes(values) + b'\n'
    if len(line) > MAX_RECORD:
        raise ValueError('A legacy row exceeds the bounded archive record size')
    return line


def export(db_path: Path, host: str, tmpfs: Path, snapshot: str,
           cloud: CloudStore, chunk_raw: int = CHUNK_RAW):
    if not re.fullmatch(r'server-[0-9]+', host):
        raise ValueError('Only explicitly named fleet server IDs are allowed')
    if not re.fullmatch(r'[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}', snapshot):
        raise ValueError('Invalid immutable snapshot ID')
    ensure_tmpfs(tmpfs)
    if not db_path.is_file() or not db_path.name.endswith('.sqlite3'):
        raise ValueError('Explicit existing native SQLite database required')
    if not 4 * MIB <= chunk_raw <= CHUNK_RAW:
        raise ValueError('Chunk size must be 4..96 MiB')
    if not cloud.prefix.startswith('yadisk:Backups/GrepMesh/'):
        raise ValueError('Cloud archive destination must be private GrepMesh backup')
    base = f'archives/{host}/{snapshot}'
    # Do not reuse a complete snapshot ID.
    try:
        cloud.get(base + '/complete.json', 256 * 1024)
    except RuntimeError:
        pass
    else:
        raise ValueError('Immutable native snapshot already exists')

    conn = sqlite3.connect('file:' + db_path.as_posix() + '?mode=ro',
                           uri=True, timeout=10)
    conn.execute('PRAGMA query_only=ON')
    conn.execute('PRAGMA cache_size=-2048')
    conn.execute('PRAGMA temp_store=MEMORY')
    conn.execute('PRAGMA mmap_size=0')
    manifest = {
        'schema': 'grepmesh-native-logical-archive/v1',
        'host_id': host, 'snapshot_id': snapshot,
        'original_database': str(db_path),
        'source_bytes': db_path.stat().st_size,
        'start_utc': time.time(), 'tables': {},
        'rebuildable': ['grepmesh_documents_fts5_token_tables'],
        'retains_deleted_source_paths': True,
        'includes_ocr_cache': True,
        'format': 'gzip-json-lines-rows',
        'consistent_read_snapshot': True,
        'chunk_raw_target': chunk_raw,
    }
    started = time.monotonic()
    try:
        if conn.execute('PRAGMA journal_mode').fetchone()[0] != 'wal':
            raise RuntimeError('Online archival requires WAL snapshot semantics')
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        missing = sorted(set(TABLES) - names)
        if missing:
            raise RuntimeError('Required legacy tables absent: ' + ','.join(missing))
        # Start one consistent point-in-time read transaction. Native indexing
        # can continue writing, but the WAL must not grow without a hard cap.
        conn.execute('BEGIN')
        conn.execute("SELECT count(*) FROM sqlite_master").fetchone()
        manifest['schema_sql'] = [
            {'name': n, 'sql': sql}
            for n, sql in conn.execute("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name")
        ]
        manifest['pragma'] = {
            key: conn.execute('PRAGMA ' + key).fetchone()[0]
            for key in ('user_version', 'schema_version', 'page_size', 'auto_vacuum')
        }

        for table in TABLES:
            cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')]
            if not cols:
                raise RuntimeError('Missing SQLite table shape')
            row_total = 0
            encoded_total = 0
            chunks = []
            rolling = hashlib.sha256()
            buffer = io.BytesIO()
            packer = gzip.GzipFile(fileobj=buffer, mode='wb', compresslevel=1, mtime=0)
            pending_raw = 0
            pending_rows = 0
            start_rowid = None
            last_rowid = None

            def flush():
                nonlocal buffer, packer, pending_raw, pending_rows, start_rowid, last_rowid
                if not pending_rows:
                    return
                packer.close()
                payload = buffer.getvalue()
                if len(payload) > CHUNK_PACKED:
                    raise RuntimeError('Compressed chunk exceeded bound; no commit')
                sha = digest(payload)
                key = f'{base}/{table}/{len(chunks):05d}-{sha}.jsonl.gz'
                cloud.put(key, payload)
                chunks.append({
                    'key': key, 'sha256': sha,
                    'compressed_bytes': len(payload),
                    'raw_bytes': pending_raw, 'rows': pending_rows,
                    'first_rowid': start_rowid, 'last_rowid': last_rowid,
                })
                print(json.dumps({'event': 'chunk_uploaded', 'host': host,
                      'table': table, 'rows_total': row_total,
                      'chunk': len(chunks), 'cloud_mib': round(len(payload)/MIB, 2),
                      'elapsed_s': round(time.monotonic()-started,1)}), flush=True)
                buffer = io.BytesIO()
                packer = gzip.GzipFile(fileobj=buffer, mode='wb', compresslevel=1, mtime=0)
                pending_raw = pending_rows = 0
                start_rowid = last_rowid = None

            # Include rowid explicitly for exact FTS -> content mapping.
            query = f'SELECT rowid,* FROM "{table}" ORDER BY rowid'
            cursor = conn.execute(query)
            for row in cursor:
                encoded = encode_row(row)
                if pending_raw and pending_raw + len(encoded) > chunk_raw:
                    flush()
                if not pending_rows:
                    start_rowid = row[0]
                packer.write(encoded)
                pending_raw += len(encoded)
                pending_rows += 1
                last_rowid = row[0]
                rolling.update(encoded)
                row_total += 1
                encoded_total += len(encoded)
                if row_total % 15000 == 0:
                    wal = Path(str(db_path) + '-wal')
                    if wal.exists() and wal.stat().st_size > WAL_LIMIT:
                        raise RuntimeError('Live WAL grew past 3 GiB; abort to protect SSD')
            flush()
            packer.close()
            manifest['tables'][table] = {
                'columns': ['rowid'] + cols,
                'rows': row_total, 'raw_bytes': encoded_total,
                'logical_sha256': rolling.hexdigest(), 'chunks': chunks,
            }
            print(json.dumps({'event':'table_complete', 'host':host,
                              'table':table, 'rows':row_total,
                              'chunks':len(chunks)}),flush=True)
        # Logical consistency between distinct row mapping and FTS contents.
        row_paths = manifest['tables']['grepmesh_document_rows']['rows']
        content_rows = manifest['tables']['grepmesh_documents_content']['rows']
        if content_rows != row_paths:
            raise RuntimeError(f'Index mapping differs from document content: {row_paths} != {content_rows}')
        conn.execute('ROLLBACK')
        manifest['end_utc'] = time.time()
        manifest['elapsed_seconds'] = round(time.monotonic()-started,2)
        blob = json_bytes(manifest)
        if len(blob) > 16 * MIB:
            raise RuntimeError('Archive manifest exceeds expected size')
        manifest_sha = digest(blob)
        manifest_key = base + '/manifest-' + manifest_sha + '.json'
        cloud.put(manifest_key, blob)
        receipt = json_bytes({
            'format': 1, 'complete': True, 'snapshot': snapshot,
            'manifest': manifest_key, 'sha256': manifest_sha,
            'host_id': host, 'document_rows': content_rows,
            'source_bytes': manifest['source_bytes'],
        })
        cloud.put(base + '/complete.json', receipt)
        print(json.dumps({'event':'complete','host':host,'documents':content_rows,
            'manifest':manifest_key,'manifest_sha256':manifest_sha,
            'all_tables':{t:v['rows'] for t,v in manifest['tables'].items()},
            'elapsed_s':manifest['elapsed_seconds']}),flush=True)
        return manifest
    finally:
        conn.close()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--database',type=Path,required=True)
    parser.add_argument('--host',required=True)
    parser.add_argument('--snapshot')
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--chunk-mib',type=int,default=96)
    args=parser.parse_args()
    snapshot=args.snapshot or time.strftime('%Y%m%dT%H%M%SZ',time.gmtime())+'-'+os.urandom(4).hex()
    export(args.database,args.host,args.root,snapshot,CloudStore(),args.chunk_mib*MIB)


if __name__=='__main__':
    main()
