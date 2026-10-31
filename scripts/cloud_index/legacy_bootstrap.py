#!/usr/bin/env python3
"""One-time, read-only migration of already extracted GrepMesh FTS/OCR text.

This is deliberately NOT an incremental source scanner. It uses an existing
legacy index only to avoid repeating expensive PDF/OCR extraction at migration.
The native index and documents are never modified. Only explicit narrow roots.
"""
from __future__ import annotations
import argparse, hashlib, json, os, pathlib, sqlite3, sys
from urllib.parse import quote

from cloud_index import (
    EXTENSIONS, MAX_DOCUMENT, SECRET_NAMES, document, json_bytes,
)

P = pathlib.Path
SUPPORTED = EXTENSIONS | {
    '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx',
    '.png', '.jpg', '.jpeg', '.webp', '.tif', '.tiff',
}

def scan_legacy(root: P, db: P, host: str, output, max_files=10000):
    root = root.resolve(strict=True)
    db = db.resolve(strict=True)
    if not root.is_dir():
        raise ValueError('An explicit readable source directory is required')
    if root == P('/') or len(root.parts) < 4:
        raise ValueError('The migration root must be narrow, never a whole home/disk')
    if not db.is_file() or not db.name.endswith(('.sqlite', '.sqlite3')):
        raise ValueError('An existing legacy SQLite database must be explicit')
    if not host or any(c.isspace() for c in host):
        raise ValueError('A stable source host ID is required')
    if not 1 <= max_files <= 100000:
        raise ValueError('Bounded migration requires max_files between 1 and 100000')
    connection = sqlite3.connect('file:' + quote(str(db)) + '?mode=ro', uri=True, timeout=3)
    connection.execute('PRAGMA query_only=ON')
    connection.execute('PRAGMA cache_size=-4096')
    low = str(root).rstrip('/') + '/'
    high = str(root).rstrip('/') + '0'
    skipped = {}
    def skip(why):
        skipped[why] = skipped.get(why, 0) + 1
    header = {
        'kind': 'begin', 'format': 1, 'source_host': host, 'source_root': str(root),
        'policy': 'legacy GrepMesh FTS/OCR bootstrap; verified current regular files,'
                  ' <=1MiB extracted UTF-8 text; no symlinks/secrets',
    }
    output.write(json.dumps(header, ensure_ascii=False) + '\n')
    count = 0
    digest = hashlib.sha256()
    # Path range is indexed by the extraction_cache PRIMARY KEY. No full DB scan.
    sql = ('SELECT cache.path,cache.size,cache.mtime_ns,cache.body,rows.fts_rowid '
           'FROM grepmesh_extraction_cache cache '
           'JOIN grepmesh_document_rows rows ON rows.path=cache.path '
           'WHERE cache.path>=? AND cache.path<? ORDER BY cache.path')
    try:
        for path, size, mtime, cache_body, fts_rowid in connection.execute(sql, (low, high)):
            candidate = P(path)
            if candidate.suffix.lower() not in SUPPORTED and candidate.name not in {'Dockerfile','Makefile','LICENSE','AGENTS.md'}:
                skip('unsupported_format'); continue
            relative = candidate.relative_to(root)
            if any(part in SECRET_NAMES or part.startswith('.env.') for part in relative.parts):
                skip('secret_name'); continue
            if candidate.is_symlink():
                skip('symlink'); continue
            # Refuse stale data and paths which have disappeared, not silently return false coverage.
            canonical = candidate.resolve(strict=True)
            if not canonical.is_relative_to(root) or not candidate.is_file():
                raise ValueError('Source path escaped the selected root')
            stat = candidate.stat()
            if stat.st_size != int(size) or stat.st_mtime_ns != int(mtime):
                raise RuntimeError('Indexed source changed; refresh source before publishing')
            fts = connection.execute('SELECT body FROM grepmesh_documents WHERE rowid=?', (fts_rowid,)).fetchone()
            if fts is None:
                raise RuntimeError('Missing FTS body; migration would lose coverage')
            body = fts[0]
            if body is None:
                raise RuntimeError('Null legacy FTS body')
            if cache_body and cache_body != body:
                raise RuntimeError('Legacy extraction cache and FTS body disagree; migration stopped')
            if len(body.encode('utf-8')) > MAX_DOCUMENT:
                skip('oversize_extracted_text'); continue
            if '\x00' in body:
                skip('nul_extracted_text'); continue
            d = document(path, body, stat.st_size, stat.st_mtime_ns)
            digest.update(json_bytes([d['path'], d['sha256']]))
            output.write(json.dumps(d, ensure_ascii=False) + '\n')
            count += 1
            if count > max_files:
                raise RuntimeError('Too many documents for this migration scope')
        output.write(json.dumps({
            'kind': 'end', 'count': count,
            'logical_sha256': digest.hexdigest(),
            'skipped': skipped, 'complete_for_policy': True,
        }, ensure_ascii=False) + '\n')
        output.flush()
    finally:
        connection.close()

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--source-root', type=P, required=True)
    ap.add_argument('--source-host', required=True)
    ap.add_argument('--legacy-db', type=P, required=True)
    ap.add_argument('--max-files', type=int, default=10000)
    args = ap.parse_args()
    if hasattr(os, 'sched_setaffinity'):
        os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:2])
    os.nice(10)
    scan_legacy(args.source_root, args.legacy_db, args.source_host,
                sys.stdout, max_files=args.max_files)

if __name__ == '__main__':
    main()
