#!/usr/bin/env python3
"""One-time read-only native-index seed for refresh_source; stdlib, no rebuild.

This exports existing indexed history, not a fresh filesystem inventory. It is
used only to move an existing Mac corpus before retiring its native database.
Subsequent updates use the source exporter instead.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import sys


def canon(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--db', type=Path, required=True)
    ap.add_argument('--host', required=True)
    args = ap.parse_args()
    connection = sqlite3.connect(args.db.resolve(strict=True).as_uri() + '?mode=ro', uri=True)
    connection.execute('PRAGMA query_only=ON')
    connection.execute('PRAGMA cache_size=-8192')
    connection.execute('BEGIN')
    print(json.dumps({'kind': 'begin', 'format': 1, 'source_host': args.host,
                     'source_root': '/', 'policy': 'one-time native indexed history seed; subsequent generations scan source files'}))
    count = 0
    logical = hashlib.sha256()
    try:
        for path, body in connection.execute('SELECT c0,c1 FROM grepmesh_documents_content ORDER BY rowid'):
            if not isinstance(path, str) or not path.startswith('/') or not isinstance(body, str):
                raise ValueError('Invalid native indexed document')
            raw = body.encode('utf-8')
            if len(raw) > 64 * 1024 * 1024:
                raise ValueError('Native body exceeds supported 64MiB cloud document bound')
            sha = hashlib.sha256(raw).hexdigest()
            logical.update(canon([path, sha]))
            print(json.dumps({'kind': 'document', 'path': path, 'body': body,
                              'size': len(raw), 'mtime_ns': 0, 'sha256': sha}, ensure_ascii=False))
            count += 1
    finally:
        connection.close()
    print(json.dumps({'kind': 'end', 'count': count,
                      'logical_sha256': logical.hexdigest(), 'complete_for_policy': True,
                      'skipped': {}}), flush=True)


if __name__ == '__main__':
    main()
