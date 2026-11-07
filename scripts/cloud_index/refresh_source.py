#!/usr/bin/env python3
"""Publish a complete source stream in RAM, then switch one host's cloud scopes.

Input is the existing begin/document/end JSONL contract. No native database is
needed. Immutable objects are uploaded in one rclone batch; the reader config
changes only after that batch succeeds. Original files are never modified.
"""
from __future__ import annotations
import argparse
import contextlib
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from cloud_index import (CloudStore, Publisher, MAX_DOCUMENT, digest,
                         ensure_tmpfs, json_bytes, safe_scope, valid_key)
from oversize_support import MAX_OVERSIZE_DOCUMENT, publish_oversize


class StagingStore:
    def __init__(self, root, reserve):
        self.root, self.reserve, self.puts = root, reserve, 0

    def put(self, key, data):
        path = self.root / valid_key(key)
        if shutil.disk_usage(self.root).free < len(data) + self.reserve:
            raise RuntimeError('RAM staging reserve exhausted')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        self.puts += 1


def partition(stream, root, host, source_root, buckets, reserve):
    """Keep compressed streams only; a failed/incomplete scan cannot be published."""
    if not 1 <= buckets <= 64:
        raise ValueError('Expected 1..64 partitions')
    header = json.loads(next(stream))
    if (header.get('kind') != 'begin' or header.get('format') != 1
            or header.get('source_host') != host
            or header.get('source_root') != source_root):
        raise ValueError('Source identity does not match admitted source')
    counts = [0] * buckets
    hashes = [hashlib.sha256() for _ in counts]
    source_hash = hashlib.sha256()
    total = oversized = 0
    footer = None
    with contextlib.ExitStack() as stack:
        outputs = [stack.enter_context(gzip.open(root / f'{i}.gz', 'wt',
                    encoding='utf-8', compresslevel=1)) for i in range(buckets)]
        large = stack.enter_context(gzip.open(root / 'oversize.gz', 'wt',
                                    encoding='utf-8', compresslevel=1))
        for out in outputs:
            out.write(json.dumps(header, ensure_ascii=False) + '\n')
        for line in stream:
            if len(line) > MAX_OVERSIZE_DOCUMENT * 7:
                raise ValueError('Source record exceeds extraction bound')
            item = json.loads(line)
            if item.get('kind') == 'end':
                footer = item
                break
            if item.get('kind') != 'document':
                raise ValueError('Invalid source record')
            path, body = item['path'], item['body']
            p = Path(path)
            if not p.is_absolute() or '..' in p.parts or not p.is_relative_to(source_root):
                raise ValueError('Path outside admitted source')
            raw = body.encode('utf-8')
            if len(raw) > MAX_OVERSIZE_DOCUMENT or digest(raw) != item['sha256']:
                raise ValueError('Invalid source body')
            pair = json_bytes([path, item['sha256']])
            source_hash.update(pair)
            total += 1
            if len(raw) > MAX_DOCUMENT:
                large.write(line)
                oversized += 1
            else:
                bucket = int.from_bytes(hashlib.sha256(path.encode()).digest()[:8], 'big') % buckets
                outputs[bucket].write(line)
                counts[bucket] += 1
                hashes[bucket].update(pair)
            if total % 1000 == 0:
                for out in outputs:
                    out.flush()
                large.flush()
                if shutil.disk_usage(root).free < reserve:
                    raise RuntimeError('RAM stream reserve exhausted')
        if (not footer or footer.get('complete_for_policy') is not True
                or footer.get('count') != total
                or footer.get('logical_sha256') != source_hash.hexdigest()
                or any(line.strip() for line in stream)):
            raise ValueError('Incomplete source scan; current scopes preserved')
        for i, out in enumerate(outputs):
            out.write(json.dumps({'kind': 'end', 'count': counts[i],
                      'logical_sha256': hashes[i].hexdigest(),
                      'complete_for_policy': True, 'skipped': footer.get('skipped', {})}) + '\n')
    # Include policy and per-document metadata in publication identity: body-only
    # equality must not make a changed source policy reuse an old manifest.
    identity = hashlib.sha256(json_bytes({'header': header,
                                         'layout': 'source-refresh-v2-oversize-4m'}))
    for i in range(buckets):
        with gzip.open(root / f'{i}.gz', 'rb') as f:
            for block in iter(lambda: f.read(1024 * 1024), b''):
                identity.update(block)
    with gzip.open(root / 'oversize.gz', 'rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            identity.update(block)
    return {'generation': identity.hexdigest()[:24], 'documents': total,
            'oversized': oversized, 'counts': counts, 'skipped': footer.get('skipped', {})}


def build(staging, scratch, receipt, host, source_root, buckets):
    stem = safe_scope('live-' + host + '-' + receipt['generation'])
    entries = []
    for i in range(buckets):
        scope = safe_scope(stem + f'-b{i:02}')
        with gzip.open(scratch / f'{i}.gz', 'rt', encoding='utf-8') as stream:
            Publisher(staging, scratch, 16 * 1024 * 1024, 8192).publish(scope, stream)
        entries.append({'scope': scope, 'source_host': host, 'source_root': source_root,
                        'partition_count': buckets})
    if receipt['oversized']:
        snapshot = receipt['generation']
        with gzip.open(scratch / 'oversize.gz', 'rt', encoding='utf-8') as stream:
            publish_oversize(staging, host, snapshot, (json.loads(x) for x in stream),
                             chunk_bytes=4 * 1024 * 1024)
        entries[0]['oversize'] = {'host': host, 'snapshot': snapshot}
    return entries


def atomic_json(path, data):
    fd, temp = tempfile.mkstemp(prefix=path.name + '.', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as out:
            out.write(json_bytes(data))
            out.flush()
            os.fsync(out.fileno())
        os.replace(temp, path)
    finally:
        Path(temp).unlink(missing_ok=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--host', required=True)
    ap.add_argument('--source-root', required=True)
    ap.add_argument('--root', type=Path, required=True)
    ap.add_argument('--reader-config', type=Path, required=True)
    ap.add_argument('--buckets', type=int, default=16)
    ap.add_argument('--reserve-mib', type=int, default=2048)
    ap.add_argument('--restart-service')
    args = ap.parse_args()
    safe_scope(args.host)
    ensure_tmpfs(args.root)
    if not Path(args.source_root).is_absolute() or args.reserve_mib < 256:
        raise ValueError('Absolute root and RAM reserve >=256 MiB required')
    if args.restart_service and (not args.restart_service.startswith('grepmesh-')
                                or not args.restart_service.endswith('.service')):
        raise ValueError('Only a GrepMesh reader service may be restarted')
    with open(args.root / 'refresh.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with tempfile.TemporaryDirectory(prefix='source-', dir=args.root) as tmp:
            scratch = Path(tmp)
            reserve = args.reserve_mib * 1024 * 1024
            receipt = partition(sys.stdin, scratch, args.host, args.source_root,
                                args.buckets, reserve)
            current = json.loads(args.reader_config.read_text())
            if not isinstance(current, list):
                raise ValueError('Reader scopes must be a list')
            prefix = 'live-' + args.host + '-' + receipt['generation'] + '-b'
            expected = {prefix + f'{i:02}' for i in range(args.buckets)}
            existing = {x['scope'] for x in current if x['source_host'] == args.host}
            if existing == expected:
                print(json.dumps({'action': 'unchanged', 'host': args.host, **receipt}))
                return
            staging_dir = scratch / 'upload'
            staging_dir.mkdir()
            staging = StagingStore(staging_dir, reserve)
            entries = build(staging, scratch, receipt, args.host, args.source_root, args.buckets)
            replacement = [x for x in current if x['source_host'] != args.host] + entries
            if len(replacement) > 128:
                raise ValueError('Reader scope capacity exceeded')
            # Local rclone uses the owner's existing authentication. Do not copy
            # credentials to source hosts. A batch failure leaves config intact.
            if os.environ.get('GREPMESH_CLOUD_GATEWAY') != 'local':
                raise ValueError('Run batch publisher on the existing rclone owner host')
            cloud = CloudStore()
            subprocess.run(cloud.command(['copy', str(staging_dir), cloud.prefix,
                           '--checksum', '--transfers', '8', '--checkers', '16',
                           '--stats', '30s', '--stats-one-line', '--stats-log-level', 'NOTICE']),
                           stdin=subprocess.DEVNULL, check=True, timeout=10800)
            # Other hosts may upload concurrently using separate RAM roots.
            # Merge against the latest config under a short shared commit lock.
            with open(str(args.reader_config) + '.lock', 'a') as config_lock:
                fcntl.flock(config_lock, fcntl.LOCK_EX)
                current = json.loads(args.reader_config.read_text())
                replacement = [x for x in current if x['source_host'] != args.host] + entries
                if len(replacement) > 128:
                    raise ValueError('Reader scope capacity exceeded')
                atomic_json(args.reader_config, replacement)
                try:
                    if args.restart_service:
                        subprocess.run(['systemctl', '--user', 'restart', args.restart_service],
                                       check=True, timeout=60)
                except BaseException:
                    atomic_json(args.reader_config, current)
                    raise
            print(json.dumps({'action': 'published', 'host': args.host,
                              'objects': staging.puts, **receipt}), flush=True)


if __name__ == '__main__':
    main()
