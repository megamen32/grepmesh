#!/usr/bin/env python3
"""Stream a bounded, current, explicitly admitted source tree without SQLite.

This is a read-only replacement for Git-only source scans when untracked
but currently existing text/code should also be searchable. It does not
extract PDF/OCR: that requires a separate, validated extractor.
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import pathlib
import re
import stat
import sys

from cloud_index import MAX_DOCUMENT, EXTENSIONS, SECRET_NAMES, document, json_bytes

P = pathlib.Path
ALLOWED_PARENT = P('/home/roomhacker/agents-projects')
MAX_FILES = 50000
MAX_SOURCE_TEXT = 256 * 1024 * 1024
FORMATS = EXTENSIONS | {
    '.templ', '.proto', '.mod', '.sum', '.svg', '.ini',
    '.conf', '.properties', '.gradle', '.graphql', '.gql',
    '.service', '.tsx', '.vue', '.lua', '.kt', '.java', '.swift',
    '.rb', '.rs', '.go', '.hcl', '.tf', '.tfvars', '.ipynb',
}
SPECIAL_NAMES = {'Dockerfile', 'Makefile', 'LICENSE', 'AGENTS.md', 'go.mod', 'go.sum'}
SKIP_TOP_DIRS = {'.tmp', '.worktrees', 'var', 'graphify-out'}
BACKUP_DIR_PREFIXES = ('dist.backup-', 'dist.bak-', 'dist.pre-', 'dist.pre-final-')
SKIP_DIRS = {
    '.git', '.hg', '.svn', 'node_modules', '.pnpm-store', '.cache',
    '.next', '.nuxt', '.vite', '.turbo', '.venv', 'venv',
    '__pycache__', '.pytest_cache', '.mypy_cache', '.tox',
    '.gradle', 'target', 'dist', 'build', 'coverage', '.idea',
    '.output', '.svelte-kit', 'vendor', '.bundle', '.yarn',
    'site-packages', 'dist-packages', '.cargo', '.rustup',
}
SECRET_NAME_RE = re.compile(
    r'^(?:\.env(?:\..*)?|(?:auth|credentials|client_secrets|secrets|'
    r'tokens?|cookies?|passwords?|session|id_rsa|id_ed25519)'
    r'\.(?:json|jsonl|yaml|yml|toml|txt|db)|'
    r'\.npmrc|\.netrc|\.pypirc|\.git-credentials)$',
    re.IGNORECASE,
)


def admissible_root(root: P) -> P:
    parent = ALLOWED_PARENT.resolve(strict=True)
    root = root.resolve(strict=True)
    if (
        root == parent or
        not root.is_relative_to(parent) or
        len(root.relative_to(parent).parts) != 1 or
        not root.is_dir()
    ):
        raise ValueError('Explicit, one-project source directory required')
    return root


def scan_tree(root: P, host: str, output, *, max_files=MAX_FILES,
              max_text_bytes=MAX_SOURCE_TEXT):
    root = admissible_root(root)
    if not host or any(c.isspace() for c in host):
        raise ValueError('Stable source host identifier is required')
    if not 1 <= max_files <= MAX_FILES or not 1 <= max_text_bytes <= MAX_SOURCE_TEXT:
        raise ValueError('Scan limits are not within admitted maximums')
    excluded = collections.Counter()
    count = 0
    body_bytes = 0
    digest = hashlib.sha256()
    header = {
        'kind': 'begin', 'format': 1, 'source_host': host,
        'source_root': str(root),
        'policy': 'current UTF-8 text/code in one project; no symlinks, secrets, '
                  'generated dependencies, binaries, PDFs or OCR; file <=1 MiB',
    }
    output.write(json.dumps(header, ensure_ascii=False) + '\n')
    for current, dirs, files in os.walk(root, topdown=True, followlinks=False):
        dirs.sort()
        retained = []
        for d in dirs:
            path = P(current) / d
            if d in SKIP_DIRS or (P(current) == root and d in SKIP_TOP_DIRS) or d.startswith(BACKUP_DIR_PREFIXES):
                excluded['generated_or_dependency_dir'] += 1
            elif path.is_symlink():
                excluded['symlink_dir'] += 1
            elif d.startswith('.env'):
                excluded['secret_dir'] += 1
            else:
                retained.append(d)
        dirs[:] = retained
        for name in sorted(files):
            if SECRET_NAME_RE.match(name) or name in SECRET_NAMES:
                excluded['secret_name'] += 1
                continue
            path = P(current) / name
            if path.suffix.lower() not in FORMATS and name not in SPECIAL_NAMES:
                excluded['unsupported_extension'] += 1
                continue
            if path.is_symlink():
                excluded['symlink_file'] += 1
                continue
            try:
                fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
            except FileNotFoundError as exc:
                raise RuntimeError('Source changed during scan') from exc
            try:
                before = os.fstat(fd)
                if not stat.S_ISREG(before.st_mode):
                    excluded['nonregular'] += 1
                    continue
                if before.st_size > MAX_DOCUMENT:
                    excluded['oversize'] += 1
                    continue
                with os.fdopen(os.dup(fd), 'rb') as f:
                    raw = f.read(MAX_DOCUMENT + 1)
                after = os.fstat(fd)
                if (before.st_size != after.st_size
                        or before.st_mtime_ns != after.st_mtime_ns
                        or len(raw) != after.st_size):
                    raise RuntimeError('Source changed during scan')
            finally:
                os.close(fd)
            if b'\0' in raw:
                excluded['binary'] += 1
                continue
            try:
                body = raw.decode('utf-8')
            except UnicodeDecodeError:
                excluded['not_utf8'] += 1
                continue
            body_bytes += len(raw)
            count += 1
            if count > max_files or body_bytes > max_text_bytes:
                raise RuntimeError('Source exceeds bounded per-scope capacity')
            canon = path.resolve(strict=True)
            if not canon.is_relative_to(root):
                raise ValueError('Source escaped admitted root')
            item = document(str(canon), body, after.st_size, after.st_mtime_ns)
            digest.update(json_bytes([item['path'], item['sha256']]))
            output.write(json.dumps(item, ensure_ascii=False) + '\n')
            if count % 200 == 0:
                output.flush()
    footer = {
        'kind': 'end', 'count': count, 'logical_sha256': digest.hexdigest(),
        'complete_for_policy': True, 'skipped': dict(excluded),
    }
    output.write(json.dumps(footer, ensure_ascii=False) + '\n')
    output.flush()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['scan'])
    parser.add_argument('--source-root', type=P, required=True)
    parser.add_argument('--source-host', required=True)
    parser.add_argument('--max-files', type=int, default=MAX_FILES)
    parser.add_argument('--max-text-mib', type=int, default=256)
    args = parser.parse_args()
    if hasattr(os, 'sched_setaffinity'):
        os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:2])
    os.nice(10)
    scan_tree(args.source_root, args.source_host, sys.stdout,
              max_files=args.max_files,
              max_text_bytes=args.max_text_mib * 1024 * 1024)


if __name__ == '__main__':
    main()
