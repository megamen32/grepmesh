#!/usr/bin/env python3
"""Isolated RAM-only offload ACL regression; synthetic corpus, no production resources.

This is not production signer acceptance: signatures here use a one-process
ephemeral test key. Publication still needs an authenticated real client, a
Hub-bound profile, source parity and reboot/rollback acceptance.
"""
import hashlib
import hmac
import io
import json
import os
import pathlib
import secrets
import socket
import tempfile
import threading
import time
import urllib.error
import urllib.request

from cloud_index import (
    ACL_POLICY_HEADER, ACL_SECRET_ENV, ACL_SIGNATURE_HEADER, MemoryStore,
    PRIVATE_REQUIRE_VERIFIED_ACL_ENV, Publisher, Reader, document, json_bytes,
    serve,
)

ROOT = pathlib.Path('/run/user') / str(os.getuid()) / 'grepmesh-offload-acl-fixtures'
assert ROOT.is_dir() and ROOT.stat().st_dev != pathlib.Path('/home').stat().st_dev, (
    'Explicit user tmpfs required; never write an offload test index to SSD')
os.environ[ACL_SECRET_ENV] = secrets.token_hex(32)
os.environ[PRIVATE_REQUIRE_VERIFIED_ACL_ENV] = '1'
checks = []

def check(label, condition):
    if not condition:
        raise AssertionError(label)
    checks.append(label)

def policy(roots):
    return {
        'version': 1, 'profile_id': 'synthetic-acl-test', 'profile_version': 2,
        'roots_digest': 'a' * 64, 'args_sha256': 'b' * 64, 'job_id': 'synthetic-job',
        'target': 'shell:synthetic-test', 'tool': 'mcp_call', 'read_roots': list(roots),
    }

def signed(body, roots):
    raw = json.dumps(policy(roots), separators=(',', ':'), sort_keys=True)
    digest = hmac.new(os.environ[ACL_SECRET_ENV].encode(), body + b'\n' + raw.encode(),
                      hashlib.sha256).hexdigest()
    return {ACL_POLICY_HEADER: raw, ACL_SIGNATURE_HEADER: digest}

def fixture_stream():
    rows = [
        {'kind': 'begin', 'format': 1, 'source_host': 'synthetic-server44',
         'source_root': '/', 'policy': 'synthetic'},
    ]
    docs = [
        document('/home/user/readable.md', 'authorized alpha'),
        document('/dev/input/event0', 'denied alpha'),
    ]
    digest = hashlib.sha256()
    for d in docs:
        rows.append(d)
        digest.update(json_bytes([d['path'], d['sha256']]))
    rows.append({'kind': 'end', 'count': len(docs),
                 'logical_sha256': digest.hexdigest(), 'skipped': {},
                 'complete_for_policy': True})
    return io.StringIO(''.join(json.dumps(row) + '\n' for row in rows))

def rpc(port, name, args, auth_roots=None, tamper=False, metadata=False, tamper_metadata=False):
    body = json_bytes({'jsonrpc': '2.0', 'id': 7, 'method': 'tools/call',
                       'params': {'name': name, 'arguments': args}})
    headers = {'Content-Type': 'application/json'}
    if auth_roots is not None:
        headers.update(signed(body, auth_roots))
    if metadata:
        headers['X-GrepMesh-Metadata-Signature'] = hmac.new(
            os.environ[ACL_SECRET_ENV].encode(), b'grepmesh-metadata-v1\n' + body,
            hashlib.sha256).hexdigest()
    if tamper_metadata:
        headers['X-GrepMesh-Metadata-Signature'] = '0' * 64
    if tamper:
        headers[ACL_SIGNATURE_HEADER] = '0' * 64
    req = urllib.request.Request('http://127.0.0.1:%d' % port,
                                 data=body, headers=headers)
    return json.loads(urllib.request.urlopen(req, timeout=8).read())

def error(response):
    return response['result'].get('isError') is True

def payload(response):
    return json.loads(response['result']['content'][0]['text'])

with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
    store = MemoryStore()
    Publisher(store, tmp, 64 * 1024).publish('private-offload', fixture_stream())
    reader = Reader(store, tmp, 'private-offload', 16 * 1024 * 1024,
                    'synthetic-server44', '/')
    probe = socket.socket()
    probe.bind(('127.0.0.1', 0))
    port = probe.getsockname()[1]
    probe.close()
    thread = threading.Thread(target=serve, args=(reader, port), daemon=True)
    thread.start()
    for _ in range(80):
        try:
            urllib.request.urlopen('http://127.0.0.1:%d' % port, timeout=1)
            break
        except urllib.error.URLError:
            time.sleep(0.05)
    else:
        raise AssertionError('synthetic loopback reader did not start')
    base = {'hop_count': 1, 'origin_host': 'synthetic-peer', 'hosts': ['local']}
    search_args = dict(base, query='alpha')
    find_args = dict(base, pattern='*.md')
    read_args = dict(base, host='local', path='/home/user/readable.md')
    denied_args = dict(base, host='local', path='/dev/input/event0')
    for name, args in [
        ('search_status', base), ('search', search_args),
        ('find_paths', find_args), ('read_text', read_args),
    ]:
        check('unsigned %s refused in private mode' % name,
              error(rpc(port, name, args)))
    for name in ['search_status', 'list_locations']:
        check('signed metadata %s succeeds' % name,
              not error(rpc(port, name, base, metadata=True)))
    check('forged metadata signature refused',
          error(rpc(port, 'search_status', base, metadata=True, tamper_metadata=True)))
    for name, args in [('search', search_args), ('find_paths', find_args),
                       ('read_text', read_args)]:
        check('metadata signature cannot authorize %s' % name,
              error(rpc(port, name, args, metadata=True)))
    check('forged HMAC refused', error(rpc(port, 'search', search_args,
                                           auth_roots=['/home'], tamper=True)))
    for forbidden in [['/'], ['/dev'], ['/dev/input'], ['/home', '/dev']]:
        check('signed forbidden roots %s refused' % forbidden,
              error(rpc(port, 'search', search_args, auth_roots=forbidden)))
    allowed = rpc(port, 'search', search_args, auth_roots=['/home'])
    check('signed narrow search succeeds', not error(allowed))
    check('source_root slash does not leak /dev in signed search',
          [r['path'] for r in payload(allowed)['results']] ==
          ['/home/user/readable.md'])
    found = rpc(port, 'search', dict(base, query='path:readable.md'), auth_roots=['/home'])
    check('signed public search path syntax finds filename',
          not error(found) and [r['path'] for r in payload(found)['results']]==['/home/user/readable.md']
          and not payload(found)['partial'])
    missing = rpc(port, 'search', dict(base, query='path:absent-qa.md'), auth_roots=['/home'])
    check('signed public path miss is complete',
          not error(missing) and not payload(missing)['results'] and not payload(missing)['partial'])
    denied_path = rpc(port, 'search', dict(base, query='path:event0'), auth_roots=['/home'])
    check('public path syntax retains content ACL',
          not error(denied_path) and not payload(denied_path)['results'])
    check('signed /home read_text succeeds',
          not error(rpc(port, 'read_text', read_args, auth_roots=['/home'])))
    check('signed /home cannot read /dev',
          error(rpc(port, 'read_text', denied_args, auth_roots=['/home'])))
    check('signed /home find_paths succeeds',
          not error(rpc(port, 'find_paths', find_args, auth_roots=['/home'])))
    check('signed /home find_paths only reveals allowed data',
          [r['path'] for r in payload(rpc(port, 'find_paths', find_args,
                                        auth_roots=['/home']))['results']] ==
          ['/home/user/readable.md'])
    check('only RAM tmpfs cache, no durable index',
          all(pathlib.Path(p).stat().st_dev == ROOT.stat().st_dev
              for p in pathlib.Path(tmp).rglob('*')))
print('%d passed (synthetic test only; no production ACL proof)' % len(checks))
