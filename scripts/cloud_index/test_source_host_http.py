#!/usr/bin/env python3
"""Real loopback MCP and rclone against two synthetic tmpfs snapshots."""
import hashlib
import hmac
import io
import json
import multiprocessing
import os
from pathlib import Path
import shutil
import socket
import tempfile
import time
import unittest
from urllib.request import Request, urlopen
from unittest.mock import patch

import cloud_index
from cloud_index import CloudStore, Publisher, document, ensure_tmpfs, json_bytes, serve
from multi_cloud import MultiReader


def make_stream(host, body):
    item = document('/shared/answer.txt', body)
    digest = hashlib.sha256(json_bytes([item['path'], item['sha256']])).hexdigest()
    return io.StringIO(''.join(json.dumps(x) + '\n' for x in [
        {'kind': 'begin', 'format': 1, 'source_host': host,
         'source_root': '/shared', 'policy': 'fixture'}, item,
        {'kind': 'end', 'count': 1, 'logical_sha256': digest,
         'skipped': {}, 'complete_for_policy': True}]))


def start_server(prefix, root, port):
    scopes = [{'scope': 'first', 'source_host': 'server-100', 'source_root': '/shared'},
              {'scope': 'second', 'source_host': 'server-88', 'source_root': '/shared'}]
    serve(MultiReader(CloudStore(prefix), root, scopes, cache_mib=128), port)


class HttpSourceHostTests(unittest.TestCase):
    def test_signed_search_read_and_ambiguous_read(self):
        env = {'GREPMESH_CLOUD_ACL_SECRET': 'synthetic-test-key-' * 4,
               'GREPMESH_CLOUD_REQUIRE_VERIFIED_ACL': '1'}
        rclone = shutil.which('rclone')
        if not rclone:
            self.skipTest('rclone is not installed')
        with patch.dict(os.environ, env), patch.object(cloud_index, 'SOURCE_MACHINE', 'local'), \
             patch.object(cloud_index, 'RCLONE', rclone):
            self.run_http_scenario()

    def run_http_scenario(self):
        base = Path('/run/user') / str(os.getuid()) / 'grepmesh-cloud-multi-tests'
        ensure_tmpfs(base)
        with tempfile.TemporaryDirectory(dir=base) as tmp:
            prefix = str(Path(tmp) / 'objects')
            publisher = Publisher(CloudStore(prefix), tmp, 64 * 1024)
            publisher.publish('first', make_stream('server-100', 'common first host'))
            publisher.publish('second', make_stream('server-88', 'common second host'))
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            process = multiprocessing.Process(target=start_server, args=(prefix, tmp, port))
            process.start()
            try:
                for _ in range(100):
                    try:
                        with urlopen(f'http://127.0.0.1:{port}/', timeout=1) as response:
                            if response.status == 200:
                                break
                    except OSError:
                        if not process.is_alive():
                            raise AssertionError('reader exited during startup')
                        time.sleep(.1)
                else:
                    raise AssertionError('reader did not start')
                policy = {'version': 1, 'profile_id': 'qa', 'profile_version': 1,
                          'job_id': 'qa', 'target': 'shell:qa', 'tool': 'mcp_call',
                          'roots_digest': 'a' * 64, 'args_sha256': 'b' * 64,
                          'read_roots': ['/shared']}
                def call(name, args, signed=True, policy=policy):
                    args.update({'hop_count': 1, 'origin_host': 'qa'})
                    body = json_bytes({'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call',
                                       'params': {'name': name, 'arguments': args}})
                    headers = {'Content-Type': 'application/json'}
                    if signed:
                        acl = json_bytes(policy).decode()
                        signature = hmac.new(os.environ['GREPMESH_CLOUD_ACL_SECRET'].encode(),
                                             body + b'\n' + acl.encode(), hashlib.sha256).hexdigest()
                        headers.update({'X-GrepMesh-Filesystem-Acl': acl,
                                        'X-GrepMesh-Acl-Signature': signature})
                    with urlopen(Request(f'http://127.0.0.1:{port}/mcp', data=body, headers=headers),
                                 timeout=30) as response:
                        return json.load(response)['result']
                found = call('search_text', {'query': 'common'})
                hits = json.loads(found['content'][0]['text'])['results']
                self.assertEqual({h['source_host'] for h in hits}, {'server-100', 'server-88'})
                for host, expected in [('server-100', 'common first host'),
                                       ('server-88', 'common second host')]:
                    result = call('read_text', {'host': 'local', 'source_host': host,
                                                'path': '/shared/answer.txt'})
                    body = json.loads(result['content'][0]['text'])
                    self.assertEqual(body['source_host'], host)
                    self.assertEqual(body['chunks'][0]['lines'][0]['text'], expected)
                for args in ({'host': 'local', 'path': '/shared/answer.txt'},
                             {'host': 'local', 'source_host': 'server-44', 'path': '/shared/answer.txt'}):
                    self.assertTrue(call('read_text', args)['isError'])
                self.assertTrue(call('read_text', {'host': 'local', 'source_host': 'server-100',
                                                  'path': '/shared/answer.txt'}, signed=False)['isError'])
                forged = json_bytes({'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call',
                    'params': {'name': 'read_text', 'arguments': {'host': 'local',
                        'path': '/shared/answer.txt', 'source_host': 'server-100',
                        'hop_count': 1, 'origin_host': 'qa'}}})
                headers = {'Content-Type': 'application/json',
                           'X-GrepMesh-Filesystem-Acl': json_bytes(policy).decode(),
                           'X-GrepMesh-Acl-Signature': '0' * 64}
                with urlopen(Request(f'http://127.0.0.1:{port}/mcp', data=forged, headers=headers),
                             timeout=5) as response:
                    self.assertTrue(json.load(response)['result']['isError'])
                denied_policy = {**policy, 'read_roots': ['/elsewhere']}
                self.assertTrue(call('read_text', {'host': 'local', 'source_host': 'server-100',
                                                  'path': '/shared/answer.txt'}, policy=denied_policy)['isError'])
            finally:
                process.terminate()
                process.join(10)


if __name__ == '__main__':
    unittest.main()
