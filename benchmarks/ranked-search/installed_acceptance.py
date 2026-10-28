#!/usr/bin/env python3
"""Read-only installed-root reachability and positive-manifest audit.

This checks source access/freshness, not comparative ranking quality.
"""
import datetime
import collections
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'scripts'))
from grepmesh_stdio import Bridge

QUERIES = {
    'serversadmin': 'reverse SSH 22220 22221',
    'herder': 'autopilot', 'broker': 'account', 'telegram': 'Telegram',
    'tgc': 'comment', 'atv': 'views', 'commentorch': 'generate',
    'videowatching': 'video', 'exmanager': 'Todo', 'airlock': 'MCP',
    'airlockadmin': 'tenant', 'userio': 'message', 'autogram': 'client',
    'httpx-reference': 'HTTPTransport retries',
}

def main():
    bridge = Bridge('http://127.0.0.1:19431/mcp', timeout=60)
    counter = 0
    def call(name, arguments):
        nonlocal counter
        counter += 1
        wire = bridge.forward({'jsonrpc': '2.0', 'id': counter, 'method': 'tools/call',
                               'params': {'name': name, 'arguments': arguments}})
        result = wire['result']
        if result.get('isError'):
            raise RuntimeError('installed tool returned an error: ' + name)
        return json.loads(next(c['text'] for c in result['content'] if c['type'] == 'text'))
    status = call('search_status', {})['local']
    assert status['index_state'] == 'Ready', status['index_state']
    rows = []
    for root, query in QUERIES.items():
        data = call('ranked_search', {'query': query, 'roots': [root], 'hosts': 'local',
                                      'limit': 2, 'context_lines': 1})
        hits = data['results']
        assert hits and all(h['freshness'] == 'source_verified' for h in hits), root
        rows.append({'root': root, 'query': query, 'partial': data['partial'],
                     'truncated': data['truncated'], 'warnings': data['warnings'],
                     'coverage': data['coverage'], 'hits': [
                         {k: h[k] for k in ('path', 'start_line', 'end_line', 'freshness', 'body_sha256')}
                         for h in hits]})
    state = Path.home() / '.local/state/grepmesh-ranked/projects'
    manifest = json.loads((state / 'source-manifest.json').read_text())
    cfg = json.loads((state / 'config.json').read_text())
    db = sqlite3.connect('file:' + cfg['index_path'] + '?mode=ro', uri=True)
    persisted = {r[0] for r in db.execute('select path from grepmesh_documents')}
    admitted = set(manifest['files'])
    missing = admitted - persisted
    missing_zero_bytes = sum(Path(p).exists() and Path(p).stat().st_size == 0 for p in missing)
    outside = persisted - admitted
    assert not outside, 'persisted paths outside positive manifest'
    pid = int(subprocess.check_output(['systemctl', '--user', 'show', 'grepmesh-ranked.service',
                                      '-p', 'MainPID', '--value'], text=True).strip())
    hasher = hashlib.sha256()
    with open('/proc/' + str(pid) + '/exe', 'rb') as executable:
        for chunk in iter(lambda: executable.read(1024 * 1024), b''):
            hasher.update(chunk)
    digest = hasher.hexdigest()
    report = {'time_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'endpoint': 'http://127.0.0.1:19431/mcp', 'pid': pid, 'binary_sha256': digest,
              'index_state': status['index_state'], 'roots': rows,
              'manifest_admitted': len(admitted), 'persisted_unique_paths': len(persisted),
              'outside_manifest': len(outside), 'admitted_missing': len(missing),
              'missing_zero_bytes': missing_zero_bytes,
              'missing_nonempty_or_gone': len(missing) - missing_zero_bytes,
              'missing_extensions': dict(collections.Counter(Path(p).suffix for p in missing)),
              'missing_paths': sorted(missing),
              'scope': 'reachability and source verification; not a ranking-quality test'}
    destination = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).with_name('installed-acceptance.json')
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({k: v for k, v in report.items() if k != 'roots'}))
    print('root_canaries=' + str(len(rows)) + ' receipt=' + str(destination))

if __name__ == '__main__':
    main()
