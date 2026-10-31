#!/usr/bin/env python3
"""Deterministic tests for update-only cloud publishing, no real network."""
import fcntl
import hashlib
import io
import json
import os
import pathlib
import tempfile

from cloud_index import MemoryStore, Publisher, Reader, document, ensure_tmpfs, digest, json_bytes
from refresh_git import load_scopes, refresh_one, refresh_all, verify_stream

BASE = pathlib.Path('/run/user') / str(os.getuid()) / 'grepmesh-cloud-refresh-tests'
ensure_tmpfs(BASE)


def encoded(records, source, host):
    lines = [{'kind': 'begin', 'format': 1, 'source_host': host,
              'source_root': source, 'policy': 'tracked UTF-8 test policy'}]
    h = hashlib.sha256()
    for p, text in records:
        d = document(source + '/' + p, text)
        lines.append(d)
        h.update(json_bytes([d['path'], d['sha256']]))
    lines.append({'kind': 'end', 'count': len(records), 'logical_sha256': h.hexdigest(),
                  'skipped': {}, 'complete_for_policy': True})
    return io.BytesIO(''.join(json.dumps(x, ensure_ascii=False) + '\n' for x in lines).encode())


def run():
    checks = []
    def passed(name, condition):
        assert condition, name
        checks.append(name)
    with tempfile.TemporaryDirectory(dir=BASE) as tmp:
        root = pathlib.Path(tmp)
        source = '/home/roomhacker/agents-projects/airlock-members-api'
        entry = {'scope': 'cloud-updater-test', 'source_root': source, 'source_host': 'server-100'}
        initial = [('alpha.md', 'old alpha source text'), ('beta.md', 'beta only text')]
        store = MemoryStore()
        Publisher(store, root).publish(entry['scope'], (line.decode() for line in encoded(initial, source, 'server-100')))
        uploads = store.puts
        check = refresh_one(store, root, entry, encoded(initial, source, 'server-100'))
        passed('unchanged corpus is skipped', check['action'] == 'unchanged')
        passed('zero cloud writes on unchanged corpus', store.puts == uploads)
        pointer_before = store.objects['scopes/cloud-updater-test/latest.json']

        updated = [('alpha.md', 'new alpha content'), ('renamed.md', 'replacement renamed text')]
        status = refresh_one(store, root, entry, encoded(updated, source, 'server-100'))
        passed('changed contents published atomically', status['action'] == 'published' and status['documents'] == 2)
        passed('manifest pointer advanced', store.objects['scopes/cloud-updater-test/latest.json'] != pointer_before)
        passed('previous rollback generation retained', any(k.startswith('manifests/cloud-updater-test/') for k in store.objects))

        reader = Reader(store, root, entry['scope'], 64 * 1024 * 1024,
                        entry['source_host'], entry['source_root'])
        reader.refresh()
        passed('new text searchable', len(reader.query('new alpha')['results']) == 1)
        passed('old text removed after regeneration', len(reader.query('old alpha')['results']) == 0)
        passed('renamed file discoverable', len(reader.query('*renamed.md', glob=True)['results']) == 1)

        pointer_current = store.objects['scopes/cloud-updater-test/latest.json']
        old_uploads = store.puts
        broken = encoded([('test.md', 'incomplete footer')], source, 'server-100')
        lines = broken.getvalue().splitlines(keepends=True)
        lines[-1] = lines[-1].replace(b'"complete_for_policy": true', b'"complete_for_policy": false')
        try: refresh_one(store, root, entry, io.BytesIO(b''.join(lines)))
        except (RuntimeError, ValueError): pass
        else: raise AssertionError('Incomplete stream was accepted')
        passed('bad source leaves cloud pointer unchanged', store.objects['scopes/cloud-updater-test/latest.json'] == pointer_current)
        passed('bad source causes zero cloud writes', store.puts == old_uploads)

        try: refresh_one(store, root, entry, encoded([], source, 'server-100'))
        except RuntimeError: pass
        else: raise AssertionError('Empty source destroyed a valid generation')
        passed('empty scan cannot wipe a corpus', store.objects['scopes/cloud-updater-test/latest.json'] == pointer_current)

        many = [(f'doc-{i:02}.md', f'document {i} only') for i in range(12)]
        refresh_one(store, root, entry, encoded(many, source, 'server-100'))
        ptr_many = store.objects['scopes/cloud-updater-test/latest.json']
        try: refresh_one(store, root, entry, encoded(many[:3], source, 'server-100'))
        except RuntimeError: pass
        else: raise AssertionError('Mass-deletion was accepted')
        passed('unreviewed mass source shrink denied', store.objects['scopes/cloud-updater-test/latest.json'] == ptr_many)

        policy = root / 'scopes.json'
        policy.write_text(json.dumps({'version': 1, 'scopes': [entry]}))
        passed('explicit narrow Git scope accepted', len(load_scopes(policy)) == 1)
        policy.write_text(json.dumps({'version': 1, 'scopes': [{**entry, 'source_root': '/home/roomhacker'}]}))
        try: load_scopes(policy)
        except ValueError: pass
        else: raise AssertionError('Broad home folder accepted')
        passed('home-wide source is rejected', True)
        # Shared publisher.lock prevents overlap with manual cloud publishing.
        def synthetic_scan(item, output, max_bytes=64*1024*1024):
            raw = encoded(many, source, 'server-100').getvalue()
            assert len(raw) <= max_bytes
            output.write(raw)
            output.flush()
            output.seek(0)
            return len(raw)

        uploads_before = store.puts
        status = refresh_all(store, root, [entry], scan=synthetic_scan)
        passed('serialized refresh skips unchanged source', status[0]['action'] == 'unchanged')
        passed('serialized unchanged refresh writes nothing', store.puts == uploads_before)

        with open(root / 'publisher.lock', 'a+b') as held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                refresh_all(store, root, [entry], scan=synthetic_scan)
            except RuntimeError as error:
                passed('concurrent cloud publisher refused', 'publisher' in str(error))
            else:
                raise AssertionError('Overlapping publisher was admitted')

        def failing_scan(item, output, max_bytes=64*1024*1024):
            output.write(b'incomplete')
            raise RuntimeError('Simulated source failure')

        try:
            refresh_all(store, root, [entry], scan=failing_scan)
        except RuntimeError:
            passed('failed scan does not publish', store.objects['scopes/cloud-updater-test/latest.json'] == ptr_many)
        else:
            raise AssertionError('Broken source scan was admitted')

        recovered = refresh_all(store, root, [entry], scan=synthetic_scan)
        passed('failed scan releases publishing lock', recovered[0]['action'] == 'unchanged')

    print(json.dumps({'all_passed': True, 'tests': len(checks), 'cases': checks}))


if __name__ == '__main__':
    if hasattr(os, 'sched_setaffinity'):
        os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:2])
    os.nice(10)
    run()
