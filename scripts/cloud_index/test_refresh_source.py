"""Local format checks only; real cloud acceptance is run separately."""
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from cloud_index import document, json_bytes
from refresh_source import partition, build, StagingStore


def stream(records, complete=True):
    items = [{'kind': 'begin', 'format': 1, 'source_host': 'test-host',
              'source_root': '/sources', 'policy': 'test source'}]
    h = hashlib.sha256()
    for name, text in records:
        d = {'kind': 'document', 'path': '/sources/' + name, 'body': text,
             'size': len(text.encode()), 'mtime_ns': 0,
             'sha256': hashlib.sha256(text.encode()).hexdigest()}
        items.append(d)
        h.update(json_bytes([d['path'], d['sha256']]))
    items.append({'kind': 'end', 'count': len(records),
                  'logical_sha256': h.hexdigest(),
                  'complete_for_policy': complete, 'skipped': {}})
    return io.StringIO(''.join(json.dumps(x) + '\n' for x in items))


class RefreshTests(unittest.TestCase):
    def test_incomplete_scan_rejected(self):
        with tempfile.TemporaryDirectory(dir='/dev/shm') as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(ValueError, 'Incomplete'):
                partition(stream([('a', 'body')], False), root,
                          'test-host', '/sources', 1, 0)
            self.assertFalse((root / 'upload').exists())

    def test_oversized_and_empty_generation(self):
        with tempfile.TemporaryDirectory(dir='/dev/shm') as tmp:
            root = Path(tmp)
            receipt = partition(stream([('a', 'small'), ('b', 'x' * (1024**2 + 1))]),
                                root, 'test-host', '/sources', 2, 0)
            upload = root / 'upload'
            upload.mkdir()
            entries = build(StagingStore(upload, 0), root, receipt,
                            'test-host', '/sources', 2)
            self.assertEqual(receipt['oversized'], 1)
            self.assertEqual(sum(receipt['counts']), 1)
            self.assertIn('oversize', entries[0])
            self.assertEqual(len(list(upload.glob('scopes/*/latest.json'))), 2)
        with tempfile.TemporaryDirectory(dir='/dev/shm') as tmp:
            root = Path(tmp)
            empty = partition(stream([]), root, 'test-host', '/sources', 1, 0)
            self.assertEqual(empty['documents'], 0)
            self.assertNotEqual(empty['generation'], receipt['generation'])

    def test_changes_and_deletions_change_generation(self):
        generations = []
        for docs in [[('a', 'old'), ('b', 'removed')], [('a', 'new')], []]:
            with tempfile.TemporaryDirectory(dir='/dev/shm') as tmp:
                r = partition(stream(docs), Path(tmp), 'test-host', '/sources', 1, 0)
                generations.append(r['generation'])
        self.assertEqual(len(set(generations)), 3)


if __name__ == '__main__':
    unittest.main()
