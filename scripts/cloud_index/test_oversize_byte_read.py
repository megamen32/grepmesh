import base64
import hashlib
import unittest

from cloud_index import MAX_DOCUMENT, MemoryStore
from oversize_support import OversizeIndex, publish_oversize


class OversizeByteReadTest(unittest.TestCase):
    def setUp(self):
        self.body = ('a' * (MAX_DOCUMENT - 1) + 'я' * 20 + '\nend').encode()
        self.path = '/allowed/large.txt'
        store = self.store = MemoryStore()
        publish_oversize(store, 'test-host', 'test-snapshot',
                         [{'path': self.path, 'body': self.body.decode()}],
                         chunk_bytes=128 * 1024)
        self.reader = OversizeIndex(store, 'test-host', 'test-snapshot')

    def test_reconstruct_and_pin(self):
        expected = hashlib.sha256(self.body).hexdigest()
        offset = 0
        out = bytearray()
        while offset < len(self.body):
            result = self.reader.read_text(self.path, authorized_roots=['/allowed'],
                byte_offset=offset, byte_limit=100003,
                expected_body_sha256=expected if offset else None)
            part = base64.b64decode(result['data_base64'], validate=True)
            self.assertEqual(hashlib.sha256(part).hexdigest(), result['part_sha256'])
            self.assertEqual(result['byte_offset'], offset)
            self.assertGreater(result['next_byte_offset'], offset)
            out.extend(part)
            offset = result['next_byte_offset']
        self.assertTrue(result['eof'])
        self.assertEqual(bytes(out), self.body)
        self.assertEqual(hashlib.sha256(out).hexdigest(), expected)

    def test_fail_closed(self):
        with self.assertRaises(PermissionError):
            self.reader.read_text(self.path, authorized_roots=['/elsewhere'], byte_offset=0)
        for kwargs in ({'byte_offset': 1}, {'byte_offset': 1, 'expected_body_sha256': '0' * 64},
                       {'byte_offset': True}, {'byte_offset': 0, 'byte_limit': 262145},
                       {'byte_offset': 0, 'end_line': 2}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.reader.read_text(self.path, authorized_roots=['/allowed'], **kwargs)

    def test_corrupt_chunk_is_rejected(self):
        self.reader.refresh()
        key = self.reader.by_path[self.path]['chunks'][0]['key']
        self.store.objects[key] = b'corrupt'
        with self.assertRaisesRegex(RuntimeError, 'integrity'):
            self.reader.read_text(self.path, authorized_roots=['/allowed'], byte_offset=0)


if __name__ == '__main__':
    unittest.main()
