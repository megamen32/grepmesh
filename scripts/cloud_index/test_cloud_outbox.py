import json
from pathlib import Path
import tempfile
import unittest

from cloud_outbox import Outbox


class OutboxTests(unittest.TestCase):
    def test_reopen_acknowledgement_and_object_delta(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            staging = root / 'staging'
            (staging / 'objects').mkdir(parents=True)
            (staging / 'objects/a').write_bytes(b'unchanged object')
            box = Outbox(root / 'queue', 'test-host')
            box.prepare(staging, [], {'generation': 'one'})
            reopened = Outbox(root / 'queue', 'test-host')
            reopened.verify(reopened.load())
            self.assertTrue(reopened.pending.exists())
            with self.assertRaises(ValueError):
                reopened.acknowledge('wrong-generation')
            self.assertTrue(reopened.pending.exists())
            reopened.acknowledge('one')
            (staging / 'objects/b').write_bytes(b'new object')
            packet = reopened.prepare(staging, [], {'generation': 'two'})
            self.assertEqual(set(packet['changed']), {'objects/b'})
            self.assertFalse((reopened.pending / 'payload/objects/a').exists())

    def test_capacity_failure_does_not_admit_partial_packet(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            staging = root / 'staging'
            staging.mkdir()
            (staging / 'object').write_bytes(b'payload')
            box = Outbox(root / 'queue', 'test-host', max_bytes=1)
            with self.assertRaisesRegex(RuntimeError, 'capacity'):
                box.prepare(staging, [], {'generation': 'one'})
            self.assertIsNone(box.load())
            self.assertFalse((box.root / 'preparing').exists())


if __name__ == '__main__':
    unittest.main()
