import ast
import gzip
import hashlib
import inspect
import os
from pathlib import Path
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch
import publish_native100_rolling_private as target
from cloud_index import Publisher, MemoryStore


class PathMetadataSpoolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='grepmesh-path-spool-test-', dir='/run/user/1000')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.spools = []
        self.addCleanup(self.close_all)

    def close_all(self):
        for p in self.spools:
            p.close()

    def spool(self):
        p = target.PathMetadataSpool(len(self.spools), self.root)
        self.spools.append(p)
        return p

    @staticmethod
    def row(path='/home/test/one.txt'):
        return (path, 0, 0, hashlib.sha256(path.encode()).hexdigest())

    def test_order_unicode_and_count(self):
        p = self.spool()
        rows = [self.row('/home/тест/два.txt'), self.row('/home/a.txt')]
        for r in rows: p.append(r)
        self.assertEqual(len(p), 2)
        self.assertEqual(list(p), rows)
        self.assertEqual(list(p), rows)
        self.assertEqual(p.path.stat().st_mode & 0o777, 0o600)

    def test_empty_spool_is_valid(self):
        p = self.spool()
        self.assertEqual(list(p), [])
        self.assertEqual(len(p), 0)

    def test_append_after_seal_refused(self):
        p = self.spool(); p.close()
        with self.assertRaises(RuntimeError): p.append(self.row())

    def test_malformed_row_refused(self):
        p = self.spool()
        for r in [None, [], ('relative',0,0,'a'*64), ('/home/x',True,0,'a'*64),
                  ('/home/x',0,-1,'a'*64), ('/home/x',0,0,'bad'), ('/home/../x',0,0,'a'*64)]:
            with self.subTest(row=r), self.assertRaises(ValueError): p.append(r)
        self.assertEqual(len(p), 0)

    def test_frame_bound(self):
        p = self.spool()
        with self.assertRaises(ValueError): p.append(self.row('/home/'+'x'*70000))
        self.assertEqual(len(p), 0)

    def test_valid_gzip_replacement_fails_logical_hash(self):
        p = self.spool(); p.append(self.row()); p.close()
        data = target.json_bytes(self.row('/home/other.txt')) + b'\n'
        p.path.write_bytes(gzip.compress(data))
        with self.assertRaises(RuntimeError): list(p)

    def test_truncated_gzip_refused(self):
        p = self.spool(); p.append(self.row()); p.close()
        p.path.write_bytes(p.path.read_bytes()[:-5])
        with self.assertRaises((EOFError, OSError)): list(p)

    def test_fifo_refused_nonblocking(self):
        p = self.spool(); p.close(); p.path.unlink(); os.mkfifo(p.path, 0o600)
        with self.assertRaises(RuntimeError): list(p)

    def test_symlink_refused(self):
        p = self.spool(); p.close(); saved = p.path.with_suffix('.saved'); p.path.rename(saved)
        p.path.symlink_to(saved)
        with self.assertRaises(OSError): list(p)

    def test_hardlink_refused(self):
        p = self.spool(); p.close(); os.link(p.path, self.root/'linked.gz')
        with self.assertRaises(RuntimeError): list(p)

    def test_public_mode_refused(self):
        p = self.spool(); p.close(); p.path.chmod(0o644)
        with self.assertRaises(RuntimeError): list(p)

    def test_symlink_root_refused(self):
        link = self.root/'link'; link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(RuntimeError): target.PathMetadataSpool(0, link)

    def test_path_index_bytes_match_list_input(self):
        rows = [self.row('/home/one.txt'), self.row('/home/Два.txt'), self.row('/opt/a.py')]
        p = self.spool()
        for r in rows: p.append(r)
        a=MemoryStore(); b=MemoryStore()
        da=Publisher(a,self.root).path_index(rows)
        db=Publisher(b,self.root).path_index(p)
        self.assertEqual(da, db)
        self.assertEqual(a.objects, b.objects)

    def test_large_spool_does_not_retain_path_objects(self):
        p = self.spool()
        tracemalloc.start()
        try:
            for i in range(20000): p.append(self.row('/home/source/project/path-%06d.txt' % i))
            current, peak = tracemalloc.get_traced_memory()
            self.assertLess(current, 2*1024*1024)
            self.assertLess(peak, 4*1024*1024)
        finally: tracemalloc.stop()
        self.assertEqual(len(p), 20000)
        self.assertEqual(sum(1 for _ in p), 20000)

    def test_partition_uses_spool_not_list(self):
        with patch.object(target, 'ROOT', self.root): part=target.Partition(0,None)
        self.spools.append(part.paths)
        self.assertIsInstance(part.paths, target.PathMetadataSpool)
        part.add('/home/one.txt','small body')
        self.assertEqual(len(part.paths),1)

    def test_sqlite_binding_stream_is_generator(self):
        import textwrap
        tree=ast.parse(textwrap.dedent(inspect.getsource(Publisher.path_index)))
        call=next(n for n in ast.walk(tree) if isinstance(n,ast.Call)
                  and isinstance(n.func,ast.Attribute) and n.func.attr=='executemany')
        self.assertIsInstance(call.args[1],ast.GeneratorExp)

if __name__ == '__main__': unittest.main()
