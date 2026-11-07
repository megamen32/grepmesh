"""Real filesystem regression for bounded, verified warm-cache restart."""
import hashlib
import pathlib
import tempfile
import unittest
from cloud_index import Reader

class CacheRestart(unittest.TestCase):
    def test_verified_cache_survives_restart(self):
        with tempfile.TemporaryDirectory(dir='/dev/shm') as temp:
            root=pathlib.Path(temp); directory=root/'cache-fixture';directory.mkdir()
            raw=b'a'*4096; ident=hashlib.sha256(raw).hexdigest()
            cached=directory/(ident+'.sqlite');cached.write_bytes(raw)
            bad=directory/('b'*64+'.sqlite');bad.write_bytes(raw)
            link=directory/('c'*64+'.sqlite');link.symlink_to(cached)
            reader=Reader(None,root,'fixture',4096)
            self.assertTrue(cached.exists())
            self.assertEqual(list(reader.cache),[ident])
            self.assertEqual(reader.used,4096)
            self.assertEqual(reader.load({'sha256':ident,'raw_bytes':4096}),cached)
            self.assertNotIn(bad.stem,reader.cache)
            self.assertNotIn(link.stem,reader.cache)
            self.assertLessEqual(reader.used,reader.limit)

if __name__=='__main__':unittest.main()
