"""Synthetic only: verify regex matching on oversized archived paths.

No network, cloud data, credentials, reader process, or profiler tests.
"""
import threading
import time
import unittest
from cloud_index import compile_bounded_regex
from multi_cloud import MultiReader

class SyntheticIndex:
    def __init__(self,path):
        self.lock=threading.RLock()
        self.manifest={'generated_at':'synthetic'}
        self.by_path={path:{'path':path,'size_bytes':1200000,
                            'body_sha256':'0'*64}}
        self.body_reads=0
    def _require(self):
        pass
    def first_match(self,record,deadline,**kwargs):
        self.body_reads+=1
        return None  # Synthetic body contains no matching text.

class OversizedRegexPathTests(unittest.TestCase):
    def setUp(self):
        self.path='/home/synthetic/doc-2026.txt'
        self.index=SyntheticIndex(self.path)
        self.reader=MultiReader.__new__(MultiReader)
        self.reader._oversize_index=lambda entry:self.index
        self.entry={'scope':'synthetic-oversize',
                    'source_host':'server-44','source_root':'/'}
    def query(self,pattern,*,regex=False,glob=False,roots=None,deadline=None):
        compiled=compile_bounded_regex(pattern) if regex else None
        return self.reader._oversize_query_hits(
            self.entry,pattern,3,glob,False,None,
            ['/home/synthetic'] if roots is None else roots,
            time.monotonic()+3 if deadline is None else deadline,
            regex=regex,compiled=compiled)
    def test_regex_matches_path_when_body_does_not(self):
        hits,warnings=self.query(r'doc-[0-9]+\.txt',regex=True)
        self.assertFalse(warnings)
        self.assertEqual(len(hits),1)
        self.assertEqual(hits[0]['path'],self.path)
        self.assertEqual(hits[0]['line_number'],0)
        self.assertEqual(hits[0]['text'],'[path match] '+self.path)
    def test_nonmatching_regex_is_not_literal_fallback(self):
        hits,warnings=self.query(r'else-[0-9]+\.txt',regex=True)
        self.assertEqual(hits,[])
        self.assertEqual(warnings,[])
    def test_verified_root_blocks_before_body_scan(self):
        hits,warnings=self.query(r'doc-[0-9]+\.txt',
                                 regex=True,roots=['/home/another'])
        self.assertEqual((hits,warnings),([],[]))
        self.assertEqual(self.index.body_reads,0)
    def test_literal_path_behavior_preserved(self):
        hits,warnings=self.query('doc-2026')
        self.assertEqual(len(hits),1)
        self.assertEqual(warnings,[])
    def test_glob_path_behavior_preserved(self):
        hits,warnings=self.query('*.txt',glob=True)
        self.assertEqual(len(hits),1)
        self.assertEqual(warnings,[])
        self.assertEqual(self.index.body_reads,0)
    def test_expired_regex_deadline_reports_partial(self):
        hits,warnings=self.query(r'doc-[0-9]+\.txt',regex=True,
                                 deadline=time.monotonic()-1)
        self.assertEqual(hits,[])
        self.assertTrue(any('deadline' in v for v in warnings))

if __name__=='__main__':
    unittest.main()
