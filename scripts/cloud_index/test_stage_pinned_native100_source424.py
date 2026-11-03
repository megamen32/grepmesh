"""No-network, tmpfs-only tests of the explicit part424 one-shot stage."""
import gzip
import hashlib
import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import publish_native100_rolling_private as native
import stage_pinned_native100_source424 as stage

class StagePart424Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir="/run/user/1000",prefix="grepmesh-part424-stage-test-")
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.body=gzip.compress(b"hello immutable source\\n",mtime=0)
        self.hash=hashlib.sha256(self.body).hexdigest()
        self.key=native.PREFIX+"/parts/000424-"+self.hash+".jsonl.gz"
        self.part={"key":self.key,"sha256":self.hash,"compressed_bytes":len(self.body)}
        self.expected_patches=[patch.object(stage,"EXPECTED_SHA",self.hash),
            patch.object(stage,"EXPECTED_BYTES",len(self.body))]
        for p in self.expected_patches:
            p.start()
            self.addCleanup(p.stop)
        for p in (patch.object(native,"ROOT",self.root),
                  patch.object(stage.native,"ROOT",self.root)):
            p.start()
            self.addCleanup(p.stop)
        self.store=self.fake_store()

    def fake_store(self):
        class Fake:
            puts=0
            def __init__(t): t.read_calls=0
            def _pinned_source_hash(t,key,size):
                if (key,size)!=(self.key,len(self.body)): raise ValueError("wrong pinned")
                return self.hash
            def _read_verified_source_cache(t,key,size,sha):
                cache=self.root/"verified-source-cache"/key.rsplit("/",1)[-1]
                if not cache.exists():return None
                data=cache.read_bytes()
                if len(data)!=size or hashlib.sha256(data).hexdigest()!=sha:
                    raise ValueError("wrong cached SHA")
                return data
            def _slow_source_part_get(t,key,size):
                t.read_calls+=1
                return self.body
        return Fake()

    def test_stage_and_reuse_without_second_read(self):
        with patch.object(native,"ensure_tmpfs",return_value=None):
            self.assertEqual(stage.stage_verified_part(self.store,self.part,target_root=self.root),"new_verified")
            self.assertEqual(stage.stage_verified_part(self.store,self.part,target_root=self.root),"existing_verified")
        self.assertEqual(self.store.read_calls,1)
        cache=self.root/"verified-source-cache"/self.key.rsplit("/",1)[-1]
        self.assertEqual(cache.read_bytes(),self.body)
        self.assertEqual(cache.stat().st_mode & 0o777,0o600)

    def test_wrong_manifest_is_rejected_before_read(self):
        for updated in ({"compressed_bytes":len(self.body)+1},{"sha256":"0"*64},
                        {"key":self.key.replace("000424","000425")}):
            with self.subTest(updated=updated):
                with self.assertRaises(ValueError):
                    stage.stage_verified_part(self.store,dict(self.part,**updated),target_root=self.root)
        self.assertEqual(self.store.read_calls,0)

    def test_corrupt_bytes_cannot_create_cache(self):
        self.store._slow_source_part_get=lambda *a: b"x"+self.body[1:]
        with self.assertRaisesRegex(ValueError,"full SHA256 mismatch"):
            stage.stage_verified_part(self.store,self.part,target_root=self.root)
        self.assertFalse((self.root/"verified-source-cache").exists())

    def test_gzip_truncation_fails(self):
        with self.assertRaises((EOFError, OSError)):
            stage.verify_gzip_eof(self.body[:-5])

    def test_decompression_budget_enforced(self):
        with self.assertRaisesRegex(ValueError,"safety bound"):
            stage.verify_gzip_eof(self.body,max_plain_bytes=2)

    def test_existing_corrupt_cache_must_not_trigger_cloud_retry(self):
        c=self.root/"verified-source-cache"
        c.mkdir(mode=0o700)
        (c/self.key.rsplit("/",1)[-1]).write_bytes(b"invalid")
        with self.assertRaisesRegex(ValueError,"wrong cached SHA"):
            stage.stage_verified_part(self.store,self.part,target_root=self.root)
        self.assertEqual(self.store.read_calls,0)

    def test_builder_active_fails_closed(self):
        completed=subprocess.CompletedProcess([],0,"ActiveState=active\nMainPID=123\n","")
        with patch("subprocess.run",return_value=completed):
            with self.assertRaisesRegex(RuntimeError,"builder is active"):
                stage.assert_builder_inactive()

    def test_builder_failure_explicit(self):
        completed=subprocess.CompletedProcess([],0,"ActiveState=failed\nMainPID=0\n","")
        with patch("subprocess.run",return_value=completed):
            stage.assert_builder_inactive()

    def test_stage_requires_apply_and_explicit_opt_in(self):
        with patch("sys.argv",["stage_pinned_native100_source424.py"]):
            with patch.dict(os.environ,{"GREPMESH_OWNER_APPROVED_PART424_FETCH":"0"}):
                with self.assertRaises(SystemExit):
                    stage.main()

if __name__=="__main__":unittest.main()
