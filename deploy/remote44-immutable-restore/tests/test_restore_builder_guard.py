import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import restore_builder_guard as guard

class GuardRegression(unittest.TestCase):
    def verify_fixture(self, files, expected, reader_active=True):
        source = guard.REMOTE_CHECK
        source = source.replace("p=subprocess.run(['/usr/bin/systemctl','--user','is-active','--quiet','grepmesh-remote44-private-reader.service'],env=env,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=6)", "p=type('Result', (), {'returncode': " + ("0" if reader_active else "3") + "})()")
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "store"
            if files is not None:
                root.mkdir()
            else:
                files = {}
            for name, content in files.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            proc = subprocess.run([sys.executable, "-c", source, str(root)], input=json.dumps(expected).encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False)
            return proc.returncode, proc.stdout.strip()

    def test_exact_set_passes(self):
        files = {f"objects/{i:03}.blob": (f"payload-{i}".encode()) for i in range(165)}
        expected = [[name, len(data), hashlib.sha256(data).hexdigest()] for name, data in files.items()]
        rc, out = self.verify_fixture(files, expected)
        self.assertEqual((rc, out), (0, b"EXACT_AND_READER_ACTIVE"))

    def test_165_wrong_names_and_content_fail(self):
        files = {f"wrong/{i:03}.blob": b"wrong" for i in range(165)}
        expected = [[f"objects/{i:03}.blob", 5, hashlib.sha256(b"right").hexdigest()] for i in range(165)]
        rc, _ = self.verify_fixture(files, expected)
        self.assertNotEqual(rc, 0)

    def test_inactive_reader_fails_closed(self):
        data=b"valid"; files={"x":data}; expected=[["x",len(data),hashlib.sha256(data).hexdigest()]]
        rc, _ = self.verify_fixture(files, expected, reader_active=False)
        self.assertEqual(rc, 10)

    def test_missing_store_requests_restore(self):
        rc, out = self.verify_fixture(None, [["x", 1, hashlib.sha256(b"x").hexdigest()]])
        self.assertEqual((rc, out), (10, b"NEEDS_RESTORE"))

    def test_partial_store_requests_restore(self):
        expected = [["x", 1, hashlib.sha256(b"x").hexdigest()], ["y", 1, hashlib.sha256(b"y").hexdigest()]]
        rc, out = self.verify_fixture({"x": b"x"}, expected)
        self.assertEqual((rc, out), (10, b"NEEDS_RESTORE"))

    def test_ssh_failure_or_timeout_never_skips_restore(self):
        for failure in (subprocess.TimeoutExpired("ssh", 1800), OSError("ssh unavailable")):
            with self.subTest(failure=type(failure).__name__), patch.object(guard.os, "getuid", return_value=1000), patch.object(guard, "state", return_value="inactive"), patch.object(guard, "CloudStore"), patch.object(guard, "build_catalog", return_value=(1,1,[("x",(1,"0"*64,None,b"x"))])), patch.object(guard.subprocess, "run", side_effect=failure):
                self.assertEqual(guard.main(), 255)

    def test_only_explicit_missing_marker_starts_restore(self):
        for rc, output, expected in ((10, b"NEEDS_RESTORE\n", 0), (10, b"", 255), (255, b"NEEDS_RESTORE\n", 255), (0, b"EXACT_AND_READER_ACTIVE\n", 1)):
            with self.subTest(rc=rc, output=output), patch.object(guard.os, "getuid", return_value=1000), patch.object(guard, "state", return_value="inactive"), patch.object(guard, "CloudStore"), patch.object(guard, "build_catalog", return_value=(1,1,[("x",(1,"0"*64,None,b"x"))])), patch.object(guard.Path, "read_text", return_value="{}"), patch.object(guard.subprocess, "run", return_value=subprocess.CompletedProcess([], rc, stdout=output, stderr=b"")):
                self.assertEqual(guard.main(), expected)

    def test_catalog_read_does_not_require_volatile_cache(self):
        with patch.object(guard.os, "getuid", return_value=1000), patch.object(guard, "state", return_value="inactive"), patch.object(guard, "CloudStore"), patch.object(guard, "build_catalog", return_value=(1,1,[("x",(1,"0"*64,None,b"x"))])) as catalog, patch.object(guard.Path, "read_text", return_value="{}"), patch.object(guard.subprocess, "run", return_value=subprocess.CompletedProcess([], 10, stdout=b"NEEDS_RESTORE\n", stderr=b"")):
            self.assertEqual(guard.main(), 0)
            self.assertFalse(catalog.call_args.kwargs["use_cache"])

    def test_unexpected_partial_file_is_not_treated_as_ready(self):
        expected = [["x", 1, hashlib.sha256(b"x").hexdigest()]]
        rc, out = self.verify_fixture({"x.partial.123": b"x"}, expected)
        self.assertNotIn((rc, out), ((0, b"EXACT_AND_READER_ACTIVE"), (10, b"NEEDS_RESTORE")))

if __name__ == "__main__": unittest.main()
