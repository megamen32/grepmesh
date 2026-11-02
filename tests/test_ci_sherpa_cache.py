"""Narrow tests for CI-only sherpa-onnx incomplete cache repair."""
import sys
import tempfile
import unittest
from pathlib import Path
import importlib.util

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "ci-repair-sherpa-cache.py"
spec = importlib.util.spec_from_file_location("ci_repair_sherpa_cache", SCRIPT)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


class RepairTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "Cargo.lock").write_text(
            '[[package]]\nname = "sherpa-onnx-sys"\nversion = "1.13.7"\n',
            encoding="utf-8",
        )

    def libdir(self, target):
        stem = mod.STEMS[target]
        return self.root / "target" / "sherpa-onnx-prebuilt" / f"sherpa-onnx-v1.13.7-{stem}" / "lib"

    def test_missing_native_cache_not_created(self):
        self.assertIn("fresh:", mod.repair(self.root, "linux-x86_64"))
        self.assertFalse((self.root / "target").exists())

    def test_valid_linux_cache_preserved(self):
        lib = self.libdir("linux-x86_64")
        lib.mkdir(parents=True)
        for name in mod.REQUIRED:
            (lib / f"lib{name}.a").write_bytes(b"not-a-real-library")
        self.assertIn("valid:", mod.repair(self.root, "linux-x86_64"))
        self.assertTrue((lib / "libsherpa-onnx-c-api.a").exists())

    def test_partial_cache_removed_but_archive_and_neighbor_preserved(self):
        lib = self.libdir("linux-x86_64")
        lib.mkdir(parents=True)
        (lib / "libonnxruntime.a").write_bytes(b"")
        archive = lib.parents[1] / (lib.parent.name + ".tar.bz2")
        archive.write_bytes(b"fake pre-existing cached archive")
        sibling = lib.parents[1] / "other-native-archive"
        sibling.mkdir()
        self.assertIn("repaired", mod.repair(self.root, "linux-x86_64"))
        self.assertFalse(lib.parent.exists())
        self.assertTrue(archive.exists())
        self.assertTrue(sibling.exists())

    def test_valid_windows_lib_names_preserved(self):
        lib = self.libdir("windows-x86_64")
        lib.mkdir(parents=True)
        for name in mod.REQUIRED:
            (lib / f"{name}.lib").write_bytes(b"not-a-real-library")
        self.assertIn("valid:", mod.repair(self.root, "windows-x86_64"))

    def test_partial_macos_arm64_cache_removed(self):
        lib = self.libdir("macos-aarch64")
        lib.mkdir(parents=True)
        (lib / "libsherpa-onnx-core.a").write_bytes(b"")
        self.assertIn("sherpa-onnx-c-api", mod.repair(self.root, "macos-aarch64"))
        self.assertFalse(lib.parent.exists())


if __name__ == "__main__":
    unittest.main()
