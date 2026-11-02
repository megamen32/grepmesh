#!/usr/bin/env python3
"""Repair ONLY an incomplete sherpa-onnx native extraction in the CI runner.

sherpa-onnx-sys 1.13.7 trusts a cached lib/ directory without verifying
libraries. A partial rust-cache restore can leave that directory present but
remove the static archives, making otherwise correct Rust builds fail to link.
The crate re-extracts the upstream archive when this specific directory is
absent. Never touch the downloaded archive or unrelated Cargo caches.
"""
import argparse
import re
import shutil
from pathlib import Path

STEMS = {
    "linux-x86_64": "linux-x64-static-lib",
    "macos-aarch64": "osx-arm64-static-lib",
    "windows-x86_64": "win-x64-static-MT-Release-lib",
}
# These are the native libraries sherpa-onnx-sys 1.13.7 explicitly links.
REQUIRED = (
    "sherpa-onnx-c-api", "sherpa-onnx-core", "kaldi-decoder-core",
    "sherpa-onnx-kaldifst-core", "sherpa-onnx-fstfar", "sherpa-onnx-fst",
    "kaldi-native-fbank-core", "kissfft-float", "piper_phonemize",
    "espeak-ng", "ucd", "onnxruntime", "ssentencepiece_core",
)


def locked_version(repo: Path) -> str:
    lock = (repo / "Cargo.lock").read_text(encoding="utf-8")
    match = re.search(
        r'(?m)^\[\[package\]\]\s*\nname = "sherpa-onnx-sys"\s*\nversion = "([^"\n]+)"',
        lock,
    )
    if match is None:
        raise ValueError("Could not determine locked sherpa-onnx-sys version")
    return match.group(1)


def library_missing(directory: Path) -> list[str]:
    names = {p.name.lower() for p in directory.iterdir() if p.is_file()}
    return [
        item for item in REQUIRED
        if not any(f"{prefix}{item}{extension}" in names for prefix in ("", "lib")
                   for extension in (".a", ".lib"))
    ]


def repair(repo: Path, target: str) -> str:
    version = locked_version(repo)
    cache_root = repo / "target" / "sherpa-onnx-prebuilt"
    extracted = cache_root / f"sherpa-onnx-v{version}-{STEMS[target]}"
    lib = extracted / "lib"
    if extracted.is_symlink() or lib.is_symlink():
        raise ValueError("Refuse symlinked sherpa-onnx extraction")
    if not lib.is_dir():
        return "fresh: no pre-existing native lib directory"
    missing = library_missing(lib)
    if not missing:
        return "valid: all locked native static libraries present"
    # Delete ONLY the exact, incomplete extraction. The cached .tar.bz2 is a
    # sibling and is intentionally kept for the upstream build.rs to re-extract.
    shutil.rmtree(extracted)
    return "repaired incomplete extracted native library cache: " + ", ".join(missing)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True, choices=sorted(STEMS))
    parser.add_argument("--repo", type=Path, default=Path("."))
    args = parser.parse_args()
    print(repair(args.repo.resolve(), args.target))


if __name__ == "__main__":
    main()
