#!/usr/bin/env python3
"""Create a GrepMesh platform archive from native grepmesh-mcp and rg binaries."""

from __future__ import annotations

import argparse
import shutil
import tarfile
import tempfile
import zipfile
from pathlib import Path


TARGETS = {
    "linux-x86_64": ("grepmesh-linux-x86_64.tar.gz", "tar.gz", "grepmesh-user.service"),
    "macos-x86_64": ("grepmesh-macos-x86_64.tar.gz", "tar.gz", "grepmesh-user.plist"),
    "macos-aarch64": ("grepmesh-macos-aarch64.tar.gz", "tar.gz", "grepmesh-user.plist"),
    "windows-x86_64": ("grepmesh-windows-x86_64.zip", "zip", None),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=TARGETS, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--rg", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    archive_name, archive_type, service_template = TARGETS[args.target]
    repo_root = Path(__file__).resolve().parent.parent
    binary_name = "grepmesh-mcp.exe" if args.target.startswith("windows-") else "grepmesh-mcp"
    rg_name = "rg.exe" if args.target.startswith("windows-") else "rg"

    for path in (args.binary, args.rg):
        if not path.is_file():
            raise SystemExit(f"required binary is missing: {path}")

    args.output.mkdir(parents=True, exist_ok=True)
    archive_path = args.output / archive_name
    with tempfile.TemporaryDirectory(prefix="grepmesh-package-") as temp_dir:
        stage = Path(temp_dir)
        shutil.copy2(args.binary, stage / binary_name)
        shutil.copy2(args.rg, stage / rg_name)
        shutil.copy2(repo_root / "config.example.json", stage / "config.example.json")
        if service_template:
            shutil.copy2(repo_root / service_template, stage / service_template)

        if archive_type == "tar.gz":
            with tarfile.open(archive_path, "w:gz") as archive:
                for path in sorted(stage.iterdir()):
                    archive.add(path, arcname=path.name)
        else:
            with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                for path in sorted(stage.iterdir()):
                    archive.write(path, arcname=path.name)

    print(archive_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
