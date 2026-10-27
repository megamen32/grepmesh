#!/usr/bin/env python3
"""Find a verified rollback receipt for an already-installed source stage."""

from __future__ import annotations

import argparse
from pathlib import Path


def find_matching_source_receipt(
    receipt_root: str | Path,
    binary_sha256: str,
    config_sha256: str,
) -> Path | None:
    root = Path(receipt_root)
    if not root.is_dir():
        return None
    for receipt in sorted((path for path in root.iterdir() if path.is_dir()), reverse=True):
        manifest_path = receipt / "manifest.txt"
        old_binary = receipt / "old/grepmesh-mcp"
        old_config = receipt / "old/config.json"
        if not manifest_path.is_file() or not old_binary.is_file() or not old_config.is_file():
            continue
        fields = {}
        try:
            for line in manifest_path.read_text(encoding="utf-8").splitlines():
                if "=" in line:
                    key, value = line.split("=", 1)
                    fields[key] = value
        except OSError:
            continue
        if (
            fields.get("new_binary_sha256") == binary_sha256
            and fields.get("installed_binary_sha256") == binary_sha256
            and fields.get("new_config_sha256") == config_sha256
            and fields.get("installed_config_sha256") == config_sha256
            and fields.get("health_check", "").startswith("PASS ")
        ):
            return receipt
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt-root", type=Path, required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--config-sha256", required=True)
    args = parser.parse_args()
    receipt = find_matching_source_receipt(
        args.receipt_root, args.binary_sha256, args.config_sha256
    )
    if receipt is not None:
        print(receipt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
