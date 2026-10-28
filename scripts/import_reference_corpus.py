#!/usr/bin/env python3
"""Import five pinned public HTTPX files into an explicit reference-state directory.

No clone, credentials, redirects, execution, broad GitHub crawling or fallback.
Does not edit/start GrepMesh. Run only when the operator selects this corpus.
"""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import tempfile
import time

REPOSITORY = "encode/httpx"
COMMIT = "b5addb64f0161ff6bfe94c124ef76f6a1fba5254"
FILES = ("LICENSE.md", "httpx/_transports/default.py", "httpx/_config.py", "docs/advanced/transports.md", "README.md")
MAX_FILE = 1024 * 1024


def fetch(relative):
    connection = http.client.HTTPSConnection("raw.githubusercontent.com", timeout=20)
    try:
        connection.request("GET", "/{}/{}/{}".format(REPOSITORY, COMMIT, relative),
                           headers={"Accept": "text/plain", "User-Agent": "GrepMesh-pinned-reference-import"})
        response = connection.getresponse()
        if response.status != 200:
            raise ValueError("pinned public file unavailable: {} HTTP {}".format(relative, response.status))
        data = response.read(MAX_FILE + 1)
        if len(data) > MAX_FILE or b"\0" in data:
            raise ValueError("reference file exceeds plain-text byte limit")
        data.decode("utf-8")
        return data
    finally:
        connection.close()


def atomic_write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o444)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True, help="dedicated absolute reference-state directory, not a source checkout")
    args = parser.parse_args()
    destination = args.state_dir
    if (not destination.is_absolute() or destination.resolve() != destination
            or destination == Path.home() or destination == Path("/")):
        parser.error("state-dir must be an explicit absolute dedicated directory without symlinks")
    if destination.exists() and any(destination.iterdir()):
        parser.error("state-dir must be empty; existing imports are never overwritten")
    # Fetch all five bounded files before publishing any local corpus.
    documents = {relative: fetch(relative) for relative in FILES}
    license_text = documents["LICENSE.md"].decode("utf-8")
    if "Redistribution and use in source and binary forms" not in license_text or "Neither the name" not in license_text:
        raise ValueError("pinned license did not match expected BSD-3-Clause conditions; import refused")
    manifest = {"version": 1, "corpus": "httpx-reference", "repository": "https://github.com/" + REPOSITORY,
                "commit": COMMIT, "license": "BSD-3-Clause", "license_file": "LICENSE.md",
                "imported_at_unix": int(time.time()), "scope": "five selected pinned files; not all HTTPX or GitHub",
                "files": [{"path": relative, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                           "url": "https://raw.githubusercontent.com/{}/{}/{}".format(REPOSITORY, COMMIT, relative)}
                          for relative, data in documents.items()]}
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    for relative, data in documents.items():
        atomic_write(destination / relative, data)
    atomic_write(destination / "corpus-manifest.json", (json.dumps(manifest, indent=2) + "\n").encode())
    print(json.dumps({"corpus": "httpx-reference", "commit": COMMIT, "files": len(documents),
                      "manifest": str(destination / "corpus-manifest.json")}))


if __name__ == "__main__":
    main()
