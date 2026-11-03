#!/usr/bin/env python3
"""ONE-SHOT native100 immutable part424 tmpfs stage, owner-gated, never a builder.

Do not execute --apply before a separately recorded scope-owner authorization.
No cloud write or archive export. Existing builder lock prevents concurrent work.
"""
from __future__ import annotations

import argparse
import fcntl
import gzip
import io
import json
import os
from pathlib import Path
import subprocess
import time

import publish_native100_rolling_private as native

ORDINAL = "000424"
EXPECTED_SHA = "cfe29a793b036ed4abdc00af2e3e1691aaea85abe801bd11445868c28158ae98"
EXPECTED_BYTES = 12086993
EXPECTED_CHECKPOINT_SHA = "96f1736a8cd30e9a9a7427255c995d7562fdc9536ec5272c78db509f519409ce"
UNIT = "grepmesh-native100-private-search-resume-20261101.service"


def verify_gzip_eof(packed: bytes, max_plain_bytes: int = 1024 * native.MIB) -> int:
    """Bound decompression and exercise gzip CRC/trailer without keeping content."""
    count = 0
    with gzip.GzipFile(fileobj=io.BytesIO(packed), mode="rb") as stream:
        while True:
            data = stream.read(256 * 1024)
            if not data:
                break
            count += len(data)
            if count > max_plain_bytes:
                raise ValueError("Pinned part inflated past safety bound")
    return count


def stage_verified_part(store: native.VerifiedImmutableReuseStore,
                        part: dict, *, target_root: Path = native.ROOT) -> str:
    """Atomic no-replace tmpfs staging; caller MUST hold the existing builder lock."""
    key, size, sha = part["key"], part["compressed_bytes"], part["sha256"]
    if (key != native.PREFIX + "/parts/" + ORDINAL + "-" + EXPECTED_SHA + ".jsonl.gz"
            or size != EXPECTED_BYTES or sha != EXPECTED_SHA
            or store._pinned_source_hash(key, size) != sha):
        raise ValueError("Part424 does not match exact pinned immutable descriptor")
    # Pre-existing cache is consumed only if already fully verified by the loader.
    cached = store._read_verified_source_cache(key, size, sha)
    if cached is not None:
        return "existing_verified"
    # Exactly one invocation of existing bounded 600-second ranged recovery;
    # transport internal per-range retries remain strictly bounded.
    packed = store._slow_source_part_get(key, size)
    if len(packed) != size or native.digest(packed) != sha:
        raise ValueError("Source part424 full SHA256 mismatch")
    verify_gzip_eof(packed)
    native.ensure_tmpfs(target_root)
    import stat
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    root_fd = cache_fd = None
    try:
        root_fd = os.open(target_root, flags)
        root_stat = os.fstat(root_fd)
        if (not stat.S_ISDIR(root_stat.st_mode)
                or root_stat.st_uid != os.getuid()
                or stat.S_IMODE(root_stat.st_mode) & 0o077
                or root_stat.st_dev != os.stat("/run/user/1000").st_dev):
            raise PermissionError("Unsafe source cache root")
        try:
            os.mkdir("verified-source-cache", 0o700, dir_fd=root_fd)
        except FileExistsError:
            pass
        cache_fd = os.open("verified-source-cache", flags, dir_fd=root_fd)
        cache_stat = os.fstat(cache_fd)
        if (not stat.S_ISDIR(cache_stat.st_mode)
                or cache_stat.st_uid != os.getuid()
                or stat.S_IMODE(cache_stat.st_mode) & 0o077
                or cache_stat.st_dev != root_stat.st_dev):
            raise PermissionError("Unsafe source cache ownership/mode")
        name = key.rsplit("/", 1)[-1]
        try:
            os.stat(name, dir_fd=cache_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise FileExistsError("Part424 cache target already exists")
        tmp = ".part424." + str(os.getpid()) + ".tmp"
        fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                     os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=cache_fd)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(packed)
                stream.flush()
                os.fsync(stream.fileno())
            # link fails with EEXIST rather than overwrite a competing writer.
            os.link(tmp, name, src_dir_fd=cache_fd, dst_dir_fd=cache_fd,
                    follow_symlinks=False)
            os.fsync(cache_fd)
        finally:
            os.unlink(tmp, dir_fd=cache_fd)
    finally:
        for handle in (cache_fd, root_fd):
            if handle is not None:
                os.close(handle)
    if store._read_verified_source_cache(key, size, sha) != packed:
        raise RuntimeError("Verified source cache final readback mismatch")
    return "new_verified"


def assert_builder_inactive() -> None:
    status = subprocess.run(
        ["systemctl", "--user", "show", UNIT,
         "-p", "ActiveState", "-p", "MainPID", "--no-pager"],
        stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=10,
        check=True,
    ).stdout
    rows = dict(line.split("=", 1) for line in status.splitlines() if "=" in line)
    if rows.get("ActiveState") not in ("failed", "inactive") or rows.get("MainPID") != "0":
        raise RuntimeError("Existing builder is active or its status is unknown")


class OneShotSourceStore(native.VerifiedImmutableReuseStore):
    """Skip unrelated cloud shard inventory; inspect only checkpoint and part424."""
    def _load_verified_inventory(self):
        return {}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if not args.apply or os.environ.get("GREPMESH_OWNER_APPROVED_PART424_FETCH") != "1":
        raise SystemExit("Owner-gated: --apply AND approved operator environment required")
    os.umask(0o077)
    assert_builder_inactive()
    native.ensure_tmpfs(native.ROOT)
    lock = os.open(native.ROOT / "private-build.lock",
                   os.O_CREAT | os.O_RDWR | os.O_CLOEXEC, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert_builder_inactive()
        store = OneShotSourceStore()
        raw = native.CloudStore.get(store, native.PREFIX + "/progress.json",
                                    16 * native.MIB)
        if native.digest(raw) != EXPECTED_CHECKPOINT_SHA:
            raise ValueError("Pinned source checkpoint SHA256 changed")
        progress = store.bind_verified_source_checkpoint(
            raw, json.loads(native.SOURCE_VERIFICATION.read_text()))
        matches = [part for part in progress["chunks"]
                   if part["key"].rsplit("/", 1)[-1].startswith(ORDINAL + "-")]
        if len(matches) != 1:
            raise ValueError("Part424 missing or duplicated in exact source checkpoint")
        result = stage_verified_part(store, matches[0])
        # Secrets, cloud names, source rows or data MUST NOT be included.
        print(json.dumps({"status": result, "part": ORDINAL, "bytes": EXPECTED_BYTES,
                          "sha256": EXPECTED_SHA, "checkpoint_sha256": EXPECTED_CHECKPOINT_SHA,
                          "cloud_writes": store.puts, "production_changed": False,
                          "consistent_snapshot": False}, sort_keys=True))
        return 0
    finally:
        os.close(lock)


if __name__ == "__main__":
    raise SystemExit(main())
