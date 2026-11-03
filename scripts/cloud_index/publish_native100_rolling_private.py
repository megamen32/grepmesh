#!/usr/bin/env python3
"""Publish an UNEXPOSED, provisional native server-100 FTS corpus to private cloud.

Source: immutable completed ROLLING archive, not a consistent live snapshot.
All intermediate SQLite scratch is in tmpfs. Never changes native SQLite,
live GrepMesh, GPTAdmin routes/ACLs, or source archive progress. Fail closed.
"""
from __future__ import annotations
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import time

from cloud_index import (CloudStore, Publisher, digest, json_bytes,
                         document, ensure_tmpfs, INDEX_LAYOUT,
                         MAX_DOCUMENT, MAX_MANIFEST, MIB, valid_key)
from native100_part_checkpoint import BatchSpool, DurableLedger

SNAPSHOT = "20261031T233108Z-5732b979"
PREFIX = "archives/server-100/rolling/" + SNAPSHOT
ARCHIVED = 3187870
PARTS = 443
PARTITIONS = 16
ROOT = Path("/run/user/1000/grepmesh-native100-private-build")
AUDIT = Path("/home/roomhacker/.local/state/grepmesh-native100-rolling-coverage-audit.json")
SOURCE_VERIFICATION = Path("/home/roomhacker/.local/state/grepmesh-native100-rolling-verify-progress.json")
RECEIPT = Path("/home/roomhacker/.local/state/grepmesh-native100-private-search-candidate.json")
PROGRESS = Path("/run/user/1000/grepmesh-native100-private-search-build.progress.json")
POLICY = ("PROVISIONAL archived native FTS history; exact original path and body; "
          "source files never read; strict verified filesystem ACL mandatory; "
          "NOT a consistent snapshot or production source")
TARGET_BYTES = 16 * MIB


def safe_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + ".tmp." + str(os.getpid()))
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump(payload, handle, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


class VerifiedImmutableReuseStore(CloudStore):
    """Reuse private immutable shards only after remote size+MD5 verification.

    Listing the flat object directory once avoids thousands of redundant
    per-object Yandex.Disk API calls after a recoverable build interruption.
    Never treat a failed/partial listing as proof that an object is absent.
    Unknown objects are still checked individually before any new upload.
    """
    def __init__(self):
        super().__init__()
        self.reused = 0
        self.reused_from_inventory = 0
        self.known = self._load_verified_inventory()
        self.verified_source_parts = None

    def _load_verified_inventory(self):
        import re
        import subprocess
        command = self.command([
            "lsjson", self.prefix + "/objects", "--files-only", "--hash"
        ])
        try:
            result = subprocess.run(command, stdin=subprocess.DEVNULL,
                                    capture_output=True, timeout=90)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("Private cloud object inventory timed out; no writes") from exc
        if result.returncode or len(result.stdout) > 32 * MIB:
            raise RuntimeError("Private cloud object inventory unavailable or too large")
        try:
            items = json.loads(result.stdout)
        except (ValueError, UnicodeDecodeError) as exc:
            raise RuntimeError("Invalid private cloud object inventory") from exc
        if not isinstance(items, list):
            raise RuntimeError("Private cloud object inventory must be an array")
        known = {}
        for item in items:
            if not isinstance(item, dict):
                raise RuntimeError("Malformed cloud inventory entry")
            name = item.get("Name")
            if not isinstance(name, str):
                raise RuntimeError("Malformed cloud inventory name")
            if not re.fullmatch(r"[a-f0-9]{64}\.sqlite\.gz", name):
                continue
            md5 = item.get("Hashes", {}).get("md5") if isinstance(item.get("Hashes"), dict) else None
            size = item.get("Size")
            if (item.get("Path") != name or item.get("IsDir") is not False
                    or type(size) is not int or size <= 0
                    or not isinstance(md5, str)
                    or not re.fullmatch(r"[a-f0-9]{32}", md5)):
                raise RuntimeError("Incomplete private cloud object size/MD5 inventory")
            key = "objects/" + name
            if key in known:
                raise RuntimeError("Duplicate object in cloud inventory")
            known[key] = (size, md5)
        return known

    def bind_verified_source_checkpoint(self, raw, receipt):
        """Pin source metadata to the independently completed replay receipt.

        A live metadata request is not needed to rediscover the size/hash of
        an immutable part already covered by this exact checkpoint digest.
        Bind atomically, only after the complete checkpoint is validated.
        """
        import re
        if (not isinstance(receipt, dict)
                or receipt.get("snapshot") != SNAPSHOT
                or receipt.get("all_chunks_replayed") is not True
                or type(receipt.get("checkpoint_chunks")) is not int
                or receipt["checkpoint_chunks"] != PARTS
                or type(receipt.get("verified_next_index")) is not int
                or receipt["verified_next_index"] != PARTS
                or receipt.get("safe_for_deletion") is not False
                or receipt.get("consistent_snapshot") is not False
                or digest(raw) != receipt.get("checkpoint_sha256")):
            raise ValueError("Independent verified source checkpoint mismatch")
        progress = json.loads(raw)
        if (not isinstance(progress, dict)
                or progress.get("finished") is not True
                or progress.get("safe_for_deletion") is not False
                or progress.get("consistent_snapshot") is not False
                or type(progress.get("rows")) is not int
                or progress["rows"] != ARCHIVED
                or not isinstance(progress.get("chunks"), list)
                or len(progress["chunks"]) != PARTS):
            raise ValueError("Independent verified source checkpoint is incomplete")
        parts = {}
        for part in progress["chunks"]:
            if not isinstance(part, dict):
                raise ValueError("Invalid pinned source part metadata")
            key, size, sha = (part.get("key"), part.get("compressed_bytes"),
                              part.get("sha256"))
            if (not isinstance(key, str) or not isinstance(sha, str)
                    or not re.fullmatch(r"[a-f0-9]{64}", sha)
                    or not re.fullmatch(re.escape(PREFIX) + r"/parts/[0-9]{6}-"
                                        + re.escape(sha) + r"\.jsonl\.gz", key)
                    or type(size) is not int or not 0 < size <= 64 * MIB
                    or key in parts):
                raise ValueError("Invalid pinned source part metadata")
            parts[key] = (size, sha)
        self.verified_source_parts = parts
        return progress

    def _pinned_source_hash(self, key, limit):
        if self.verified_source_parts is None:
            return None
        metadata = self.verified_source_parts.get(key)
        if metadata is None or type(limit) is not int or metadata[0] != limit:
            raise ValueError("Requested object does not match pinned source part")
        return metadata[1]

    def _slow_source_part_get(self, key, limit):
        """Read immutable source parts in bounded HTTP ranges when full cat stalls.

        Yandex.Disk's full-object rclone cat can time out after 240 seconds for
        a 12 MiB source part even though 1 MiB ranged requests work. Verify
        pinned checkpoint size/SHA first (or remote size/MD5 when unbound).
        Validate every non-overlapping range; pinned reads request one extra
        byte on the final range to prove EOF and reject appended content.
        The caller still verifies source SHA-256 and row accounting.
        No persistent source/scratch writes, ACL changes, or partial returns.
        """
        import re
        import subprocess
        key = valid_key(key)
        if not (key.startswith(PREFIX + "/parts/")
                and 0 < limit <= 64 * MIB):
            raise ValueError("Ranged source recovery requires a bounded archive part")
        remote = self.prefix + "/" + key
        sha = self._pinned_source_hash(key, limit)
        md5 = None
        if sha is None:
            try:
                stat = subprocess.run(
                    self.command(["lsjson", remote, "--stat", "--hash"]),
                    stdin=subprocess.DEVNULL, capture_output=True, timeout=45)
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError("Immutable source part metadata timed out") from exc
            if stat.returncode:
                raise RuntimeError("Immutable source part metadata unavailable")
            try:
                info = json.loads(stat.stdout)
            except ValueError as exc:
                raise RuntimeError("Malformed immutable source part metadata") from exc
            if not isinstance(info, dict):
                raise RuntimeError("Malformed immutable source part metadata")
            hashes = info.get("Hashes")
            md5 = hashes.get("md5") if isinstance(hashes, dict) else None
            if (type(info.get("Size")) is not int or info["Size"] != limit
                    or not isinstance(md5, str)
                    or not re.fullmatch("[a-fA-F0-9]{32}", md5)):
                raise ValueError("Immutable source part remote size/hash mismatch")

        deadline = time.monotonic() + 600
        segments = []
        # Retain only a validated part ordinal and fixed failure categories:
        # raw stderr may contain source names, credentials, or bearer headers.
        part_ordinal = key.rsplit("/", 1)[-1].split("-", 1)[0]
        if not re.fullmatch(r"[0-9]{6}", part_ordinal):
            part_ordinal = "unknown"
        for offset in range(0, limit, MIB):
            amount = min(MIB, limit - offset)
            read_amount = amount + int(sha is not None and offset + amount == limit)
            last_failure = "unknown"
            for attempt in range(3):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError(
                        "Immutable source ranged-read time budget exhausted "
                        f"(part={part_ordinal}, offset={offset})")
                try:
                    result = subprocess.run(
                        self.command(["cat", remote, "--offset", str(offset),
                                      "--count", str(read_amount)]),
                        stdin=subprocess.DEVNULL, capture_output=True,
                        timeout=min(60, max(1, remaining)))
                except subprocess.TimeoutExpired:
                    result = None
                    last_failure = "timeout"
                if result is not None and result.returncode == 0:
                    if len(result.stdout) != amount:
                        raise ValueError("Immutable source part range length mismatch")
                    segments.append(result.stdout)
                    break
                if result is not None:
                    error = result.stderr.decode("utf-8", "replace").lower()
                    if any(word in error for word in (
                            "unauthorized", "permission denied", "forbidden",
                            "401", "403", "not found", "directory not found")):
                        raise RuntimeError("Immutable source range denied or missing")
                    last_failure = ("rate_limited" if any(
                        word in error for word in ("429", "too many requests",
                                                   "rate limit"))
                        else f"exit_{result.returncode}")
                if attempt < 2:
                    time.sleep(min(2 ** attempt, 3))
            else:
                raise RuntimeError(
                    "Immutable source ranged read unavailable after bounded retries "
                    f"(part={part_ordinal}, offset={offset}, last_failure={last_failure})")
        body = b"".join(segments)
        if sha is not None:
            if len(body) != limit or digest(body) != sha:
                raise ValueError("Immutable source part complete SHA256 mismatch")
        elif len(body) != limit or hashlib.md5(body).hexdigest() != md5.lower():
            raise ValueError("Immutable source part complete MD5 mismatch")
        self.gets += 1
        self.bytes_in += len(body)
        return body

    def _read_verified_source_cache(self, key, limit, sha):
        """Use an existing owner-only tmpfs part iff it matches the pinned snapshot.

        No cache creation, cloud reads, archive mutation or fallback on corrupt
        bytes. O_NOFOLLOW protects the root, cache directory and part file.
        """
        import re
        import stat

        if not sha or self.verified_source_parts is None:
            return None
        if self._pinned_source_hash(key, limit) != sha:
            raise ValueError("Pinned source cache descriptor mismatch")
        name = key.rsplit("/", 1)[-1]
        if not re.fullmatch(r"[0-9]{6}-" + re.escape(sha) + r"\.jsonl\.gz", name):
            raise ValueError("Invalid pinned source cache filename")
        dir_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        root_fd = cache_fd = part_fd = None
        try:
            root_fd = os.open(ROOT, dir_flags)
            cache_fd = os.open("verified-source-cache", dir_flags, dir_fd=root_fd)
            for handle in (root_fd, cache_fd):
                meta = os.fstat(handle)
                if (not stat.S_ISDIR(meta.st_mode) or meta.st_uid != os.getuid()
                        or meta.st_dev != os.stat("/run/user/1000").st_dev
                        or stat.S_IMODE(meta.st_mode) & 0o077):
                    raise PermissionError("Unsafe pinned source cache directory")
            part_fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                              dir_fd=cache_fd)
            meta = os.fstat(part_fd)
            if (not stat.S_ISREG(meta.st_mode) or meta.st_uid != os.getuid()
                    or meta.st_dev != os.stat("/run/user/1000").st_dev
                    or meta.st_nlink != 1 or stat.S_IMODE(meta.st_mode) & 0o077):
                raise PermissionError("Unsafe pinned source cache part")
            if meta.st_size != limit:
                raise ValueError("Pinned source cache size mismatch")
            with os.fdopen(part_fd, "rb") as handle:
                part_fd = None
                data = handle.read(limit + 1)
            if len(data) != limit or digest(data) != sha:
                raise ValueError("Pinned source cache SHA256 mismatch")
        except FileNotFoundError:
            return None
        finally:
            for handle in (part_fd, cache_fd, root_fd):
                if handle is not None:
                    os.close(handle)
        self.gets += 1
        self.bytes_in += len(data)
        return data

    def get(self, key, limit=64 * MIB):
        # After two bounded fast reads, use single range fallback; never restart
        # the entire 12 MiB full read three more times or accept partial bytes.
        import subprocess
        import re
        source_part = (key.startswith(PREFIX + "/parts/")
                       and 0 < limit <= 64 * MIB)
        sha = self._pinned_source_hash(key, limit) if source_part else None
        if source_part:
            cached = self._read_verified_source_cache(key, limit, sha)
            if cached is not None:
                return cached
            required = os.environ.get("GREPMESH_REQUIRE_CACHED_SOURCE_PART", "")
            if required and not re.fullmatch(r"[0-9]{6}", required):
                raise RuntimeError("Invalid required source cache ordinal")
            if required and key.rsplit("/", 1)[-1].startswith(required + "-"):
                raise RuntimeError("Required verified source cache missing; no cloud retry")
        attempts = 3
        for attempt in range(attempts):
            try:
                if source_part and attempt == 2:
                    return self._slow_source_part_get(key, limit)
                body = super().get(key, limit)
                if sha is not None and (len(body) != limit or digest(body) != sha):
                    raise ValueError("Immutable source part complete SHA256 mismatch")
                return body
            except subprocess.TimeoutExpired as exc:
                failure = exc
            except RuntimeError as exc:
                if not str(exc).startswith("Cloud read failed: "):
                    raise
                failure = exc
            if attempt + 1 < attempts:
                time.sleep(min(2 ** attempt, 3))
        raise RuntimeError("Immutable cloud source read unavailable after bounded retries") from failure

    def put(self, key, data):
        key = valid_key(key)
        if not key.startswith("objects/"):
            return super().put(key, data)
        expected = (len(data), hashlib.md5(data).hexdigest())
        remote = self.known.get(key)
        if remote is not None:
            if remote != expected:
                raise RuntimeError("Immutable cloud object collision or corruption")
            self.reused += 1
            self.reused_from_inventory += 1
            return
        import subprocess
        cmd = self.command(["lsjson", self.prefix + "/" + key,
                            "--stat", "--hash"])
        for attempt in range(2):
            try:
                result = subprocess.run(cmd, stdin=subprocess.DEVNULL,
                                        capture_output=True, timeout=45)
            except subprocess.TimeoutExpired as exc:
                if attempt:
                    raise RuntimeError("Cloud object check timed out; refusing overwrite") from exc
                time.sleep(2)
                continue
            if result.returncode == 0:
                try:
                    info = json.loads(result.stdout)
                except ValueError as exc:
                    raise RuntimeError("Malformed remote cloud object metadata") from exc
                remote = (info.get("Size"), info.get("Hashes", {}).get("md5"))
                if remote != expected:
                    raise RuntimeError("Immutable cloud object collision or corruption")
                self.known[key] = expected
                self.reused += 1
                return
            message = result.stderr.decode("utf-8", "replace").lower()
            if not ("directory not found" in message or "object not found" in message):
                raise RuntimeError("Immutable cloud existence check failed; refusing overwrite")
            try:
                super().put(key, data)
            except subprocess.TimeoutExpired as exc:
                # Upload may have landed; never blindly upload the same object again.
                # Reconcile the immutable object by exact remote size and MD5.
                for verify_attempt in range(2):
                    try:
                        verified = subprocess.run(
                            cmd, stdin=subprocess.DEVNULL,
                            capture_output=True, timeout=45)
                    except subprocess.TimeoutExpired:
                        if verify_attempt == 0:
                            time.sleep(2)
                            continue
                        raise RuntimeError(
                            "Post-upload immutable object verification timed out") from exc
                    if verified.returncode:
                        raise RuntimeError(
                            "Post-upload immutable object verification unavailable") from exc
                    try:
                        info = json.loads(verified.stdout)
                    except ValueError as bad:
                        raise RuntimeError(
                            "Malformed post-upload immutable object metadata") from bad
                    remote = (info.get("Size"), info.get("Hashes", {}).get("md5"))
                    if remote != expected:
                        raise RuntimeError(
                            "Post-upload immutable object size/hash mismatch") from exc
                    # Parent increments puts only after its verification succeeds.
                    self.puts += 1
                    break
            self.known[key] = expected  # exact remote size/MD5 is verified
            return
        raise RuntimeError("Cloud object check ended without an authoritative result")


class PathMetadataSpool:
    """Ordered path metadata in compressed owner-only tmpfs, not Python tuples.

    No source text or extra search index is created. The original path-index
    schema, order, SHA256 and row count are preserved. Iteration validates the
    entire private spool before the caller may commit or publish the index.
    """
    MAX_LINE = 65536

    def __init__(self, index, root):
        import stat
        root = Path(root)
        st = root.lstat()
        runtime = Path('/run/user') / str(os.getuid())
        if (not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid()
                or st.st_mode & 0o077 or st.st_dev != runtime.stat().st_dev):
            raise RuntimeError('Path metadata must stay in owner-only tmpfs')
        self.path = root / ('path-metadata-%02d-%s.jsonl.gz' % (index, os.urandom(8).hex()))
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        self._file = os.fdopen(fd, 'wb')
        self._gzip = gzip.GzipFile(fileobj=self._file, mode='wb', compresslevel=1, mtime=0)
        self._count = 0
        self._logical = hashlib.sha256()
        self._closed = False
        self._device = st.st_dev

    @staticmethod
    def _validate(row):
        import re
        if (not isinstance(row, (tuple, list)) or len(row) != 4
                or not isinstance(row[0], str) or not row[0].startswith('/')
                or '\x00' in row[0] or '..' in Path(row[0]).parts
                or type(row[1]) is not int or type(row[2]) is not int
                or row[1] < 0 or row[2] < 0
                or not isinstance(row[3], str) or not re.fullmatch('[0-9a-f]{64}', row[3])):
            raise ValueError('Invalid private path metadata row')

    def append(self, row):
        if self._closed:
            raise RuntimeError('Path metadata spool already sealed')
        self._validate(row)
        data = json_bytes(row) + b'\n'
        if len(data) > self.MAX_LINE:
            raise ValueError('Path metadata row exceeds bound')
        self._gzip.write(data)
        self._logical.update(data)
        self._count += 1

    def close(self):
        if not self._closed:
            self._gzip.close()
            self._file.close()
            self._closed = True

    def __len__(self):
        return self._count

    def __iter__(self):
        import stat
        self.close()
        fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as f:
            st = os.fstat(f.fileno())
            if (not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid()
                    or st.st_nlink != 1 or st.st_mode & 0o077
                    or st.st_dev != self._device):
                raise RuntimeError('Unsafe path metadata spool')
            logical = hashlib.sha256()
            count = 0
            with gzip.GzipFile(fileobj=f, mode='rb') as stream:
                while True:
                    data = stream.readline(self.MAX_LINE + 1)
                    if not data:
                        break
                    if len(data) > self.MAX_LINE or not data.endswith(b'\n'):
                        raise ValueError('Path metadata frame exceeds bound')
                    row = json.loads(data)
                    self._validate(row)
                    logical.update(data)
                    count += 1
                    yield tuple(row)
            if count != self._count or logical.digest() != self._logical.digest():
                raise RuntimeError('Path metadata count or checksum mismatch')


class Partition:
    def __init__(self, index, pub):
        self.index = index
        self.publisher = pub
        self.batch_recorder = None
        self.scope = "rolling100-20261031t233108z-5732b979-%02d" % index
        self.paths = PathMetadataSpool(index, ROOT)
        self.parts = []
        self.docs = []
        self.pending_bytes = 0
        self.count = 0
        self.logical = hashlib.sha256()

    def add(self, path, body):
        doc = document(path, body, 0, 0)
        self.paths.append((path, 0, 0, doc["sha256"]))
        if self.batch_recorder is not None:
            self.batch_recorder.append(self.index, path, 0, 0, doc["sha256"])
        self.docs.append(doc)
        self.pending_bytes += len(body.encode("utf-8"))
        self.count += 1
        self.logical.update(json_bytes([path, doc["sha256"]]))
        if self.pending_bytes >= TARGET_BYTES or len(self.docs) >= 512:
            self.flush()

    def flush(self):
        if not self.docs:
            return
        self.parts.extend(self.publisher.shard(self.docs))
        if len(self.parts) > 500:
            raise RuntimeError("Shard count overflow; keep unexposed, use more partitions")
        self.docs = []
        self.pending_bytes = 0


def preflight_required_source_cache(store, progress):
    """Check the exact pinned part before replaying earlier archive chunks."""
    import re
    required = os.environ.get("GREPMESH_REQUIRE_CACHED_SOURCE_PART", "")
    if not required:
        return
    if not re.fullmatch(r"[0-9]{6}", required):
        raise ValueError("Invalid required immutable source part ordinal")
    if not isinstance(store, VerifiedImmutableReuseStore):
        raise RuntimeError("Required source cache needs verified immutable reuse")
    matches = [part for part in progress["chunks"]
               if part["key"].rsplit("/", 1)[-1].startswith(required + "-")]
    if len(matches) != 1:
        raise RuntimeError("Required source part is not unique in pinned snapshot")
    part = matches[0]
    cached = store._read_verified_source_cache(
        part["key"], part["compressed_bytes"], part["sha256"])
    if cached is None:
        raise RuntimeError("Required verified source cache missing before replay")


def run():
    os.umask(0o077)
    ensure_tmpfs(ROOT)
    import fcntl
    lock_fd = os.open(ROOT / "private-build.lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        os.close(lock_fd)
        raise RuntimeError("Private FTS build already running; refusing duplicate") from exc
    try:
        return _run_locked()
    finally:
        os.close(lock_fd)


def _run_locked():
    if RECEIPT.exists():
        raise RuntimeError("Existing private search receipt must be inspected; no duplicate run")
    if not AUDIT.is_file():
        raise RuntimeError("Full immutable native100 archive audit has not completed")
    audit = json.loads(AUDIT.read_text())
    if not (audit.get("snapshot") == SNAPSHOT
            and audit.get("historical_documents") == ARCHIVED
            and audit.get("cloud_parts_fully_replayed") == PARTS
            and audit.get("duplicate_paths") == 0
            and audit.get("malformed_paths") == 0
            and audit.get("safe_for_deletion") is False
            and audit.get("consistent_snapshot") is False):
        raise RuntimeError("Full independent source audit has not passed")
    reuse = os.environ.get("GREPMESH_REUSE_VERIFIED_SHARDS") == "1"
    store = VerifiedImmutableReuseStore() if reuse else CloudStore()
    checkpoint = store.get(PREFIX + "/progress.json", 16 * MIB)
    progress = (store.bind_verified_source_checkpoint(
                    checkpoint, json.loads(SOURCE_VERIFICATION.read_text()))
                if reuse else json.loads(checkpoint))
    if not (progress.get("finished") is True
            and progress.get("safe_for_deletion") is False
            and progress.get("consistent_snapshot") is False
            and progress.get("rows") == ARCHIVED
            and len(progress["chunks"]) == PARTS):
        raise RuntimeError("Provisional cloud source checkpoint was changed")
    preflight_required_source_cache(store, progress)
    started = time.monotonic()
    publisher = Publisher(store, ROOT, shard_text_bytes=TARGET_BYTES)
    partitions = [Partition(i, publisher) for i in range(PARTITIONS)]
    # Off by default, opt-in only under the already reviewed source, cloud
    # destination, exclusive builder lease and exact restored checkpoint.
    durable_mode = os.environ.get("GREPMESH_DURABLE_BUILD_MODE", "")
    if durable_mode not in ("", "init", "resume"):
        raise ValueError("Unknown durable checkpoint mode")
    if durable_mode and not reuse:
        raise RuntimeError("Durable checkpoint requires pinned verified reuse")
    ledger = (DurableLedger(store, digest(checkpoint), SNAPSHOT, PARTITIONS)
              if durable_mode else None)
    count = 0
    oversize = 0
    raw_body_bytes = 0
    last_rowid = 0
    first_samples = {}
    start_part = 0
    if durable_mode == "init":
        ledger.init()  # require authoritative absence, never infer from error
    elif durable_mode == "resume":
        restored = ledger.restore(partitions)
        start_part = restored["parts_done"]
        count = restored["source_rows"]
        oversize = restored["oversize"]
        raw_body_bytes = restored["raw_body_bytes"]
        last_rowid = restored["last_rowid"]
        first_samples = {int(k):v for k,v in restored.get("first_samples",{}).items()}
    batch = BatchSpool(ROOT, PARTITIONS) if ledger and start_part < PARTS else None
    for partition in partitions:
        partition.batch_recorder = batch
    for idx, part in enumerate(progress["chunks"]):
        if idx < start_part:
            continue  # only a fully verified remote checkpoint can skip parts
        packed = store.get(part["key"], part["compressed_bytes"])
        if len(packed) != part["compressed_bytes"] or digest(packed) != part["sha256"]:
            raise RuntimeError("Source archive part failed SHA256")
        part_count = 0
        with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
            for line in stream:
                row = json.loads(line)
                if not isinstance(row, list) or len(row) != 3:
                    raise RuntimeError("Invalid historical source row")
                rowid, path, body = row
                if (type(rowid) is not int or rowid <= last_rowid
                        or not isinstance(path, str) or not path.startswith("/")
                        or "\x00" in path or ".." in Path(path).parts
                        or not isinstance(body, str)):
                    raise RuntimeError("Historical identity/path/body failed validation")
                last_rowid = rowid
                count += 1
                part_count += 1
                encoded = body.encode("utf-8")
                raw_body_bytes += len(encoded)
                if len(encoded) > MAX_DOCUMENT:
                    oversize += 1  # Exact body already preserved in immutable source.
                    continue
                partition_id = hashlib.sha256(path.encode("utf-8")).digest()[0] % PARTITIONS
                bucket = partitions[partition_id]
                bucket.add(path, body)
                if partition_id not in first_samples:
                    first_samples[partition_id] = {
                        "path": path, "body_sha256": digest(encoded)}
        if part_count != part["rows"] or last_rowid != part["last_rowid"]:
            raise RuntimeError("Archive source row accounting changed")
        if (idx + 1) % 10 == 0 or idx + 1 == PARTS:
            if ledger is not None:
                for partition in partitions:
                    partition.flush()  # no pending docs in a committed batch
                durable_state = {
                    "parts_done": idx+1, "source_rows": count,
                    "oversize": oversize, "raw_body_bytes": raw_body_bytes,
                    "last_rowid": last_rowid, "first_samples": first_samples,
                    "partitions": [
                        {"count":p.count, "logical_sha256":p.logical.hexdigest(),
                         "parts":p.parts} for p in partitions
                    ],
                    "consistent_snapshot": False, "safe_for_deletion": False,
                }
                ledger.commit(batch, durable_state)
                batch = BatchSpool(ROOT, PARTITIONS) if idx+1 < PARTS else None
                for partition in partitions:
                    partition.batch_recorder = batch
            safe_json(PROGRESS, {
                **({"durable_committed_source_parts":ledger.state["parts_done"]}
                   if ledger else {}),
                "source_parts_read": idx + 1,
                "source_rows_read": count,
                "searchable_documents": sum(p.count for p in partitions),
                "oversize_preserved_in_source": oversize,
                "cloud_immutable_shards_uploaded": sum(len(p.parts) for p in partitions),
                "cloud_immutable_shards_reused": getattr(store, "reused", 0),
                "cloud_immutable_shards_uploaded_this_run": store.puts,
                "elapsed_seconds": round(time.monotonic() - started, 1),
                "consistent_snapshot": False,
                "safe_for_deletion": False,
            })
    if (count != ARCHIVED or last_rowid != progress["last_rowid"]
            or oversize != audit["oversize_documents"]
            or raw_body_bytes != audit["total_text_body_bytes"]
            or sum(p.count for p in partitions) + oversize != ARCHIVED):
        raise RuntimeError("Full source coverage parity failed")
    # No latest pointers exist yet. First publish and verify every immutable
    # per-partition path index and manifest, only then publish private pointers.
    candidates = []
    for p in partitions:
        p.flush()
        path_index = publisher.path_index(p.paths)
        manifest = {
            "path_index": path_index,
            "index_layout": INDEX_LAYOUT,
            "format": 1,
            "scope": p.scope,
            "source_host": "server-100",
            "source_root": "/",
            "policy": POLICY + "; bucket %d/%d" % (p.index, PARTITIONS),
            "skipped": {"oversized_historical_body": oversize},
            "documents": p.count,
            "logical_sha256": p.logical.hexdigest(),
            "parts": p.parts,
            "generated_at": time.time(),
            "complete_for_policy": True,
            "storage": "private immutable Yandex.Disk shards / tmpfs-only bounded cache",
            "build_seconds": round(time.monotonic() - started, 3),
        }
        blob = json_bytes(manifest)
        if len(blob) > MAX_MANIFEST:
            raise RuntimeError("Cloud scope manifest too big; no pointers published")
        key = "manifests/" + p.scope + "/" + digest(blob) + ".json"
        store.put(key, blob)
        # Independent remote admission of manifest, before any latest pointers.
        if digest(store.get(key, MAX_MANIFEST)) != digest(blob):
            raise RuntimeError("Manifest round-trip hash mismatch")
        candidates.append({
            "scope": p.scope, "manifest": key, "sha256": digest(blob),
            "documents": p.count, "parts": len(p.parts),
            "path_index_sha256": path_index["sha256"],
        })
    for p, candidate in zip(partitions, candidates):
        pointer = {
            "manifest": candidate["manifest"], "sha256": candidate["sha256"],
            "documents": p.count, "source_host": "server-100",
            "source_root": "/"}
        store.put("scopes/" + p.scope + "/latest.json", json_bytes(pointer))
    summary = {
        "event": "provisional_server100_private_search_candidate_published",
        "archive_snapshot": SNAPSHOT,
        "historical_source_documents": ARCHIVED,
        "indexed_documents": sum(p.count for p in partitions),
        "oversize_preserved_in_original_archive_only": oversize,
        "partitions": candidates,
        "sample_path_sha256": [
            {"bucket": i, "path_hash": digest(sample["path"].encode()),
             "body_sha256": sample["body_sha256"]}
            for i, sample in sorted(first_samples.items())],
        "consistent_snapshot": False, "safe_for_deletion": False,
        "verified_fs_acl_accepted": False,
        "production_route_switched": False,
        "old_sqlite_deleted": False,
        "ssd_reclaimed_bytes": 0,
        "cloud_immutable_shards_reused": getattr(store, "reused", 0),
        "elapsed_seconds": round(time.monotonic() - started, 1),
    }
    safe_json(RECEIPT, summary)
    print(json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    run()
