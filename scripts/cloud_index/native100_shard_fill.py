"""Bounded per-hash input queues; not an index, cache, or durable checkpoint.

Queued documents are compressed frames in the SAME total tmpfs envelope as
SQLite scratch. Only one admitted <=512-document / <=8 MiB body batch is ever
decoded. A successful checkpoint drains all queues. After SIGKILL, only this
marked, reproducible scratch is discarded; confirmed cloud objects survive.
"""
from __future__ import annotations
import hashlib
import json
import os
import struct
import zlib
from cloud_index import MAX_DOCUMENT, MIB, digest, json_bytes

PACKING_POLICY = 'hash16-filled-v1'
FRAME_HEADER = struct.Struct('>I')
MAX_FRAME_RAW = 6 * MAX_DOCUMENT + 32768


def queue_name(pid):
    if type(pid) is not int or not 0 <= pid < 16:
        raise ValueError('Invalid fill partition')
    return 'fill-%02d.frames' % pid


class FilledQueues:
    """16 fixed counters/digests, capped tmpfs bytes, no retained path lists."""
    def __init__(self, scratch, limits):
        self.scratch = scratch
        self.limits = limits
        # A quarter of the EXISTING scratch budget, not an additional budget.
        self.cap = min(16 * MIB, limits.max_sqlite_bytes // 4)
        self.counts = [0] * 16
        self.body_bytes = [0] * 16
        self.file_bytes = [0] * 16
        self.hashes = [hashlib.sha256() for _ in range(16)]
        self.used = 0
        self.peak_bytes = 0
        self.peak_documents = 0

    @staticmethod
    def encode(doc):
        raw = json_bytes(doc)
        if len(raw) > MAX_FRAME_RAW:
            raise ValueError('Fill frame exceeds bounded JSON encoding')
        packed = zlib.compress(raw, 1)
        if len(packed) > MAX_FRAME_RAW + 65536:
            raise ValueError('Fill frame exceeds compressed admission')
        return FRAME_HEADER.pack(len(packed)) + packed

    def needs_flush(self, pid, body_size):
        queue_name(pid)
        return (self.counts[pid] >= self.limits.pending_documents or
                self.body_bytes[pid] + body_size > self.limits.pending_body_bytes)

    def fullest(self):
        # Used only for total tmpfs pressure; recover the most occupied bytes.
        pid = max(range(16), key=lambda i: self.file_bytes[i])
        if not self.file_bytes[pid]:
            raise ValueError('Single fill frame exceeds existing scratch budget')
        return pid

    def _account(self):
        self.scratch.reserved_bytes = self.used
        self.peak_bytes = max(self.peak_bytes, self.used)
        self.peak_documents = max(self.peak_documents, sum(self.counts))
        self.scratch.measure_total(self.limits.max_sqlite_bytes)

    def append(self, pid, frame, body_size):
        name = queue_name(pid)
        if (type(body_size) is not int or not 0 <= body_size <= MAX_DOCUMENT or
                self.needs_flush(pid, body_size) or self.used + len(frame) > self.cap):
            raise ValueError('Fill queue admission exceeded before write')
        path = self.scratch.root / name
        if self.counts[pid]:
            fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
            try:self.scratch.check_regular_fd(fd)
            except BaseException:
                os.close(fd);raise
            if os.fstat(fd).st_size != self.file_bytes[pid]:
                os.close(fd)
                raise RuntimeError('Fill queue changed before append')
        else:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(fd, 'ab', buffering=0) as f:
                view = memoryview(frame)
                while view:
                    n = f.write(view)
                    if not n:
                        raise OSError('Short fill queue write')
                    view = view[n:]
        except BaseException:
            # No retry/cleanup after an ambiguous partial write. The original
            # locked runner will fail and replay only its uncommitted batch.
            raise
        self.hashes[pid].update(frame)
        self.counts[pid] += 1
        self.body_bytes[pid] += body_size
        self.file_bytes[pid] += len(frame)
        self.used += len(frame)
        self._account()

    def take(self, pid):
        """Verify one complete queue before removing its reproducible frames."""
        path = self.scratch.root / queue_name(pid)
        if not self.counts[pid]:
            return []
        if (self.counts[pid] > self.limits.pending_documents or
                self.body_bytes[pid] > self.limits.pending_body_bytes):
            raise RuntimeError('Decoded fill batch would exceed memory admission')
        fd = self.scratch._open_regular(path)
        docs = []
        sha = hashlib.sha256()
        used = body_bytes = 0
        with os.fdopen(fd, 'rb') as f:
            before = os.fstat(f.fileno())
            if before.st_size != self.file_bytes[pid]:
                raise RuntimeError('Fill queue length changed')
            for _ in range(self.counts[pid]):
                header = f.read(FRAME_HEADER.size)
                if len(header) != FRAME_HEADER.size:
                    raise ValueError('Truncated fill frame header')
                n = FRAME_HEADER.unpack(header)[0]
                if not 0 < n <= MAX_FRAME_RAW + 65536:
                    raise ValueError('Fill frame size invalid')
                packed = f.read(n)
                if len(packed) != n:
                    raise ValueError('Truncated fill frame')
                sha.update(header)
                sha.update(packed)
                used += len(header) + n
                dec = zlib.decompressobj()
                raw = dec.decompress(packed, MAX_FRAME_RAW + 1)
                if (len(raw) > MAX_FRAME_RAW or not dec.eof or
                        dec.unused_data or dec.unconsumed_tail):
                    raise ValueError('Fill frame decompression bound/EOF failed')
                d = json.loads(raw)
                if (not isinstance(d, dict) or d.get('kind') != 'document' or
                        not isinstance(d.get('path'), str) or not isinstance(d.get('body'), str)):
                    raise ValueError('Malformed fill document')
                body = d['body'].encode('utf8')
                if (len(body) > MAX_DOCUMENT or digest(body) != d.get('sha256') or
                        hashlib.sha256(d['path'].encode('utf8')).digest()[0] % 16 != pid):
                    raise ValueError('Fill document hash/partition mismatch')
                body_bytes += len(body)
                if body_bytes > self.limits.pending_body_bytes:
                    raise ValueError('Decoded fill bodies exceed memory bound')
                docs.append(d)
            if f.read(1) or used != self.file_bytes[pid] or sha.digest() != self.hashes[pid].digest():
                raise ValueError('Fill queue checksum/count mismatch')
            if body_bytes != self.body_bytes[pid]:
                raise ValueError('Fill queue body accounting mismatch')
            now = path.lstat()
            if (now.st_ino, now.st_dev, now.st_size) != (before.st_ino, before.st_dev, before.st_size):
                raise RuntimeError('Fill queue replaced during verification')
        path.unlink()  # only fully verified, marked, uncommitted scratch
        self.used -= self.file_bytes[pid]
        self.counts[pid] = self.body_bytes[pid] = self.file_bytes[pid] = 0
        self.hashes[pid] = hashlib.sha256()
        self._account()
        return docs
