"""Optional, fail-closed cloud checkpoint for the provisional native100 builder.

No production pointer or filesystem ACL changes. Every committed source batch:
  verified immutable FTS shard writes -> one verified immutable metadata segment
  -> verified immutable state manifest -> private mutable checkpoint head.
Only a head whose content+manifest+segments re-verify can skip source parts.
A half-upload or failed head update can never silently advance the source offset.
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import subprocess

from cloud_index import digest, json_bytes, valid_key

MAX_COMPRESSED = 48 * 1024 * 1024
MAX_MANIFEST = 8 * 1024 * 1024
MAX_ROW = 65536
MAX_BATCH_ROWS = 200000
MAX_DECOMPRESSED = 128 * 1024 * 1024
CHECKPOINT_FORMAT = 1
_HEX = re.compile(r"[a-f0-9]{64}\Z")
_NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,90}\Z")


def _check_root(root: Path):
    root = Path(root)
    info = root.lstat()
    runtime = Path('/run/user') / str(os.getuid())
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or info.st_mode & 0o077 or info.st_dev != runtime.stat().st_dev):
        raise RuntimeError('Checkpoint staging must be private tmpfs')
    return root


def _row_valid(row, partitions):
    from publish_native100_rolling_private import PathMetadataSpool
    if (not isinstance(row, (list, tuple)) or len(row) != 5
            or type(row[0]) is not int or not 0 <= row[0] < partitions):
        raise ValueError('Invalid checkpoint partition row')
    PathMetadataSpool._validate(row[1:])


def _decode_rows(payload, *, count, body_sha, partitions):
    if len(payload) > MAX_COMPRESSED or digest(payload) != body_sha:
        raise ValueError('Checkpoint segment SHA/size mismatch')
    if type(count) is not int or not 0 <= count <= MAX_BATCH_ROWS:
        raise ValueError('Invalid checkpoint segment row bound')
    n = 0
    nbytes = 0
    with gzip.GzipFile(fileobj=io.BytesIO(payload), mode='rb') as f:
        while True:
            line = f.readline(MAX_ROW+1)
            if not line:
                break
            if len(line) > MAX_ROW or not line.endswith(b'\n'):
                raise ValueError('Checkpoint metadata frame overflow')
            nbytes += len(line)
            if nbytes > MAX_DECOMPRESSED:
                raise ValueError('Checkpoint uncompressed metadata exceeds bound')
            obj = json.loads(line)
            _row_valid(obj, partitions)
            n += 1
            if n > count:
                raise ValueError('Checkpoint segment unexpected extra rows')
            yield obj
    if n != count:
        raise ValueError('Checkpoint segment short read')


class BatchSpool:
    """Exactly one source-batch worth of path metadata, never source bodies."""
    def __init__(self, root, partitions):
        self.root = _check_root(Path(root))
        self.partitions = partitions
        name = 'batch-metadata-' + os.urandom(12).hex() + '.jsonl.gz'
        self.path = self.root / name
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        self.file = os.fdopen(fd, 'wb')
        self.gz = gzip.GzipFile(fileobj=self.file, mode='wb', compresslevel=1, mtime=0)
        self.count = 0
        self.h = hashlib.sha256()
        self.sealed = False

    def append(self, partition, path, size, mtime_ns, sha):
        if self.sealed:
            raise RuntimeError('Checkpoint batch already sealed')
        row = [partition, path, size, mtime_ns, sha]
        _row_valid(row, self.partitions)
        raw = json_bytes(row) + b'\n'
        if len(raw) > MAX_ROW:
            raise ValueError('Checkpoint row exceeds bound')
        self.gz.write(raw)
        self.h.update(raw)
        self.count += 1

    def seal(self):
        if not self.sealed:
            self.gz.close()
            self.file.flush()
            os.fsync(self.file.fileno())
            self.file.close()
            self.sealed = True
        fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as f:
            st = os.fstat(f.fileno())
            if (not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid()
                    or st.st_nlink != 1 or st.st_mode & 0o077
                    or st.st_size > MAX_COMPRESSED):
                raise RuntimeError('Unsafe checkpoint batch')
            body = f.read(MAX_COMPRESSED + 1)
        if len(body) > MAX_COMPRESSED:
            raise ValueError('Checkpoint batch too large')
        # Full streamed gzip CRC/EOF and count verification before publication.
        it = _decode_rows(body, count=self.count, body_sha=digest(body),
                          partitions=self.partitions)
        h = hashlib.sha256()
        for row in it:
            h.update(json_bytes(row) + b'\n')
        if h.digest() != self.h.digest():
            raise ValueError('Checkpoint staged metadata digest changed')
        return body

    def cleanup(self):
        if not self.sealed:
            self.gz.close()
            self.file.close()
            self.sealed = True
        self.path.unlink(missing_ok=True)


class DurableLedger:
    """Single-authority private batch ledger. Not a production MCP index."""
    def __init__(self, store, source_sha256, snapshot, partitions=16):
        if not _HEX.fullmatch(source_sha256) or not _NAME.fullmatch(snapshot):
            raise ValueError('Invalid pinned checkpoint identity')
        if type(partitions) is not int or partitions != 16:
            raise ValueError('Partition layout must be pinned')
        self.store = store
        self.source_sha = source_sha256
        self.partitions = partitions
        self.prefix = valid_key('private-source-checkpoints/native100/' + snapshot)
        self.head = self.prefix + '/head.json'
        self.segments = []
        self.state = None

    def _write_readback(self, key, body):
        key = valid_key(key)
        self.store.put(key, body)
        if self.store.get(key, len(body) + 1) != body:
            raise RuntimeError('Cloud checkpoint write/readback mismatch')

    def _lookup_exists(self, key):
        # Never turn an unauthenticated timeout/403 into "no prior checkpoint".
        if hasattr(self.store, 'objects'):
            return key in self.store.objects
        remote = self.store.prefix + '/' + key
        cmd = self.store.command(['lsjson', remote, '--stat'])
        try:
            r = subprocess.run(cmd, stdin=subprocess.DEVNULL,
                               capture_output=True, timeout=30)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError('Checkpoint existence unresolved') from exc
        if r.returncode == 0:
            return True
        # rclone exit 3 = path not found; all other statuses fail closed.
        if r.returncode == 3:
            return False
        raise RuntimeError('Checkpoint existence unresolved')

    def init(self):
        if self._lookup_exists(self.head):
            raise RuntimeError('Existing checkpoint found; select resume')
        self.segments = []
        self.state = None

    def _check_state(self, state, segments):
        if not isinstance(state, dict):
            raise ValueError('Checkpoint state malformed')
        ints = ('parts_done','source_rows','oversize','raw_body_bytes','last_rowid')
        if any(type(state.get(k)) is not int or state[k] < 0 for k in ints):
            raise ValueError('Checkpoint state counters invalid')
        if not (1 <= state['parts_done'] <= 443
                and state['source_rows'] <= 3187870
                and state['oversize'] <= state['source_rows']
                and state['last_rowid'] >= state['source_rows']):
            raise ValueError('Checkpoint state range invalid')
        p = state.get('partitions')
        if not isinstance(p, list) or len(p) != self.partitions:
            raise ValueError('Checkpoint partition count mismatch')
        total = 0
        for item in p:
            if not isinstance(item, dict):
                raise ValueError('Checkpoint partition entry malformed')
            if (type(item.get('count')) is not int or item['count'] < 0
                    or not _HEX.fullmatch(str(item.get('logical_sha256')))
                    or not isinstance(item.get('parts'), list)
                    or len(item['parts']) > 500):
                raise ValueError('Checkpoint partition metadata invalid')
            shard_count = 0
            for shard in item['parts']:
                if not isinstance(shard, dict):
                    raise ValueError('Checkpoint shard descriptor malformed')
                key = shard.get('key')
                if (not isinstance(key, str)
                        or not re.fullmatch(r'objects/[a-f0-9]{64}\.sqlite\.gz', key)
                        or not _HEX.fullmatch(str(shard.get('packed_sha256')))
                        or type(shard.get('packed_bytes')) is not int
                        or not 0 < shard['packed_bytes'] <= 64 * 1024 * 1024
                        or type(shard.get('documents')) is not int
                        or not 0 < shard['documents'] <= 512):
                    raise ValueError('Checkpoint immutable shard invalid')
                shard_count += shard['documents']
                known = getattr(self.store, 'known', None)
                if known is not None:
                    if key not in known or known[key][0] != shard['packed_bytes']:
                        raise ValueError('Checkpoint immutable shard missing from verified inventory')
            if shard_count != item['count']:
                raise ValueError('Checkpoint shard/doc accounting mismatch')
            total += item['count']
        if total + state['oversize'] != state['source_rows']:
            raise ValueError('Checkpoint searchable/oversize accounting mismatch')
        samples = state.get('first_samples')
        if not isinstance(samples, dict) or len(samples) > self.partitions:
            raise ValueError('Checkpoint sample metadata invalid')
        for k,v in samples.items():
            if (not str(k).isdigit() or not 0 <= int(k) < self.partitions
                    or not isinstance(v, dict)
                    or not isinstance(v.get('path'), str)
                    or not v['path'].startswith('/')
                    or not _HEX.fullmatch(str(v.get('body_sha256')))):
                raise ValueError('Checkpoint first sample invalid')
        if not isinstance(segments, list) or not segments:
            raise ValueError('Missing checkpoint path metadata')
        if len(segments) != (state['parts_done'] + 9)//10:
            raise ValueError('Checkpoint batch sequence incomplete')
        if sum(s.get('count', -1) for s in segments if isinstance(s, dict)) != total:
            raise ValueError('Checkpoint metadata row accounting mismatch')
        if (state.get('consistent_snapshot') is not False
                or state.get('safe_for_deletion') is not False):
            raise ValueError('Unsafe checkpoint promotion flags')

    def _segment_rows(self, segment):
        if not isinstance(segment, dict):
            raise ValueError('Bad checkpoint segment descriptor')
        key, size, sha = segment.get('key'), segment.get('bytes'), segment.get('sha256')
        if (not isinstance(key, str)
                or not re.fullmatch(re.escape(self.prefix) + r'/segments/[a-f0-9]{64}\.jsonl\.gz', key)
                or not _HEX.fullmatch(str(sha))
                or key.rsplit('/',1)[-1] != sha+'.jsonl.gz'
                or type(size) is not int or not 0 < size <= MAX_COMPRESSED):
            raise ValueError('Invalid checkpoint metadata segment identity')
        payload = self.store.get(key, size)
        if len(payload) != size or digest(payload) != sha:
            raise ValueError('Checkpoint metadata integrity mismatch')
        yield from _decode_rows(payload, count=segment['count'],
                                body_sha=sha, partitions=self.partitions)

    def restore(self, partitions):
        """Load all metadata in bounded batches; never read original source body."""
        raw_head = self.store.get(self.head, 2048)
        head = json.loads(raw_head)
        if (not isinstance(head, dict) or head.get('format') != CHECKPOINT_FORMAT
                or head.get('source_checkpoint_sha256') != self.source_sha
                or type(head.get('parts_done')) is not int
                or not _HEX.fullmatch(str(head.get('sha256')))):
            raise ValueError('Checkpoint head invalid')
        name = (self.prefix + '/manifests/' +
                str(head['parts_done']).zfill(6) + '-' + head['sha256'] + '.json')
        if head.get('manifest') != name:
            raise ValueError('Checkpoint manifest identity mismatch')
        raw = self.store.get(name, MAX_MANIFEST)
        if digest(raw) != head['sha256']:
            raise ValueError('Checkpoint manifest SHA mismatch')
        manifest = json.loads(raw)
        if (manifest.get('format') != CHECKPOINT_FORMAT
                or manifest.get('source_checkpoint_sha256') != self.source_sha
                or manifest.get('parts_done') != head['parts_done']
                or manifest.get('partitions') != self.partitions):
            raise ValueError('Checkpoint identity mismatch')
        state = manifest.get('state')
        segments = manifest.get('segments')
        self._check_state(state, segments)
        if len(partitions) != self.partitions:
            raise ValueError('Checkpoint partition layout mismatch')
        counts = [0]*self.partitions
        digests = [hashlib.sha256() for _ in range(self.partitions)]
        for segment in segments:
            for row in self._segment_rows(segment):
                pid, path, size, mtime, sha = row
                partitions[pid].paths.append((path,size,mtime,sha))
                counts[pid] += 1
                digests[pid].update(json_bytes([path,sha]))
        for i, part in enumerate(partitions):
            expected = state['partitions'][i]
            if counts[i] != expected['count'] or digests[i].hexdigest() != expected['logical_sha256']:
                raise ValueError('Checkpoint partition digest/count mismatch')
        for i, part in enumerate(partitions):
            part.count = counts[i]
            part.logical = digests[i]
            part.parts = list(state['partitions'][i]['parts'])
            if part.docs or part.pending_bytes:
                raise RuntimeError('Cannot restore into dirty partition')
        self.segments = list(segments)
        self.state = state
        return state

    def commit(self, spool, state):
        """Publish a new immutable batch then update private head, once."""
        old = self.state
        ordinal = state.get('parts_done')
        if (type(ordinal) is not int
                or ordinal > 443 or ordinal < 1
                or (old is not None and ordinal <= old['parts_done'])):
            raise ValueError('Non-monotonic source checkpoint')
        expected_next = (old['parts_done'] + 10 if old is not None else 10)
        if old is not None and old['parts_done'] == 440:
            expected_next = 443
        if ordinal != expected_next:
            raise ValueError('Checkpoint must advance one exact pinned batch')
        if any(k in state for k in ('source_root', 'read_roots', 'write_roots')):
            raise ValueError('Source checkpoint cannot grant filesystem rights')
        self._check_state(state, self.segments + [{
            'count':spool.count
        }])
        if old is not None:
            delta = state['source_rows'] - old['source_rows']
            if (delta <= 0 or state['oversize'] < old['oversize']
                    or state['last_rowid'] <= old['last_rowid']
                    or state['raw_body_bytes'] < old['raw_body_bytes']
                    or delta != spool.count + state['oversize']-old['oversize']):
                raise ValueError('Checkpoint batch row accounting mismatch')
            for previous, current in zip(old['partitions'], state['partitions']):
                old_parts = previous['parts']
                if (current['count'] < previous['count']
                        or current['parts'][:len(old_parts)] != old_parts):
                    raise ValueError('Checkpoint immutable shard prefix changed')
        elif state['source_rows'] != spool.count + state['oversize']:
            raise ValueError('First checkpoint batch accounting mismatch')
        body = spool.seal()
        sha = digest(body)
        chunk_key = self.prefix + '/segments/' + sha + '.jsonl.gz'
        self._write_readback(chunk_key, body)
        segment = {'key':chunk_key,'sha256':sha,'bytes':len(body),
                   'count':spool.count}
        all_segments = self.segments + [segment]
        manifest = {'format':CHECKPOINT_FORMAT, 'source_checkpoint_sha256':self.source_sha,
                    'parts_done':ordinal,'partitions':self.partitions,
                    'segments':all_segments,'state':state}
        packed = json_bytes(manifest)
        if len(packed) > MAX_MANIFEST:
            raise ValueError('Checkpoint manifest exceeds admission bound')
        manifest_sha = digest(packed)
        key = self.prefix + '/manifests/' + str(ordinal).zfill(6) + '-' + manifest_sha + '.json'
        self._write_readback(key, packed)
        head = json_bytes({'format':CHECKPOINT_FORMAT,
                           'source_checkpoint_sha256':self.source_sha,
                           'parts_done':ordinal, 'manifest':key,'sha256':manifest_sha})
        self._write_readback(self.head, head)
        self.segments = all_segments
        self.state = state
        spool.cleanup()
        return state
