"""Durable pending cloud object patches; never a searchable native index.

Only a completed, fsynced packet can be resumed. Acknowledgement comes after
upload AND reader activation. Unchanged content-addressed objects are omitted.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil

from cloud_index import safe_scope, valid_key, json_bytes


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def save(path, value):
    temporary = path.with_name(path.name + '.new')
    with temporary.open('wb') as out:
        out.write(json_bytes(value))
        out.flush()
        os.fsync(out.fileno())
    os.replace(temporary, path)
    sync_dir(path.parent)


class Outbox:
    def __init__(self, root, host, max_bytes=8 * 1024**3):
        self.root = Path(root) / safe_scope(host)
        self.host, self.max_bytes = host, max_bytes
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.root.chmod(0o700)
        self.pending = self.root / 'pending'
        self.acknowledged = self.root / 'acknowledged'
        # Interrupted cleanup must never look like a damaged pending upload.
        if self.acknowledged.exists():
            shutil.rmtree(self.acknowledged)
            sync_dir(self.root)

    def load(self):
        if not self.pending.exists():
            return None
        packet = json.loads((self.pending / 'packet.json').read_text())
        if packet['host'] != self.host:
            raise ValueError('Outbox host mismatch')
        return packet

    def prepare(self, staging, entries, receipt):
        if self.pending.exists():
            raise RuntimeError('Drain the previous outbox packet before collecting another')
        state = self.root / 'published-objects.json'
        known = json.loads(state.read_text()) if state.exists() else {}
        partial = self.root / 'preparing'
        # A partial packet was never admitted or published; source data remains.
        if partial.exists():
            shutil.rmtree(partial)
        partial.mkdir(mode=0o700)
        payload = partial / 'payload'
        payload.mkdir()
        objects, changed = {}, {}
        total = 0
        try:
            for source in sorted(Path(staging).rglob('*')):
                if source.is_symlink():
                    raise ValueError('Outbox source must not contain symlinks')
                if not source.is_file():
                    continue
                key = valid_key(source.relative_to(staging).as_posix())
                h = hashlib.sha256()
                with source.open('rb') as inp:
                    for data in iter(lambda: inp.read(1024 * 1024), b''):
                        h.update(data)
                sha = h.hexdigest()
                objects[key] = sha
                if known.get(key) == sha:
                    continue
                size = source.stat().st_size
                total += size
                if total > self.max_bytes or shutil.disk_usage(self.root).free < size + 1024**3:
                    raise RuntimeError('Outbox capacity reached; no pending data discarded')
                target = payload / key
                target.parent.mkdir(parents=True, exist_ok=True)
                with source.open('rb') as inp, target.open('xb') as out:
                    shutil.copyfileobj(inp, out, 1024 * 1024)
                    out.flush()
                    os.fsync(out.fileno())
                changed[key] = sha
            packet = {'version': 1, 'host': self.host, 'entries': entries,
                      'receipt': receipt, 'objects': objects, 'changed': changed,
                      'bytes': total}
            save(partial / 'packet.json', packet)
            for directory in sorted((p for p in partial.rglob('*') if p.is_dir()),
                                    key=lambda p: len(p.parts), reverse=True):
                sync_dir(directory)
            sync_dir(partial)
            os.replace(partial, self.pending)
            sync_dir(self.root)
            return packet
        except BaseException:
            # A completed pending packet is never removed by a failed prepare.
            if partial.exists():
                shutil.rmtree(partial)
            raise

    def verify(self, packet):
        for key, expected in packet['changed'].items():
            path = self.pending / 'payload' / valid_key(key)
            h = hashlib.sha256()
            with path.open('rb') as inp:
                for data in iter(lambda: inp.read(1024 * 1024), b''):
                    h.update(data)
            if h.hexdigest() != expected:
                raise ValueError('Pending outbox object is incomplete or corrupt')

    def acknowledge(self, generation):
        packet = self.load()
        if packet is None:
            return
        if packet['receipt']['generation'] != generation:
            raise ValueError('Cannot acknowledge a different pending generation')
        save(self.root / 'published-objects.json', packet['objects'])
        os.replace(self.pending, self.acknowledged)
        sync_dir(self.root)
        shutil.rmtree(self.acknowledged)
        sync_dir(self.root)
