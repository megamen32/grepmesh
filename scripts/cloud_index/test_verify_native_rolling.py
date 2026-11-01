#!/usr/bin/env python3
"""Synthetic, offline checkpoint verifier tests; no source SQLite or network."""
import gzip
import hashlib
import json

from verify_native_rolling import verify


SNAP = "20261031T233108Z-5732b979"
BASE = f"archives/server-100/rolling/{SNAP}"
SOURCE = "/home/roomhacker/.cache/grepmesh/server-100-65415a79c0e91f4b.sqlite3"


class Store:
    def __init__(self):
        self.data = {}
    def put(self, key, raw):
        self.data[key] = raw
    def get(self, key, limit):
        value = self.data[key]
        if len(value) > limit:
            raise ValueError("bound exceeded")
        return value


def fixture():
    store = Store()
    data = [
        [(1, "/first/previously-deleted.md", "Кириллица\nexample"), (3, "/x", "two")],
        [(10, "/y", "line 1\nline 2")],
    ]
    parts = []
    for i, rows in enumerate(data):
        raw = b"".join((json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n").encode() for row in rows)
        packed = gzip.compress(raw, mtime=0)
        sha = hashlib.sha256(packed).hexdigest()
        key = f"{BASE}/parts/{i:06d}-{sha}.jsonl.gz"
        parts.append({"key": key, "sha256": sha, "compressed_bytes": len(packed),
                      "raw_bytes": len(raw), "rows": len(rows),
                      "first_rowid": rows[0][0], "last_rowid": rows[-1][0]})
        store.put(key, packed)
    state = {"format":"grepmesh-rolling-text-v1", "source":SOURCE, "snapshot":SNAP,
             "upper_rowid":12, "last_rowid":10, "rows":3,
             "raw_bytes":sum(p["raw_bytes"] for p in parts), "chunks":parts,
             "consistent_snapshot":False, "safe_for_deletion":False,
             "finished":True}
    store.put(BASE + "/progress.json", json.dumps(state).encode())
    return store, state


def must_fail(fn):
    try:
        fn()
    except (ValueError, KeyError, OSError, EOFError):
        pass
    else:
        raise AssertionError("Verifier accepted an invalid checkpoint")


def run():
    s, state = fixture()
    result = verify(s, SNAP)
    assert result["checkpoint_rows"] == 3
    assert result["all_chunks_replayed"] and result["verified_rows"] == 3
    assert result["consistent_snapshot"] is False
    assert result["safe_for_deletion"] is False
    one = verify(s, SNAP, [1])
    assert one["verified_chunks"] == 1 and not one["all_chunks_replayed"]
    assert one["verified_rows"] == 1
    must_fail(lambda: verify(s, SNAP, [-1]))
    must_fail(lambda: verify(s, "invalid", [0]))
    old = s.data[state["chunks"][0]["key"]]
    s.data[state["chunks"][0]["key"]] = old[:-1] + b"X"
    must_fail(lambda: verify(s, SNAP))
    s.data[state["chunks"][0]["key"]] = old
    checkpoint = BASE + "/progress.json"
    for key, val in [("safe_for_deletion", True), ("consistent_snapshot", True),
                     ("last_rowid", 11), ("raw_bytes", 1)]:
        tampered = dict(state, **{key:val})
        s.put(checkpoint, json.dumps(tampered).encode())
        must_fail(lambda: verify(s, SNAP, [1]))
    s.put(checkpoint, json.dumps(state).encode())
    tampered = dict(state, chunks=[state["chunks"][0], state["chunks"][0]])
    s.put(checkpoint, json.dumps(tampered).encode())
    must_fail(lambda: verify(s, SNAP, [0]))
    # Every catch-up chunk must be strictly above a completed immutable base.
    s.put(checkpoint,json.dumps(state).encode())
    delta='20261101T044000Z-fedca987'
    delta_prefix=f'archives/server-100/rolling/{delta}'
    raw=(json.dumps([13,'/new','just added'])+'\n').encode()
    packed=gzip.compress(raw,mtime=0)
    sha=hashlib.sha256(packed).hexdigest()
    part=dict(key=f'{delta_prefix}/parts/000000-{sha}.jsonl.gz',
              sha256=sha,compressed_bytes=len(packed),raw_bytes=len(raw),
              rows=1,first_rowid=13,last_rowid=13)
    s.put(part['key'],packed)
    child=dict(state,snapshot=delta,lower_rowid=12,base_snapshot=SNAP,
               capture_mode='provisional_high_rowid_only',upper_rowid=13,
               last_rowid=13,rows=1,raw_bytes=len(raw),chunks=[part])
    key=delta_prefix+'/progress.json'
    s.put(key,json.dumps(child).encode())
    checked=verify(s,delta)
    assert checked['verified_rows']==1 and checked['lower_rowid']==12
    assert checked['base_snapshot']==SNAP
    s.put(key,json.dumps(dict(child,lower_rowid=11)).encode())
    must_fail(lambda: verify(s,delta))
    s.put(key,json.dumps(dict(child,safe_for_deletion=True)).encode())
    must_fail(lambda: verify(s,delta))
    print(json.dumps({"passed":True, "checks":14, "all_chunks_replayed":True,
                      "safe_for_deletion":False}))


if __name__ == "__main__":
    run()
