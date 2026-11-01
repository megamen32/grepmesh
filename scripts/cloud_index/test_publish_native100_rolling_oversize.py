#!/usr/bin/env python3
"""Small no-network integration gate for native100 cold-history publisher."""
import gzip
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import publish_native100_rolling_oversize as m
from cloud_index import MemoryStore, MAX_DOCUMENT, digest, json_bytes
from oversize_support import OversizeIndex, publish_oversize, oversize_pointer_key

# Tiny complete archive fixture, including an >1MiB deleted-history body.
m.EXPECTED_ROWS = 3
m.EXPECTED_OVERSIZE = 1
body = "α" * (MAX_DOCUMENT // 2 + 1)
rows = [
    [1, "/test/plain.txt", "abc"],
    [5, "/test/large.txt", body],
    [6, "/test/empty.txt", ""],
]
packed = gzip.compress(b"".join(json_bytes(row) + b"\n" for row in rows))
store = MemoryStore()
store.put("test/part-0.gz", packed)
checkpoint = {
    "chunks": [{
        "key": "test/part-0.gz", "compressed_bytes": len(packed),
        "sha256": digest(packed), "rows": 3, "last_rowid": 6,
    }],
    "last_rowid": 6,
}
audit = {"oversize_documents": 1, "total_text_body_bytes": len(body.encode()) + 3}
stats = {"rows": 0, "text_bytes": 0, "oversize": 0, "oversize_bytes": 0}
docs = list(m.source_docs(store, checkpoint, audit, stats))
assert docs == [{"path": "/test/large.txt", "body": body}]
assert stats["rows"] == 3
assert stats["oversize_bytes"] == len(body.encode())
result = publish_oversize(store, "server-100", "unit-test-provisional", docs)
index = OversizeIndex(store, "server-100", "unit-test-provisional")
index.refresh()
assert index.manifest["documents"] == result["documents"] == 1
assert index.by_path["/test/large.txt"]["body_sha256"] == digest(body.encode())

# Failed provenance verification may upload content-addressed orphan chunks,
# but MUST NEVER publish an index manifest/latest pointer.
bad = dict(checkpoint)
bad["chunks"] = [dict(checkpoint["chunks"][0], rows=4)]
rejected_store = MemoryStore()
rejected_store.put("test/part-0.gz", packed)
try:
    publish_oversize(
        rejected_store, "server-100", "unit-test-rejected",
        m.source_docs(rejected_store, bad, audit, {
            "rows": 0, "text_bytes": 0, "oversize": 0, "oversize_bytes": 0,
        }),
    )
except RuntimeError:
    pass
else:
    raise AssertionError("Bad historical archive was published")
assert oversize_pointer_key("server-100", "unit-test-rejected") not in rejected_store.objects
print(json.dumps({"validated": True, "sample_rows": 3, "cold_docs": 1, "invalid_source_pointer_published": False}))
