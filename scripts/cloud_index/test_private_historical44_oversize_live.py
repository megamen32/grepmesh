#!/usr/bin/env python3
"""Bounded private historical-oversize acceptance; never publishes or deletes."""
from __future__ import annotations
import fnmatch
import json
import os
import re
import time
from pathlib import Path

from cloud_index import CloudStore, ensure_tmpfs, compile_bounded_regex
from multi_cloud import MultiReader

ROOT = Path("/run/user/1000/grepmesh-historical44-oversize-acceptance")
CONF = Path("/home/roomhacker/.config/grepmesh-private44-historical-scopes.json")
RECEIPT = Path("/home/roomhacker/.local/state/grepmesh-historical44-oversize-acceptance-20261101.json")
SNAPSHOT = "20261031T214414Z-ae84d605"

def main():
    os.umask(0o077)
    ensure_tmpfs(ROOT)
    entries=json.loads(CONF.read_text())
    expected=[{"scope":f"legacy-server44-{i}","source_host":"server-44","source_root":"/"} for i in range(8)]
    assert len(entries)==8
    assert [{k:entry[k] for k in ("scope","source_host","source_root")} for entry in entries]==expected
    assert [entry.get("oversize") for entry in entries]==[{"host":"server-44","snapshot":SNAPSHOT}]+[None]*7
    federated=MultiReader(CloudStore(),ROOT,entries,cache_mib=512)
    manifest=federated.refresh()
    assert manifest["documents"]==849721
    assert len(manifest.get("oversize_entries",[]))==1
    entry=federated.oversize[0]
    index=federated._oversize_index(entry)
    index.refresh()
    assert len(index.by_path)==75, "Cold oversized archive must be exact"
    entry["index"]=index
    records=sorted(index.by_path.values(), key=lambda row: (row["chunks"][0]["packed_bytes"],row["path"]))
    records=[r for r in records if r["line_count"]>=2 and not any(c in r["path"] for c in "*?[]")]
    assert records,"No bounded oversize sample available"
    sample=None
    for row in records[:5]:
        path=row["path"]
        try:
            index.read_text(path,1,1,authorized_roots=["/definitely-nonmatching-GrepMesh-root"],deadline_seconds=2)
        except PermissionError:
            pass
        else:
            raise AssertionError("Denied oversize read accepted")
        result=index.read_text(path,1,1,authorized_roots=[path],deadline_seconds=120)
        assert result["body_sha256"]==row["body_sha256"] and result["source_host"]=="server-44"
        lines=result["chunks"][0]["lines"]
        if not lines:
            continue
        word=re.search(r"(?u)[^\W_]{6,24}",lines[0]["text"])
        if word:
            sample=(row,word.group())
            break
    assert sample is not None,"No representative historical line"
    row,needle=sample
    path=row["path"]
    federated_read=federated.read_text(path,1,1,authorized_roots=[path],deadline_seconds=25)
    assert federated_read["body_sha256"]==row["body_sha256"]
    query_beg=time.monotonic()
    plain,plain_warnings=federated._oversize_query_hits(entry,needle,1,False,True,[path],[path],query_beg+180)
    assert len(plain)==1 and not plain_warnings,"Oversize literal query did not find the admitted history"
    regex,regex_warnings=federated._oversize_query_hits(entry,re.escape(needle),1,False,True,[path],[path],time.monotonic()+180,regex=True)
    assert len(regex)==1 and not regex_warnings,"Oversize regex query did not find the admitted history"
    denial,denial_warnings=federated._oversize_query_hits(entry,needle,1,False,True,[path],["/definitely-nonmatching-GrepMesh-root"],time.monotonic()+15)
    assert not denial and not denial_warnings,"Oversize search leaks a denied path"
    receipt={"snapshot":SNAPSHOT,"fts_documents":manifest["documents"],"oversize_documents":len(index.by_path),
            "read_text_parity":True,"oversize_literal":True,"oversize_regex":True,
            "unauthorized_read_denied":True,"unauthorized_search_denied":True,
            "actual_gptadmin_verified_fs_acl_accepted":False,"consistent_snapshot":False,
            "production_route_switched":False,"safe_for_deletion":False,"reclaimed_ssd_bytes":0}
    tmp=RECEIPT.with_suffix(".next")
    with tmp.open("x") as f:
        json.dump(receipt,f,sort_keys=True);f.write("\n");f.flush();os.fsync(f.fileno())
    tmp.replace(RECEIPT)
    print("PRIVATE_HISTORICAL_OVERSIZE_ACCEPTANCE_PASS",json.dumps(receipt,sort_keys=True),flush=True)

if __name__=="__main__":
    main()
