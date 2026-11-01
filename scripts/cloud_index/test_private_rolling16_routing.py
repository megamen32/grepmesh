#!/usr/bin/env python3
"""16 private historical bucket routing and ACL regression, entirely in RAM."""
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from cloud_index import (ensure_tmpfs, MemoryStore, Publisher,
                         document, json_bytes)
from multi_cloud import MultiReader

ROOT=Path("/run/user")/str(os.getuid())/"grepmesh-private-rolling-tests"
ensure_tmpfs(ROOT)
with tempfile.TemporaryDirectory(dir=ROOT,prefix="buckets-") as temp:
    store=MemoryStore()
    publisher=Publisher(store, temp, 64*1024)
    scopes=[]
    names={}
    for i in range(16):
        j=0
        while True:
            path="/private/history/file-%02d-%d.txt"%(i,j)
            if hashlib.sha256(path.encode()).digest()[0]%16==i:
                break
            j+=1
        names[i]=path
        scope="rolling100-20261031t233108z-5732b979-%02d"%i
        scopes.append({"scope":scope,"source_host":"server-100",
                       "source_root":"/"})
        body="private bucket %d keywordabc"%i
        d=document(path,body)
        h=hashlib.sha256()
        h.update(json_bytes([path,d["sha256"]]))
        records=[
            {"kind":"begin","format":1,"source_host":"server-100",
             "source_root":"/","policy":"private test; historical ACL mandatory"},
            d,
            {"kind":"end","count":1,"logical_sha256":h.hexdigest(),
             "skipped":{},"complete_for_policy":True},
        ]
        publisher.publish(scope,io.StringIO("".join(
            json.dumps(x,ensure_ascii=False)+"\n" for x in records)))
    reader=MultiReader(store,temp,scopes,cache_mib=1024)
    reader.refresh()
    assert reader.manifest["documents"]==16
    for i,path in names.items():
        got=reader.read_text(path,1,1,authorized_roots=["/private/history"])
        assert got["chunks"][0]["lines"][0]["text"]=="private bucket %d keywordabc"%i
        try:reader.read_text(path,1,1,authorized_roots=["/other"])
        except PermissionError:pass
        else:raise AssertionError("16-way read bypassed ACL")
    find=reader.query("keywordabc",limit=20,authorized_roots=["/private/history"],
                      deadline_seconds=25)
    assert len(find["results"])==16 and not find["partial"] and not find["truncated"],find
    deny=reader.query("keywordabc",limit=20,authorized_roots=["/deny"],
                      deadline_seconds=25)
    assert not deny["results"],deny
    glob=reader.query("*file-*.txt",glob=True,limit=20,
                      authorized_roots=["/private/history"])
    assert len(glob["results"])==16 and not glob["partial"],glob
    print(json.dumps({"event":"16_bucket_private_route_acl_test_passed",
                     "checks":["all_16_historical_read_text",
                               "all_16_acl_denies","literal_across_16",
                               "deny_all_literal","path_glob_across_16"],
                     "documents":16}))
