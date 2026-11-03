#!/usr/bin/python3
"""Sealed offload reader: immutable objects already mirrored to tmpfs.
No rclone, no publishing, no production routing, and no synthetic ACL signer.
Requires a REAL signer-bound deployment environment before starting.
"""
import os, sys, json, re, threading
from pathlib import Path
STAGE=Path("/run/user/1000/grepmesh-remote-reader-staged-20261103")
sys.path.insert(0,str(STAGE))
from cloud_index import serve, ensure_tmpfs, valid_key, acl_secret
from multi_cloud import MultiReader

class ImmutableTmpfsStore:
    def __init__(self):
        self.root=STAGE/"store"
        ensure_tmpfs(self.root)
        self.gets=0
        self.bytes_in=0
        self.puts=0
        self.lock=threading.Lock()
    def get(self,key,limit=64*1024*1024):
        valid_key(key)
        if not isinstance(limit,int) or limit<0 or limit>64*1024*1024:
            raise ValueError("Out of bounds immutable read")
        p=self.root.joinpath(*key.split('/'))
        for parent in p.parents:
            if parent==self.root.parent:break
            if parent.is_symlink():raise PermissionError("Symlink in immutable store")
        fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
        try:
            if os.fstat(fd).st_dev!=os.stat('/run/user/1000').st_dev:
                raise RuntimeError("Persistent disk object forbidden")
            with os.fdopen(fd,'rb') as f:
                fd=-1
                data=f.read(limit+1)
        finally:
            if fd>=0:os.close(fd)
        if len(data)>limit:raise ValueError("Immutable object exceeds bound")
        with self.lock:
            self.gets+=1
            self.bytes_in+=len(data)
        return data
    def put(self,*_args,**_kwargs):
        raise PermissionError("Read-only immutable mirror; cloud writes forbidden")

def main():
    if os.environ.get("GREPMESH_CLOUD_REQUIRE_VERIFIED_ACL")!="1":
        raise RuntimeError("Refusing private reader without verified ACL mode")
    if not acl_secret() or len(acl_secret())<32:
        raise RuntimeError("No real signer-bound ACL secret; private reader disabled")
    cache=STAGE/"cache"
    ensure_tmpfs(cache)
    scopes=json.load(open("/home/roomhacker/.config/grepmesh-private44-historical-scopes.json"))
    if len(scopes)!=8:raise RuntimeError("Wrong scope count")
    reader=MultiReader(ImmutableTmpfsStore(),cache,scopes,512)
    serve(reader,19440)
if __name__=="__main__":main()

