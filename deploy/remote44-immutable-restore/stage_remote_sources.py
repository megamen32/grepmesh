#!/usr/bin/python3
"""Restore reviewed Python source to server-44 tmpfs, without any credentials."""
import base64
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys

ROOT = Path("/home/roomhacker/.local/lib/grepmesh-offload/releases/e540528-remote44-20261103")
NAMES = ("cloud_index.py", "multi_cloud.py", "oversize_support.py", "sealed_offload_reader.py")
# Immutable reviewed release inventory; no manifest+module simultaneous substitution.
MANIFEST_SHA256 = "b3026736855d2ad146197cc83da3c13131d9e073229d1e0a091526199f5f9aae"
SSH = ["ssh", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
       "-o", "ConnectTimeout=6", "-o", "ConnectionAttempts=1",
       "-o", "NumberOfPasswordPrompts=0", "roomhacker@192.168.2.5"]
REMOTE = r'''
import base64, hashlib, json, os, pathlib, stat, sys
root=pathlib.Path("/run/user/1000/grepmesh-remote-reader-staged-20261103")
if os.getuid()!=1000 or root.is_symlink():
    raise RuntimeError("Wrong principal or symlinked remote root")
# Never chmod/repair an unknown directory or change existing ACLs.
root.mkdir(mode=0o700,parents=False,exist_ok=True)
rs=root.lstat()
if (not stat.S_ISDIR(rs.st_mode) or rs.st_uid!=os.getuid()
        or rs.st_mode&0o077 or rs.st_dev!=pathlib.Path("/run/user/1000").stat().st_dev):
    raise RuntimeError("Refuse unsafe or non-tmpfs runtime root")
raw=sys.stdin.buffer.read(300001)
if len(raw)>300000: raise RuntimeError("Oversized source")
items=json.loads(raw)
names={"cloud_index.py","multi_cloud.py","oversize_support.py","sealed_offload_reader.py"}
if set(items)!=names: raise RuntimeError("Unexpected source set")
copied=0
for name, item in sorted(items.items()):
    path=root/name
    blob=base64.b64decode(item["data"],validate=True)
    if len(blob)!=item["bytes"] or hashlib.sha256(blob).hexdigest()!=item["sha256"]:
        raise RuntimeError("Source checksum mismatch")
    if path.is_symlink():raise RuntimeError("Refuse symlink")
    if path.exists():
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
        with os.fdopen(fd,"rb") as f:
            fs=os.fstat(f.fileno())
            if (not stat.S_ISREG(fs.st_mode) or fs.st_uid!=os.getuid()
                    or fs.st_mode&0o077 or fs.st_nlink not in (1,2)
                    or fs.st_size!=item["bytes"] or
                    hashlib.sha256(f.read(item["bytes"]+1)).hexdigest()!=item["sha256"]):
                raise RuntimeError("Conflicting existing source: refusing overwrite")
        if fs.st_nlink==2:
            # SIGKILL after os.link(tmp, final) but before tmp.unlink()
            # leaves a valid final file with exactly one own pending hardlink.
            # Remove ONLY that matching, private, inode-identical scratch.
            matches=[]
            for i,candidate in enumerate(root.glob(name+".pending-*")):
                if i>=32:raise RuntimeError("Too many unknown pending sources")
                cs=candidate.lstat()
                if cs.st_dev==fs.st_dev and cs.st_ino==fs.st_ino:
                    if (not stat.S_ISREG(cs.st_mode) or cs.st_uid!=os.getuid()
                            or cs.st_mode&0o077 or cs.st_nlink!=2):
                        raise RuntimeError("Untrusted pending hardlink")
                    matches.append(candidate)
            if len(matches)!=1:
                raise RuntimeError("Ambiguous pending reader source")
            matches[0].unlink()
            df=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:os.fsync(df)
            finally:os.close(df)
            verify=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
            try:
                current=os.fstat(verify)
                if (current.st_dev!=fs.st_dev or current.st_ino!=fs.st_ino
                        or current.st_nlink!=1):
                    raise RuntimeError("Reader inode changed after reconciliation")
            finally:os.close(verify)
        continue
    # A crash before publication may leave ONLY a PID-scoped temporary file;
    # the final reader path is never made visible with partial content.
    tmp=root/(name+".pending-"+str(os.getpid()))
    fd=os.open(tmp,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
    try:
        with os.fdopen(fd,"wb") as f:
            f.write(blob)
            f.flush()
            os.fsync(f.fileno())
        os.link(tmp,path,follow_symlinks=False)
        df=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(df)
        finally:os.close(df)
    finally:
        tmp.unlink(missing_ok=True)
    copied+=1
print(json.dumps({"sources_verified":4,"new_source_files":copied,"storage":"tmpfs"}))
'''

def main():
    if os.getuid() != 1000:
        raise RuntimeError("Expected roomhacker identity")
    manifest_bytes = (ROOT / "manifest.json").read_bytes()
    if hashlib.sha256(manifest_bytes).hexdigest() != MANIFEST_SHA256:
        raise RuntimeError("Pinned source manifest drift; no remote staging")
    manifest = json.loads(manifest_bytes)
    if set(manifest) != set(NAMES):
        raise RuntimeError("Pinned source manifest incomplete")
    items = {}
    for name in NAMES:
        raw = (ROOT / name).read_bytes()
        expected = manifest[name]
        if len(raw) != expected["bytes"] or hashlib.sha256(raw).hexdigest() != expected["sha256"]:
            raise RuntimeError("Source release checksum mismatch")
        items[name] = {"bytes":len(raw),"sha256":expected["sha256"],
                       "data":base64.b64encode(raw).decode("ascii")}
    command = SSH + ["python3 -B -c " + shlex.quote(REMOTE)]
    result = subprocess.run(command, input=json.dumps(items).encode(), capture_output=True, timeout=35)
    if result.returncode:
        raise RuntimeError("Authorized SSH source stage failed")
    receipt = json.loads(result.stdout)
    if receipt.get("sources_verified") != 4 or receipt.get("storage") != "tmpfs":
        raise RuntimeError("Unexpected remote source-stage receipt")
    print(json.dumps(receipt,sort_keys=True))

if __name__ == "__main__":
    main()
