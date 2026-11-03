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
SSH = ["ssh", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
       "-o", "ConnectTimeout=6", "-o", "ConnectionAttempts=1",
       "-o", "NumberOfPasswordPrompts=0", "roomhacker@192.168.2.5"]
REMOTE = r'''
import base64, hashlib, json, os, pathlib, stat, sys
root=pathlib.Path("/run/user/1000/grepmesh-remote-reader-staged-20261103")
if os.getuid()!=1000 or root.is_symlink():
    raise RuntimeError("Wrong principal or symlinked remote root")
root.mkdir(mode=0o700,parents=True,exist_ok=True)
root.chmod(0o700)
if root.stat().st_dev!=pathlib.Path("/run/user/1000").stat().st_dev:
    raise RuntimeError("Refuse persistent disk")
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
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=item["sha256"]:
            raise RuntimeError("Conflicting existing source: refusing overwrite")
        continue
    fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,"wb") as f:
        f.write(blob)
        f.flush()
        os.fsync(f.fileno())
    copied+=1
print(json.dumps({"sources_verified":4,"new_source_files":copied,"storage":"tmpfs"}))
'''

def main():
    if os.getuid() != 1000:
        raise RuntimeError("Expected roomhacker identity")
    manifest = json.loads((ROOT / "manifest.json").read_text())
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
