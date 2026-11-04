#!/usr/bin/python3
"""Fail-closed ExecCondition for the server-44 immutable replica."""
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from restore_immutable_objects import CloudStore, build_catalog

REMOTE_ROOT = "/run/user/1000/grepmesh-remote-reader-staged-20261103/store"
MAX_OBJECTS = 256
MAX_TOTAL_BYTES = 1024 * 1024 * 1024
REMOTE_CHECK = """import hashlib,json,os,stat,sys,subprocess
root=sys.argv[1]; expected=json.load(sys.stdin)
if len(expected)>256: raise SystemExit(20)
want={}; total=0
for row in expected:
 if not isinstance(row,list) or len(row)!=3: raise SystemExit(21)
 name,size,digest=row
 if not isinstance(name,str) or name.startswith('/') or '..' in name.split('/') or not isinstance(size,int) or size<0 or size>64*1024*1024 or not isinstance(digest,str) or len(digest)!=64: raise SystemExit(22)
 if name in want: raise SystemExit(23)
 total+=size
 if total>1024*1024*1024: raise SystemExit(24)
 want[name]=(size,digest)
try: st=os.stat(root,follow_symlinks=False)
except FileNotFoundError:
 print('NEEDS_RESTORE'); raise SystemExit(10)
if not stat.S_ISDIR(st.st_mode): raise SystemExit(25)
seen=set()
for base,dirs,files in os.walk(root,followlinks=False):
 for d in dirs:
  if os.path.islink(os.path.join(base,d)): raise SystemExit(26)
 for name in files:
  full=os.path.join(base,name); rel=os.path.relpath(full,root); s=os.lstat(full)
  if not stat.S_ISREG(s.st_mode) or rel not in want: raise SystemExit(27)
  size,digest=want[rel]
  if s.st_size!=size: raise SystemExit(28)
  h=hashlib.sha256(); n=0
  with open(full,'rb',buffering=0) as f:
   while True:
    b=f.read(1024*1024)
    if not b: break
    n+=len(b)
    if n>size: raise SystemExit(29)
    h.update(b)
  if n!=size or h.hexdigest()!=digest: raise SystemExit(30)
  seen.add(rel)
if seen!=set(want):
 print('NEEDS_RESTORE'); raise SystemExit(10)
env=os.environ.copy(); env['XDG_RUNTIME_DIR']='/run/user/1000'; env['DBUS_SESSION_BUS_ADDRESS']='unix:path=/run/user/1000/bus'
p=subprocess.run(['/usr/bin/systemctl','--user','is-active','--quiet','grepmesh-remote44-private-reader.service'],env=env,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=6)
if p.returncode==3:
 print('NEEDS_RESTORE'); raise SystemExit(10)
if p.returncode: raise SystemExit(32)
print('EXACT_AND_READER_ACTIVE')
"""

def state(unit):
    env=dict(os.environ,XDG_RUNTIME_DIR="/run/user/1000",DBUS_SESSION_BUS_ADDRESS="unix:path=/run/user/1000/bus")
    try:
        p=subprocess.run(["systemctl","--user","show",unit,"-p","ActiveState","--value"],capture_output=True,text=True,timeout=6,env=env)
    except (OSError,subprocess.TimeoutExpired): return None
    return p.stdout.strip() if p.returncode == 0 else None

def main():
    if os.getuid()!=1000: return 255
    source=state("grepmesh-native100-private-search-resume-20261101.service")
    transfer=state("grepmesh-offload44-immutable-transfer-20261103.service")
    if source not in ("inactive","failed") or transfer not in ("inactive","failed"): return 1
    try:
        scopes=json.loads(Path("/home/roomhacker/.config/grepmesh-cloud-legacy44.json").read_text())
        # The volatile cache is created by ExecStart, after this condition.
        # It cannot be required for the first restoration following reboot.
        _,_,catalog=build_catalog(CloudStore(),scopes,use_cache=False)
        expected=[[key,size,sha] for key,(size,sha,_part,_metadata) in catalog]
        if not expected or len(expected)>MAX_OBJECTS or sum(x[1] for x in expected)>MAX_TOTAL_BYTES: return 255
        payload=json.dumps(expected,separators=(",",":")).encode()
        remote_command="python3 -c "+shlex.quote(REMOTE_CHECK)+" "+shlex.quote(REMOTE_ROOT)
        ssh=["ssh","-T","-o","BatchMode=yes","-o","StrictHostKeyChecking=yes","-o","ConnectTimeout=5","-o","ConnectionAttempts=1","-o","ServerAliveInterval=15","-o","ServerAliveCountMax=3","roomhacker@192.168.2.5",remote_command]
        p=subprocess.run(ssh,input=payload,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=1800)
        if p.returncode==0 and p.stdout.strip()==b"EXACT_AND_READER_ACTIVE": return 1
        if p.returncode==10 and p.stdout.strip()==b"NEEDS_RESTORE": return 0
        return 255
    except (OSError,subprocess.TimeoutExpired,ValueError,KeyError,TypeError,RuntimeError,json.JSONDecodeError):
        return 255

if __name__=="__main__": sys.exit(main())
