#!/usr/bin/python3
"""Bounded historical immutable cloud shard offload. No index builders or uploads."""
import os, sys, json, hashlib, re, time, shlex, subprocess, stat
from pathlib import Path
sys.path.insert(0, "/home/roomhacker/.local/lib/grepmesh-offload/releases/e540528-remote44-20261103")
from cloud_index import CloudStore, digest, valid_key, MAX_MANIFEST, MAX_RAW_SHARD, ensure_tmpfs
LOCAL=Path("/run/user/1000/grepmesh-historical44-object-offload-20261103")
LOCAL_META=LOCAL/"metadata"
REMOTE="roomhacker@192.168.2.5"
REMOTE_ROOT="/run/user/1000/grepmesh-remote-reader-staged-20261103/store"
PROGRESS=LOCAL/"progress.json"
SSH=["ssh","-T","-o","BatchMode=yes","-o","StrictHostKeyChecking=yes","-o","ConnectTimeout=6","-o","ConnectionAttempts=1","-o","NumberOfPasswordPrompts=0","-o","ServerAliveInterval=10",REMOTE]
UNIT="grepmesh-native100-private-search-resume-20261101.service"
REMOTE_SCRIPT=r"""
import os,sys,re,hashlib,stat
from pathlib import Path
root=Path('/run/user/1000/grepmesh-remote-reader-staged-20261103/store')
key=sys.argv[1]; size=int(sys.argv[2]); sha=sys.argv[3]; mode=sys.argv[4]
if (os.getuid()!=1000 or not re.fullmatch(r'[A-Za-z0-9_./-]+',key)
    or '..' in Path(key).parts or key.startswith('/')
    or not re.fullmatch(r'[0-9a-f]{64}',sha) or not 0<=size<=64*1024*1024
    or mode not in ('check','put')):
 raise ValueError('Unapproved private object descriptor')
user_root=Path('/run/user/1000')
mounted=user_root.stat().st_dev
def trusted_dir(p,create=False):
 if create:
  try:p.mkdir(mode=0o700)
  except FileExistsError:pass
 s=p.lstat()
 if (not stat.S_ISDIR(s.st_mode) or s.st_uid!=os.getuid()
     or s.st_mode&0o077 or s.st_dev!=mounted):
  raise RuntimeError('Refuse untrusted source directory')
trusted_dir(root.parent)
trusted_dir(root,True)
parts=Path(key).parts
parent=root
for segment in parts[:-1]:
 parent=parent/segment
 trusted_dir(parent,True)
path=parent/parts[-1]
if path.is_symlink():raise RuntimeError('Refuse symlink object')
def dirsync():
 fd=os.open(parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
 try:os.fsync(fd)
 finally:os.close(fd)
def verified():
 try:s=path.lstat()
 except FileNotFoundError:return False
 if (not stat.S_ISREG(s.st_mode) or s.st_uid!=os.getuid()
     or s.st_mode&0o077 or s.st_nlink not in (1,2) or s.st_size!=size):
  raise RuntimeError('Unsafe existing immutable object')
 fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
 try:
  with os.fdopen(fd,'rb') as f:
   live=os.fstat(f.fileno())
   if (live.st_dev!=s.st_dev or live.st_ino!=s.st_ino
       or live.st_nlink!=s.st_nlink):
    raise RuntimeError('Immutable object inode drift')
   h=hashlib.sha256();total=0
   while True:
    chunk=f.read(1024*1024)
    if not chunk:break
    total+=len(chunk)
    if total>size:raise RuntimeError('Immutable object larger than pinned')
    h.update(chunk)
   if total!=size or h.hexdigest()!=sha:
    raise RuntimeError('Existing immutable object digest mismatch')
 finally:pass
 if s.st_nlink==2:
  # Recover only our identical hardlink left after SIGKILL after publication.
  matches=[]
  for i,candidate in enumerate(parent.glob(path.name+'.partial.*')):
   if i>=32:raise RuntimeError('Ambiguous immutable pending sources')
   cs=candidate.lstat()
   if cs.st_dev==s.st_dev and cs.st_ino==s.st_ino:
    if (not stat.S_ISREG(cs.st_mode) or cs.st_uid!=os.getuid()
        or cs.st_mode&0o077 or cs.st_nlink!=2):
     raise RuntimeError('Untrusted immutable pending source')
    matches.append(candidate)
  if len(matches)!=1:raise RuntimeError('Ambiguous immutable object hardlink')
  matches[0].unlink();dirsync()
  end=path.lstat()
  if end.st_ino!=s.st_ino or end.st_dev!=s.st_dev or end.st_nlink!=1:
   raise RuntimeError('Immutable inode changed during recovery')
 return True
if mode=='check':
 if verified():print('PRESENT')
 else:print('MISSING')
 sys.exit(0)
if verified():raise RuntimeError('Refuse overwrite of immutable object')
tmp=path.with_name(path.name+'.partial.'+str(os.getpid()))
fd=os.open(tmp,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
try:
 h=hashlib.sha256();n=0
 with os.fdopen(fd,'wb') as f:
  while n<size:
   b=sys.stdin.buffer.read(min(1024*1024,size-n))
   if not b:break
   n+=len(b);h.update(b);f.write(b)
  f.flush();os.fsync(f.fileno())
 if n!=size or h.hexdigest()!=sha:
  raise RuntimeError('Transferred immutable object integrity mismatch')
 # Exclusive publication: never replace another writer's final file.
 os.link(tmp,path,follow_symlinks=False)
 dirsync()
 print('COPIED')
finally:
 tmp.unlink(missing_ok=True)

"""
def writejson(path,d):
 tmp=path.with_name(path.name+'.partial')
 with open(tmp,'w') as f:json.dump(d,f,sort_keys=True,indent=2)
 os.chmod(tmp,0o600)
 os.replace(tmp,path)
def remote_call(key,size,sha,mode,data=None):
 valid_key(key)
 cmd=SSH+[shlex.join(["python3","-c",REMOTE_SCRIPT,key,str(size),sha,mode])]
 p=subprocess.run(cmd,input=data,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=90)
 if p.returncode:
  raise RuntimeError("Remote immutable object transfer failed ("+mode+", exit="+str(p.returncode)+")")
 answer=p.stdout.decode("utf-8","replace").strip()
 if answer not in ("PRESENT","MISSING","COPIED"):raise RuntimeError("Unexpected remote receipt")
 return answer
def _metadata_private_dir(path,create=False):
 if create:
  try:path.mkdir(mode=0o700)
  except FileExistsError:pass
 s=path.lstat()
 if (not stat.S_ISDIR(s.st_mode) or s.st_uid!=os.getuid()
     or s.st_mode&0o077 or s.st_dev!=Path('/run/user/1000').stat().st_dev):
  raise RuntimeError("Untrusted private metadata directory")

def _metadata_dirsync(path):
 fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
 try:os.fsync(fd)
 finally:os.close(fd)

def cached(key,store,limit=MAX_MANIFEST):
 valid_key(key)
 _metadata_private_dir(LOCAL_META)
 parent=LOCAL_META
 parts=Path(key).parts
 for component in parts[:-1]:
  parent=parent/component
  _metadata_private_dir(parent,create=True)
 p=parent/parts[-1]
 try:s=p.lstat()
 except FileNotFoundError:s=None
 if s is not None:
  if (not stat.S_ISREG(s.st_mode) or s.st_uid!=os.getuid()
      or s.st_mode&0o077 or s.st_nlink not in (1,2) or s.st_size>limit):
   raise RuntimeError("Untrusted cached metadata inode")
  fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
  with os.fdopen(fd,'rb') as f:
   live=os.fstat(f.fileno())
   if live.st_dev!=s.st_dev or live.st_ino!=s.st_ino or live.st_nlink!=s.st_nlink:
    raise RuntimeError("Cached metadata inode changed")
   b=f.read(limit+1)
  if len(b)>limit:raise RuntimeError("Cached metadata too large")
  if s.st_nlink==2:
   # Reconcile only our exact post-link pre-unlink interrupted publication.
   matches=[]
   for i,candidate in enumerate(parent.glob(p.name+'.pending-*')):
    if i>=32:raise RuntimeError("Ambiguous metadata scratch")
    cs=candidate.lstat()
    if cs.st_dev==s.st_dev and cs.st_ino==s.st_ino:
     if (not stat.S_ISREG(cs.st_mode) or cs.st_uid!=os.getuid()
         or cs.st_mode&0o077 or cs.st_nlink!=2):
      raise RuntimeError("Unsafe pending metadata hardlink")
     matches.append(candidate)
   if len(matches)!=1:raise RuntimeError("Unknown metadata hardlink")
   matches[0].unlink()
   _metadata_dirsync(parent)
   end=p.lstat()
   if end.st_dev!=s.st_dev or end.st_ino!=s.st_ino or end.st_nlink!=1:
    raise RuntimeError("Metadata inode changed after recovery")
  return b
 b=store.get(key,limit)
 if not isinstance(b,bytes) or len(b)>limit:
  raise RuntimeError("Cloud metadata exceeds bound")
 # Never expose a partial final cache file, even after SIGKILL.
 tmp=p.with_name(p.name+'.pending-'+str(os.getpid()))
 fd=os.open(tmp,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
 try:
  with os.fdopen(fd,'wb') as f:
   f.write(b);f.flush();os.fsync(f.fileno())
  os.link(tmp,p,follow_symlinks=False)
  _metadata_dirsync(parent)
 finally:
  tmp.unlink(missing_ok=True)
 return b
def builder_quiet():
 env=dict(os.environ)
 env["XDG_RUNTIME_DIR"]="/run/user/1000"
 env["DBUS_SESSION_BUS_ADDRESS"]="unix:path=/run/user/1000/bus"
 p=subprocess.run(["systemctl","--user","show",UNIT,"-p","ActiveState","--value"],
                  stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=5,env=env,text=True)
 if p.returncode!=0 or p.stdout.strip() not in ("failed","inactive"):
  raise RuntimeError("Sole builder active/unknown; offload suspended")
def build_catalog(store,scopes,use_cache=True):
 """Return the exact immutable object descriptors used by restore and guard."""
 read_metadata=(lambda key,limit:cached(key,store,limit)) if use_cache else store.get
 if len(scopes)!=8:raise RuntimeError("Unexpected historical scope count")
 objects={};docs=0;parts=0
 for entry in scopes:
  scope=entry['scope']
  pkey='scopes/'+scope+'/latest.json'
  pb=read_metadata(pkey,65536)
  pointer=json.loads(pb)
  mkey=valid_key(pointer['manifest'])
  mb=read_metadata(mkey,MAX_MANIFEST)
  if digest(mb)!=pointer['sha256']:raise RuntimeError("Manifest hash mismatch")
  m=json.loads(mb)
  if (m['scope']!=scope or m['source_host']!='server-44' or m['source_root']!='/'
      or not m['complete_for_policy'] or m['documents']!=sum(x['documents'] for x in m['parts'])):
   raise RuntimeError("Unexpected manifest accounting")
  docs+=m['documents'];parts+=len(m['parts'])
  objects[pkey]=(len(pb),digest(pb),None,pb)
  objects[mkey]=(len(mb),digest(mb),None,mb)
  for p in m['parts']+([m['path_index']] if m.get('path_index') else []):
   key=valid_key(p['key']); size=p['packed_bytes'];sha=p['packed_sha256']
   if p['key']!='objects/'+p['sha256']+'.sqlite.gz' or size>MAX_RAW_SHARD or p['raw_bytes']>MAX_RAW_SHARD:
    raise RuntimeError("Invalid shard descriptor")
   old=objects.get(key)
   current=(size,sha,p,None)
   if old and old[:2]!=current[:2]:raise RuntimeError("Conflicting object descriptor")
   objects[key]=current
 if docs!=849721 or parts!=141:raise RuntimeError("Historical manifest parity mismatch")
 selected=sorted(objects.items(),key=lambda kv:(kv[1][2] is not None,kv[0]))
 return docs,parts,selected

def main():
 ensure_tmpfs(LOCAL)
 LOCAL_META.mkdir(mode=0o700,parents=True,exist_ok=True)
 builder_quiet()
 store=CloudStore()
 scopes=json.load(open("/home/roomhacker/.config/grepmesh-cloud-legacy44.json"))
 docs,parts,selected=build_catalog(store,scopes)
 progress={'state':'in_progress','scopes':8,'documents':docs,'shards':parts,'expected_objects':len(selected),'expected_packed_bytes':sum(v[0] for k,v in selected if v[2] is not None),'copied':0,'already_verified':0,'bytes_copied':0,'started_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}
 writejson(PROGRESS,progress)
 max_shards=int(sys.argv[1]) if len(sys.argv)>1 else 0
 processed_shards=0
 for i,(key,(size,sha,part,metadata)) in enumerate(selected):
  if i%10==0:builder_quiet()
  status=remote_call(key,size,sha,'check')
  if status=="PRESENT":
   progress['already_verified']+=1
   continue
  if part is not None:
   if max_shards and processed_shards>=max_shards:continue
   builder_quiet()
   payload=store.get(key,size)
   if len(payload)!=size or digest(payload)!=sha:raise RuntimeError("Cloud shard digest mismatch")
   processed_shards+=1
  else:payload=metadata
  if remote_call(key,size,sha,'put',payload)!='COPIED':raise RuntimeError("Remote put not completed")
  progress['copied']+=1
  progress['bytes_copied']+=len(payload)
  if i%5==0:writejson(PROGRESS,progress)
 progress['state']='canary_completed' if max_shards else 'completed'
 progress['finished_utc']=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
 progress['cloud_reads']=store.gets;progress['cloud_bytes_fetched']=store.bytes_in
 writejson(PROGRESS,progress)
 print(json.dumps(progress,sort_keys=True))
if __name__=='__main__':
 try:main()
 except Exception as exc:
  p={'state':'blocked','error_class':type(exc).__name__,'reason':str(exc)[:150], 'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}
  try: writejson(PROGRESS,p)
  except Exception:pass
  print(json.dumps(p))
  sys.exit(1)

