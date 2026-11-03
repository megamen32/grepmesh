#!/usr/bin/python3
"""Bounded historical immutable cloud shard offload. No index builders or uploads."""
import os, sys, json, hashlib, re, time, shlex, subprocess
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
import os,sys,re,hashlib
from pathlib import Path
root=Path('/run/user/1000/grepmesh-remote-reader-staged-20261103/store')
key=sys.argv[1]; size=int(sys.argv[2]); sha=sys.argv[3]; mode=sys.argv[4]
if (not re.fullmatch(r'[A-Za-z0-9_./-]+',key) or '..' in Path(key).parts or key.startswith('/')
    or not re.fullmatch(r'[0-9a-f]{64}',sha) or not 0 <= size <= 64*1024*1024):
 raise ValueError('Invalid staged object descriptor')
root.mkdir(mode=0o700,parents=True,exist_ok=True)
if os.stat(root).st_dev!=os.stat('/run/user/1000').st_dev:
 raise RuntimeError('Refuse non-tmpfs store')
path=root.joinpath(*key.split('/')); path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
if path.is_symlink() or any(p.is_symlink() for p in path.parents if p!=root.parent):
 raise RuntimeError('Refuse symlink')
if mode=='check':
 if not path.exists(): print('MISSING');sys.exit(0)
 with open(path,'rb') as f:
  h=hashlib.sha256();n=0
  for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk);n+=len(chunk)
 if n!=size or h.hexdigest()!=sha:raise RuntimeError('Existing object mismatch')
 print('PRESENT');sys.exit(0)
if mode!='put':raise ValueError('Invalid mode')
if path.exists():raise RuntimeError('Refuse overwrite')
temp=path.with_name(path.name+'.partial.'+str(os.getpid()))
h=hashlib.sha256();n=0
try:
 with open(temp,'xb') as f:
  while n<size:
   b=sys.stdin.buffer.read(min(1024*1024,size-n))
   if not b:break
   n+=len(b);h.update(b);f.write(b)
 if n!=size or h.hexdigest()!=sha:raise RuntimeError('Transferred object integrity mismatch')
 os.chmod(temp,0o600);os.replace(temp,path)
 print('COPIED')
finally:
 try:temp.unlink()
 except FileNotFoundError:pass
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
def cached(key,store,limit=MAX_MANIFEST):
 valid_key(key)
 p=LOCAL_META.joinpath(*key.split('/'))
 if p.exists():
  b=p.read_bytes()
  if len(b)>limit:raise RuntimeError("Cached metadata too large")
  return b
 b=store.get(key,limit)
 p.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
 with open(p,'xb') as f:f.write(b)
 os.chmod(p,0o600)
 return b
def builder_quiet():
 env=dict(os.environ)
 env["XDG_RUNTIME_DIR"]="/run/user/1000"
 env["DBUS_SESSION_BUS_ADDRESS"]="unix:path=/run/user/1000/bus"
 p=subprocess.run(["systemctl","--user","show",UNIT,"-p","ActiveState","--value"],
                  stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=5,env=env,text=True)
 if p.returncode!=0 or p.stdout.strip() not in ("failed","inactive"):
  raise RuntimeError("Sole builder active/unknown; offload suspended")
def main():
 ensure_tmpfs(LOCAL)
 LOCAL_META.mkdir(mode=0o700,parents=True,exist_ok=True)
 builder_quiet()
 store=CloudStore()
 scopes=json.load(open("/home/roomhacker/.config/grepmesh-cloud-legacy44.json"))
 if len(scopes)!=8:raise RuntimeError("Unexpected historical scope count")
 objects={};docs=0;parts=0
 for entry in scopes:
  scope=entry['scope']
  pkey='scopes/'+scope+'/latest.json'
  pb=cached(pkey,store,65536)
  pointer=json.loads(pb)
  mkey=valid_key(pointer['manifest'])
  mb=cached(mkey,store,MAX_MANIFEST)
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

