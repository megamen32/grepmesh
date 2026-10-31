#!/usr/bin/env python3
"""GrepMesh cloud-backed text index. Immutable cloud objects; bounded tmpfs only.
No source SQLite is required. Scope snapshots replace atomically and retain rollback.
The native fleet database is NEVER deleted or modified by this module.
"""
from __future__ import annotations
import argparse, base64, collections, contextlib, fnmatch, gzip, hashlib, io, json
import os, pathlib, re, shlex, shutil, sqlite3, subprocess, sys, threading, time, zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote

P = pathlib.Path
DEFAULT_ROOT = P('/run/user') / str(os.getuid()) / 'grepmesh-cloud-index'
PREFIX = 'yadisk:Backups/GrepMesh/cloud-index-v1'
SOURCE_MACHINE = os.environ.get('GREPMESH_CLOUD_GATEWAY', '')
RCLONE = os.environ.get('GREPMESH_CLOUD_RCLONE', 'rclone')
HOST = 'server-44-cloud-text'
MIB = 1024 * 1024
MAX_DOCUMENT = MIB
MAX_RAW_SHARD = 64 * MIB
MAX_MANIFEST = 16 * MIB
BLOOM_BYTES = 16384
EXTENSIONS = {'.rs','.py','.go','.js','.ts','.tsx','.jsx','.md','.txt','.toml','.json','.yaml','.yml','.sh','.html','.css','.sql','.c','.h','.cpp','.cmake','.xml','.csv'}
SECRET_NAMES = {'auth.json','credentials.json','client_secrets.json','id_rsa','id_ed25519','.env','.netrc'}


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def json_bytes(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(',',':')).encode('utf-8')


def ensure_tmpfs(root: P):
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root.chmod(0o700)
    fs = subprocess.check_output(['findmnt','-n','-o','FSTYPE','-T',str(root)],text=True).strip()
    if fs != 'tmpfs':
        raise RuntimeError('Refusing persistent-disk index/cache; runtime must be tmpfs')
    os.umask(0o077)


def safe_scope(name):
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,79}',name):
        raise ValueError('Invalid corpus identifier')
    return name


def valid_key(key):
    if not re.fullmatch(r'[A-Za-z0-9_./-]+', key) or '..' in P(key).parts or key.startswith('/'):
        raise ValueError('Unsafe cloud key')
    return key


class CloudStore:
    """Use existing authenticated rclone on 100; copy no credentials anywhere."""
    def __init__(self, prefix=PREFIX):
        self.prefix=prefix.rstrip('/');self.gets=0;self.bytes_in=0;self.puts=0
    def command(self,args):
        # rclone's optional scratch/cache are also explicitly in RAM on the gateway.
        if not SOURCE_MACHINE:raise ValueError('Set GREPMESH_CLOUD_GATEWAY to an authorized existing SSH/rclone gateway')
        tmp=os.environ.get('GREPMESH_CLOUD_GATEWAY_TMP', '/run/user/1000/grepmesh-cloud-gateway')
        cmd='umask 077; mkdir -p '+shlex.quote(tmp)+'; export TMPDIR='+shlex.quote(tmp)+'; exec '+shlex.join([RCLONE]+args+['--cache-dir',tmp])
        return ['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=8','-o','ServerAliveInterval=15',SOURCE_MACHINE,cmd]
    def get(self,key,limit=MAX_RAW_SHARD):
        key=valid_key(key)
        # --head bounds even a corrupted/hostile object response before it reaches RAM.
        r=subprocess.run(self.command(['cat',self.prefix+'/'+key,'--head',str(limit+1)]),stdin=subprocess.DEVNULL,capture_output=True,timeout=90)
        if r.returncode:raise RuntimeError('Cloud read failed: '+key)
        if len(r.stdout)>limit:raise ValueError('Cloud object exceeds declared bound')
        self.gets+=1;self.bytes_in+=len(r.stdout);return r.stdout
    def put(self,key,data):
        key=valid_key(key)
        r=subprocess.run(self.command(['rcat',self.prefix+'/'+key,'--size',str(len(data))]),input=data,capture_output=True,timeout=120)
        if r.returncode:raise RuntimeError('Cloud upload failed: '+key)
        r=subprocess.run(self.command(['lsjson',self.prefix+'/'+key,'--stat','--hash']),stdin=subprocess.DEVNULL,capture_output=True,timeout=45)
        if r.returncode:raise RuntimeError('Cloud upload verification unavailable')
        info=json.loads(r.stdout)
        if info.get('Size')!=len(data) or info.get('Hashes',{}).get('md5')!=hashlib.md5(data).hexdigest():
            raise RuntimeError('Cloud upload size/hash mismatch')
        self.puts+=1


class MemoryStore:
    """Test transport; never used for production durability."""
    def __init__(self):self.objects={};self.gets=0;self.bytes_in=0;self.puts=0
    def put(self,k,b):self.objects[k]=bytes(b);self.puts+=1
    def get(self,k,limit=MAX_RAW_SHARD):
        b=self.objects[k]
        if len(b)>limit:raise ValueError('Object limit')
        self.gets+=1;self.bytes_in+=len(b);return b


def grams(text):
    text=text.casefold()
    return {text[i:i+3] for i in range(max(0,len(text)-2))}


def bloom_for(texts):
    bits=bytearray(BLOOM_BYTES)
    for text in texts:
        for g in grams(text):
            h=zlib.crc32(g.encode('utf-8'))
            for shift in (0,10,20):
                i=((h>>shift) ^ (h*0x9E3779B1)) % (BLOOM_BYTES*8)
                bits[i>>3]|=1<<(i&7)
    return base64.b64encode(bits).decode('ascii')


def might_match(encoded,query):
    bits=base64.b64decode(encoded,validate=True)
    if len(bits)!=BLOOM_BYTES:raise ValueError('Invalid filter length')
    for g in grams(query):
        h=zlib.crc32(g.encode('utf-8'))
        for shift in (0,10,20):
            i=((h>>shift) ^ (h*0x9E3779B1)) % (BLOOM_BYTES*8)
            if not bits[i>>3] & (1<<(i&7)):return False
    return True


def document(path,body,size=0,mtime_ns=0):
    data=body.encode('utf-8')
    if len(data)>MAX_DOCUMENT:raise ValueError('Document too large')
    return {'kind':'document','path':path,'body':body,'size':size,'mtime_ns':mtime_ns,'sha256':digest(data)}


def scan_git(root: P, host: str, output):
    """Read original tracked files, not a local search database. No disk spool."""
    root=root.resolve(strict=True)
    p=subprocess.run(['git','-C',str(root),'ls-files','-z'],capture_output=True,check=True,timeout=30)
    names=p.stdout.decode('utf-8').split('\0')
    header={'kind':'begin','source_host':host,'source_root':str(root),'policy':'tracked UTF-8 text; no symlinks, secrets or binaries','format':1}
    output.write(json.dumps(header,ensure_ascii=False)+'\n');output.flush()
    skipped=collections.Counter();count=0;h=hashlib.sha256()
    for name in sorted(set(n for n in names if n)):
        rel=P(name)
        if rel.is_absolute() or '..' in rel.parts:raise ValueError('Unsafe git path')
        path=root/rel
        if any(x in SECRET_NAMES or x.startswith('.env.') for x in rel.parts):skipped['secret_name']+=1;continue
        if path.suffix.lower() not in EXTENSIONS and path.name not in {'Dockerfile','Makefile','LICENSE','AGENTS.md'}:skipped['format_policy']+=1;continue
        try:
            if path.is_symlink():skipped['symlink']+=1;continue
            canonical=path.resolve(strict=True)
            if not canonical.is_relative_to(root):raise ValueError('Source escaped authorized root')
            with open(canonical,'rb') as f:
                before=os.fstat(f.fileno());data=f.read(MAX_DOCUMENT+1);after=os.fstat(f.fileno())
            if before.st_mtime_ns!=after.st_mtime_ns or before.st_size!=after.st_size:raise RuntimeError('File changed during source read')
            if len(data)>MAX_DOCUMENT:skipped['oversize']+=1;continue
            if b'\0' in data:skipped['binary']+=1;continue
            body=data.decode('utf-8')
        except UnicodeDecodeError:skipped['not_utf8']+=1;continue
        # Permission/missing-file errors abort: do not publish an incomplete destructive replacement.
        doc=document(str(canonical),body,len(data),after.st_mtime_ns)
        h.update(json_bytes([doc['path'],doc['sha256']]))
        output.write(json.dumps(doc,ensure_ascii=False)+'\n');count+=1
    output.write(json.dumps({'kind':'end','count':count,'logical_sha256':h.hexdigest(),'skipped':dict(skipped),'complete_for_policy':True})+'\n')
    output.flush()


class Publisher:
    def __init__(self,store,root,shard_text_bytes=8*MIB):
        self.store=store;self.root=P(root);self.target=max(64*1024,min(shard_text_bytes,8*MIB))
    def shard(self,docs):
        scratch=self.root/('build-'+os.urandom(8).hex()+'.sqlite')
        c=sqlite3.connect(scratch)
        try:
            c.executescript("PRAGMA journal_mode=OFF; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-4096; CREATE TABLE bodies(sha TEXT PRIMARY KEY,compressed BLOB NOT NULL); CREATE TABLE docs(id INTEGER PRIMARY KEY,path TEXT UNIQUE NOT NULL,size INTEGER,mtime_ns INTEGER,sha TEXT NOT NULL); CREATE VIRTUAL TABLE terms USING fts5(path,body,content='',tokenize='trigram');")
            expanding=False
            for i,d in enumerate(docs,1):
                raw=d['body'].encode();sha=digest(raw)
                if sha!=d['sha256']:raise ValueError('Input document digest mismatch')
                c.execute('INSERT OR IGNORE INTO bodies VALUES(?,?)',(sha,zlib.compress(raw,3)))
                c.execute('INSERT INTO docs VALUES(?,?,?,?,?)',(i,d['path'],d['size'],d['mtime_ns'],sha))
                c.execute('INSERT INTO terms(rowid,path,body) VALUES(?,?,?)',(i,d['path'],d['body']))
                expanding|=(d['body'].casefold()!=d['body'].lower() or d['path'].casefold()!=d['path'].lower())
            c.commit();c.execute("INSERT INTO terms(terms) VALUES('optimize')");c.commit()
            if c.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise RuntimeError('Shard integrity failed')
            c.close()
            if scratch.stat().st_size>MAX_RAW_SHARD:
                if len(docs)<2:raise ValueError('Single-document shard exceeds bound')
                mid=len(docs)//2
                return self.shard(docs[:mid])+self.shard(docs[mid:])
            raw=scratch.read_bytes();sha=digest(raw);packed=gzip.compress(raw,compresslevel=3,mtime=0)
            key='objects/'+sha+'.sqlite.gz'
            self.store.put(key,packed)
            return [{'key':key,'sha256':sha,'packed_sha256':digest(packed),'raw_bytes':len(raw),'packed_bytes':len(packed),'documents':len(docs),'bloom':bloom_for([d['body']+'\n'+d['path'] for d in docs]),'expanding_casefold':expanding}]
        finally:
            with contextlib.suppress(Exception):c.close()
            scratch.unlink(missing_ok=True)
    def publish(self,scope,lines):
        safe_scope(scope);started=time.monotonic();parts=[];docs=[];size=0;count=0;seen=set();h=hashlib.sha256();end=None
        it=iter(lines);head=json.loads(next(it))
        if head.get('kind')!='begin' or head.get('format')!=1:raise ValueError('Missing stream header')
        source=P(head['source_root'])
        if not source.is_absolute() or '..' in source.parts:raise ValueError('Invalid source root')
        for line in it:
            if len(line)>MAX_DOCUMENT*7:raise ValueError('Oversized input record')
            d=json.loads(line)
            if d.get('kind')=='end':end=d;break
            if d.get('kind')!='document':raise ValueError('Malformed input')
            p=P(d['path'])
            if not p.is_absolute() or '..' in p.parts or not p.is_relative_to(source):raise ValueError('Document outside admitted source')
            if d['path'] in seen:raise ValueError('Duplicate path in source stream')
            if len(d['body'].encode())>MAX_DOCUMENT:raise ValueError('Oversized document')
            if digest(d['body'].encode())!=d['sha256']:raise ValueError('Input digest mismatch')
            seen.add(d['path']);h.update(json_bytes([d['path'],d['sha256']]))
            docs.append(d);size+=len(d['body'].encode());count+=1
            if size>=self.target or len(docs)>=512:
                parts.extend(self.shard(docs));docs=[];size=0
                if len(parts)>500:raise ValueError('Scope needs further partitioning; manifest not published')
        if not end or not end.get('complete_for_policy') or end['count']!=count or end['logical_sha256']!=h.hexdigest():
            raise RuntimeError('Incomplete source stream; existing cloud generation preserved')
        if any(line.strip() for line in it):raise ValueError('Unexpected records after source footer')
        if docs:parts.extend(self.shard(docs))
        manifest={'format':1,'scope':scope,'source_host':head['source_host'],'source_root':str(source),'policy':head['policy'],'skipped':end['skipped'],'documents':count,'logical_sha256':h.hexdigest(),'parts':parts,'generated_at':time.time(),'complete_for_policy':True,'storage':'cloud immutable shards / bounded RAM cache','build_seconds':round(time.monotonic()-started,3)}
        blob=json_bytes(manifest)
        if len(blob)>MAX_MANIFEST:raise ValueError('Manifest too large')
        key='manifests/'+scope+'/'+digest(blob)+'.json';self.store.put(key,blob)
        # Publish the pointer last. Complete old generations remain valid for rollback.
        pointer={'manifest':key,'sha256':digest(blob),'documents':count,'source_host':head['source_host'],'source_root':str(source)}
        self.store.put('scopes/'+scope+'/latest.json',json_bytes(pointer))
        return manifest


class Reader:
    def __init__(self,store,root,scope,cache_bytes=512*MIB,allowed_host=None,allowed_root=None):
        self.store=store;self.root=P(root);self.scope=safe_scope(scope);self.limit=cache_bytes
        self.allowed_host=allowed_host;self.allowed_root=allowed_root;self.manifest=None
        self.cache=collections.OrderedDict();self.used=0;self.lock=threading.RLock()
        self.directory=self.root/('cache-'+scope);self.directory.mkdir(mode=0o700,parents=True,exist_ok=True)
        for old in self.directory.glob('*.sqlite'):old.unlink()  # own reconstructible RAM cache only
    def refresh(self):
        pointer=json.loads(self.store.get('scopes/'+self.scope+'/latest.json',65536))
        blob=self.store.get(pointer['manifest'],MAX_MANIFEST)
        if digest(blob)!=pointer['sha256']:raise ValueError('Manifest checksum mismatch')
        m=json.loads(blob)
        if m['scope']!=self.scope or not m['complete_for_policy']:raise ValueError('Manifest scope/coverage mismatch')
        if self.allowed_host and m['source_host']!=self.allowed_host:raise ValueError('Source host not admitted')
        if self.allowed_root and m['source_root']!=self.allowed_root:raise ValueError('Source root not admitted')
        if sum(p['documents'] for p in m['parts'])!=m['documents']:raise ValueError('Manifest row accounting failed')
        for part in m['parts']:
            if not re.fullmatch(r'[0-9a-f]{64}',part['sha256']) or part['raw_bytes']>MAX_RAW_SHARD or part['raw_bytes']<4096:raise ValueError('Invalid shard descriptor')
            if part['key']!='objects/'+part['sha256']+'.sqlite.gz':raise ValueError('Shard identity mismatch')
            if part['packed_bytes']>MAX_RAW_SHARD:raise ValueError('Packed shard too large')
            might_match(part['bloom'],'')
        self.manifest=m;return m
    def clear(self):
        with self.lock:
            for path in self.cache.values():path.unlink(missing_ok=True)
            self.cache.clear();self.used=0
    def load(self,part):
        ident=part['sha256']
        if ident in self.cache:
            path=self.cache.pop(ident);self.cache[ident]=path;return path
        if part['raw_bytes']>self.limit:raise ValueError('Shard exceeds RAM cache admission limit')
        while self.used+part['raw_bytes']>self.limit and self.cache:
            _,old=self.cache.popitem(last=False);self.used-=old.stat().st_size;old.unlink()
        packed=self.store.get(part['key'],part['packed_bytes'])
        if len(packed)!=part['packed_bytes'] or digest(packed)!=part['packed_sha256']:raise ValueError('Cloud shard checksum mismatch')
        path=self.directory/(ident+'.sqlite');temp=path.with_suffix('.partial');h=hashlib.sha256();n=0
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(packed)) as src,open(temp,'xb') as dst:
                while True:
                    b=src.read(1024*1024)
                    if not b:break
                    n+=len(b)
                    if n>part['raw_bytes']:raise ValueError('Decompressed shard exceeds bound')
                    h.update(b);dst.write(b)
            if n!=part['raw_bytes'] or h.hexdigest()!=ident:raise ValueError('Shard raw checksum mismatch')
            os.replace(temp,path);self.cache[ident]=path;self.used+=n;return path
        finally:temp.unlink(missing_ok=True)
    def query(self,query,limit=20,glob=False,case_sensitive=False,roots=None,path_globs=None,deadline_seconds=25,authorized_roots=None):
        if not query or len(query)>2048:raise ValueError('Query must be 1..2048 characters')
        limit=max(1,min(limit,200));hits=[];incomplete=[];started=time.monotonic();reads0=self.store.gets
        with self.lock:
            if not self.manifest:self.refresh()
            m=self.manifest
            if roots and not any(r in ['local',self.scope,m['source_root']] for r in roots):
                permitted=[str(P(r)) for r in roots if P(r).is_absolute() and P(r).is_relative_to(m['source_root'])]
                if not permitted:raise ValueError('No authorized root in this corpus')
            else:permitted=[m['source_root']]
            for index,part in enumerate(m['parts']):
                if time.monotonic()-started>deadline_seconds:incomplete.append('query deadline reached; remaining shards not searched');break
                if not glob and not might_match(part['bloom'],query):continue
                try:
                    path=self.load(part)
                    c=sqlite3.connect('file:'+quote(str(path))+'?mode=ro&immutable=1',uri=True)
                    c.row_factory=sqlite3.Row;c.execute('PRAGMA cache_size=-2048');c.execute('PRAGMA temp_store=MEMORY')
                    c.set_progress_handler(lambda: time.monotonic()-started>deadline_seconds,1000)
                    try:
                        if not glob and len(query)>=3 and not part.get('expanding_casefold'):
                            sql='SELECT d.* FROM terms t JOIN docs d ON d.id=t.rowid WHERE terms MATCH ?'
                            rows=c.execute(sql,('"'+query.replace('"','""')+'"',))
                        else:rows=c.execute('SELECT * FROM docs')
                        for r in rows:
                            if time.monotonic()-started>deadline_seconds:raise TimeoutError('query deadline')
                            if not any(r['path']==v or r['path'].startswith(v.rstrip('/')+'/') for v in permitted):continue
                            if authorized_roots is not None and not any(r['path']==v or r['path'].startswith(v.rstrip('/')+'/') for v in authorized_roots):continue
                            if path_globs and not any(fnmatch.fnmatchcase(r['path'],g) for g in path_globs):continue
                            if glob:
                                pattern=query.casefold()
                                if not (fnmatch.fnmatchcase(r['path'].casefold(),pattern) or fnmatch.fnmatchcase(P(r['path']).name.casefold(),pattern)):continue
                                text='';line=0
                            else:
                                blob=c.execute('SELECT compressed FROM bodies WHERE sha=?',(r['sha'],)).fetchone()[0]
                                dec=zlib.decompressobj();raw=dec.decompress(blob,MAX_DOCUMENT+1)
                                if not dec.eof or len(raw)>MAX_DOCUMENT or digest(raw)!=r['sha']:raise ValueError('Document checksum mismatch')
                                body=raw.decode('utf-8');needle=query if case_sensitive else query.casefold()
                                lines=body.splitlines();matched=None
                                for number,value in enumerate(lines,1):
                                    if needle in (value if case_sensitive else value.casefold()):matched=(number,value);break
                                if matched:line,text=matched
                                elif needle in (r['path'] if case_sensitive else r['path'].casefold()):line,text=0,'[path match] '+r['path']
                                elif needle in (body if case_sensitive else body.casefold()):line,text=1,body[:1000]
                                else:continue
                            hits.append({'host_id':HOST,'source_host':m['source_host'],'path':r['path'],'line_number':line,'column':0,'context':[],'text':text[:3000],'size':r['size'],'modified_ms':r['mtime_ns']//1000000,'created_ms':None,'body_sha256':r['sha'],'snapshot_at':m['generated_at']})
                            if len(hits)>=limit+1:break
                    finally:c.close()
                except Exception as e:incomplete.append(type(e).__name__+': '+str(e)[:180])
                if len(hits)>limit:break
        return {'results':hits[:limit],'partial':bool(incomplete),'truncated':len(hits)>limit,'warnings':incomplete,'content_indexed':True,'scope':self.scope,'source_documents':m['documents'],'cache_bytes':self.used,'cache_limit_bytes':self.limit,'cloud_fetches':self.store.gets-reads0,'elapsed_ms':round((time.monotonic()-started)*1000,2),'complete_for_policy':m['complete_for_policy']}


def envelope(args,partial=False,error=None):
    return {'request_id':args.get('request_id') or os.urandom(12).hex(),'origin_host':args.get('origin_host') or HOST,'hop_count':int(args.get('hop_count') or 0),'host_id':HOST,'partial':partial,'truncated':False,'host_status':[{'host_id':HOST,'ok':not partial,'state':'partial' if partial else 'ok','error':error}]}


def tools():
    common={'hosts':{'anyOf':[{'type':'string'},{'type':'array','items':{'type':'string'}}]},'roots':{'type':'array','items':{'type':'string'}},'request_id':{'type':'string'},'origin_host':{'type':'string'},'hop_count':{'type':'integer'},'limit':{'type':'integer'},'max_matches':{'type':'integer'},'mode':{'type':'string'},'path_globs':{'type':'array','items':{'type':'string'}}}
    result=[]
    for name,desc in [('search','Search text and paths in the declared cloud text corpus.'),('search_text','Native GrepMesh peer alias for literal text search.'),('find_paths','Find paths in the declared cloud text corpus.'),('list_locations','Discover this cloud-backed text corpus.'),('search_status','Cloud coverage and bounded RAM cache status.')]:
        props=dict(common)
        if name in ['search','search_text']:props['query']={'type':'string'}
        if name=='find_paths':props.update(query={'type':'string'},pattern={'type':'string'})
        result.append({'name':name,'description':desc,'inputSchema':{'type':'object','properties':props,'required':['query'] if name in ['search','search_text'] else []},'annotations':{'readOnlyHint':True,'destructiveHint':False}})
    return result


def serve(reader,port):
    reader.refresh()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            body=json_bytes({'ready':True,'scope':reader.scope,'documents':reader.manifest['documents'],'cache_bytes':reader.used,'cache_limit_bytes':reader.limit,'persistent_index_bytes':0})
            self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(body)
        def do_POST(self):
            try:
                length=int(self.headers.get('Content-Length','0'))
                if length<=0 or length>65536:raise ValueError('Invalid request size')
                req=json.loads(self.rfile.read(length));method=req.get('method');par=req.get('params',{})
                if method=='initialize':result={'protocolVersion':'2024-11-05','capabilities':{'tools':{}},'serverInfo':{'name':'grepmesh','version':'cloud-index-v1'}}
                elif method=='notifications/initialized':self.send_response(202);self.end_headers();return
                elif method=='tools/list':result={'tools':tools()}
                elif method=='ping':result={}
                elif method=='tools/call':
                    args=par.get('arguments') or {};name=par.get('name');m=reader.manifest
                    selected=args.get('hosts') or ['local'];selected=[selected] if isinstance(selected,str) else selected
                    if not any(x in ['local','*',HOST] for x in selected):raise ValueError('Wrong cloud host')
                    if int(args.get('hop_count') or 0)>1:raise ValueError('Mesh hop limit exceeded')
                    if name=='list_locations':out={**envelope(args),'target_host_id':HOST,'locations':[{'host':HOST,'name':reader.scope,'path':m['source_root']}],'source_host':m['source_host'],'content_indexed':True,'coverage_policy':m['policy']}
                    elif name=='search_status':
                        local={'host_id':HOST,'root':m['source_root'],'roots':[reader.scope],'backend':'cloud shards / contentless FTS / bounded RAM','file_count':m['documents'],'indexed_files':m['documents'],'index_db_bytes':reader.used,'index_state':'Ready','index_generation':int(m['generated_at']),'index_last_error':None,'index_directory_activity':[]}
                        out={**envelope(args),'local':local,'nodes':[local],'topology':{'freshness':None,'generation':1,'last_refresh_error':None},'persistent_index_bytes':0,'cache_limit_bytes':reader.limit,'cloud_shards':len(m['parts']),'scope':reader.scope,'coverage_policy':m['policy']}
                    elif name in ['search','search_text','find_paths']:
                        if args.get('mode')=='regex':raise ValueError('Regex is not yet supported; not an empty result')
                        allowed=None
                        if int(args.get('hop_count') or 0)>0 and name in ['search','search_text']:
                            acl=args.get('filesystem_acl')
                            if not isinstance(acl,dict) or acl.get('version')!=1 or not acl.get('profile_id') or not acl.get('read_roots'):raise ValueError('Forwarded text search requires a verified GPTAdmin filesystem ACL')
                            allowed=acl['read_roots']
                            if not isinstance(allowed,list) or not all(isinstance(x,str) and P(x).is_absolute() and '..' not in P(x).parts for x in allowed):raise ValueError('Invalid filesystem ACL roots')
                        out=reader.query(str(args.get('pattern',args.get('query',''))),int(args.get('limit',args.get('max_matches',20))),glob=name=='find_paths',case_sensitive=args.get('mode')=='literal',roots=args.get('roots'),path_globs=args.get('path_globs'),authorized_roots=allowed)
                        env=envelope(args,out['partial'],'; '.join(out['warnings']) if out['warnings'] else None);env['truncated']=out['truncated'];out.update(env)
                        if name=='find_paths':out['paths']=[{'host':HOST,'path':r['path'],'kind':'file'} for r in out['results']]
                    else:raise ValueError('Unsupported tool')
                    result={'content':[{'type':'text','text':json.dumps(out,ensure_ascii=False)}],'isError':False}
                else:raise ValueError('Unsupported RPC method')
                data=json_bytes({'jsonrpc':'2.0','id':req.get('id'),'result':result})
                self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
            except Exception as e:
                data=json_bytes({'jsonrpc':'2.0','id':locals().get('req',{}).get('id'),'result':{'isError':True,'content':[{'type':'text','text':str(e)[:500]}]}})
                self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(data)
    ThreadingHTTPServer(('127.0.0.1',port),Handler).serve_forever()


def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['scan','publish','search','serve']);p.add_argument('--root',default=str(DEFAULT_ROOT));p.add_argument('--scope',default='grepmesh-source');p.add_argument('--source-root',default=os.environ.get('GREPMESH_SOURCE_ROOT',''));p.add_argument('--source-host',default=os.environ.get('GREPMESH_SOURCE_HOST',''));p.add_argument('--cache-mib',type=int,default=512);p.add_argument('--shard-kib',type=int,default=8192);p.add_argument('--query');p.add_argument('--port',type=int,default=19435);a=p.parse_args()
    if hasattr(os,'sched_setaffinity'):os.sched_setaffinity(0,sorted(os.sched_getaffinity(0))[:2])
    os.nice(10)
    if not a.source_root or not a.source_host:raise ValueError('Explicit --source-root and --source-host are required')
    if a.action=='scan':scan_git(P(a.source_root),a.source_host,sys.stdout);return
    ensure_tmpfs(P(a.root));store=CloudStore()
    if a.action=='publish':
        import fcntl
        with open(P(a.root)/'publisher.lock','w') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            m=Publisher(store,a.root,a.shard_kib*1024).publish(a.scope,sys.stdin)
            report={k:v for k,v in m.items() if k!='parts'};report.update(shards=len(m['parts']),cloud_bytes=sum(x['packed_bytes'] for x in m['parts']),raw_index_bytes=sum(x['raw_bytes'] for x in m['parts']),cloud_uploads=store.puts)
            (P(a.root)/'published.json').write_bytes(json_bytes(report));print(json.dumps(report,ensure_ascii=False))
    else:
        reader=Reader(store,a.root,a.scope,a.cache_mib*MIB,a.source_host,a.source_root)
        if a.action=='serve':serve(reader,a.port)
        else:print(json.dumps(reader.query(a.query or ''),ensure_ascii=False))

if __name__=='__main__':main()
