#!/usr/bin/env python3
"""GrepMesh cloud-backed text index. Immutable cloud objects; bounded tmpfs only.
No source SQLite is required. Scope snapshots replace atomically and retain rollback.
The native fleet database is NEVER deleted or modified by this module.
"""
from __future__ import annotations
import argparse, base64, collections, contextlib, fnmatch, gzip, hashlib, hmac, io, json
import os, pathlib, re, shlex, shutil, sqlite3, subprocess, sys, threading, time, zlib
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote

P = pathlib.Path
DEFAULT_ROOT = P('/run/user') / str(os.getuid()) / 'grepmesh-cloud-index'
PREFIX = 'yadisk:Backups/GrepMesh/cloud-index-v1'
SOURCE_MACHINE = os.environ.get('GREPMESH_CLOUD_GATEWAY', '')
RCLONE = os.environ.get('GREPMESH_CLOUD_RCLONE', 'rclone')
HOST = os.environ.get('GREPMESH_CLOUD_HOST_ID', 'server-44-cloud-text')
INDEX_LAYOUT = 'fts5-trigram-detail-none-v2'
MIB = 1024 * 1024
MAX_DOCUMENT = MIB
MAX_RAW_SHARD = 64 * MIB
MAX_MANIFEST = 16 * MIB
BLOOM_BYTES = 16384
QUADGRAM_BLOOM_BYTES = 256 * 1024
EXTENSIONS = {'.rs','.py','.go','.js','.ts','.tsx','.jsx','.vue','.proto','.mjs','.cjs','.jsonl','.md','.txt','.toml','.json','.yaml','.yml','.sh','.html','.css','.sql','.c','.h','.cpp','.cmake','.xml','.csv'}
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
        if SOURCE_MACHINE == 'local':
            # On the rclone owner host, avoid an SSH round trip to itself.
            # Reuse that user's existing rclone authentication and tmpfs only.
            ensure_tmpfs(P(tmp))
            return ['/bin/sh','-c',cmd]
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


def fts_gram_query(query):
    """Bounded FTS5 conjunction of individual trigrams, valid with detail=none.

    Every real substring contains these grams; exact body verification filters
    false positives. Restrict to alphanumeric grams for tokenizer portability.
    """
    q=query.casefold()
    tokens=sorted({q[i:i+3] for i in range(max(0,len(q)-2))
                   if all(ch.isalnum() for ch in q[i:i+3])})
    return ' AND '.join('"' + token.replace('"','""') + '"' for token in tokens[:16])


def sre_literal_required(pattern):
    """A contiguous literal substring required for EVERY match, or None.

    Used only for sound prefiltering: absence of the required literal in a
    body guarantees the regex cannot match, so skipping that body never loses
    a result. Returns the longest safe contiguous prefix extracted from the
    parse tree; optional/ranged/classed elements break the extraction.
    Anchors like ^ $ \\b are not consumed and end the literal prefix.
    """
    try:
        import sre_parse, sre_constants as sc
        ast = sre_parse.parse(pattern)
    except Exception:
        return None
    chars = []
    def walk(seq):
        nonlocal chars
        for op, av in seq:
            if op is sc.LITERAL:
                chars.append(av); continue
            if op is sc.MAX_REPEAT:
                lo, hi, sub = av
                if lo == hi == 1:
                    if not walk(sub): return False
                    continue
                if lo < 1 or len(sub) != 1 or sub[0][0] is not sc.LITERAL:
                    return False
                chars.extend([sub[0][1]] * min(lo, 64)); continue
            if op is sc.SUBPATTERN:
                _group, _add, _del, sub = av
                if not walk(sub): return False
                continue
            if op is sc.AT:
                continue  # anchors assert but do not consume; literal run continues
            if op in (sc.NOT_LITERAL, sc.ANY, sc.IN, sc.BRANCH,
                      sc.ASSERT, sc.ASSERT_NOT, sc.CATEGORY,
                      getattr(sc, 'ATOMIC_GROUP', None),
                      getattr(sc, 'POSSESSIVE_REPEAT', None)):
                return False
            return False
        return True
    if not walk(ast):
        pass  # accumulated contiguous prefix stays; later elements break it
    literal = ''.join(chr(c) for c in chars[:64])
    return literal if len(literal) >= 2 else None


def compile_bounded_regex(pattern):
    """Compile once; optional hard-timeout engine for bounded per-call latency.

    (standard re compiled object, hard-bounded compiled object or None).
    The re object keeps exact re-compatible semantics; the hard object only
    accelerates interruptibility. Returns None, None for invalid patterns.
    """
    try:
        std = re.compile(pattern)
    except re.error:
        return None
    hard = None
    try:
        import regex
        hard = regex.compile(pattern)
    except Exception:
        hard = None
    return std, hard


def regex_can_span_newlines(pattern):
    """True when the pattern may consume a newline and.match across lines.

    Conservative classification over the parse tree: unknown constructs
    return True (an extra bounded engine call), so cross-line matching is
    never silently lost. False positives only cost latency, never results.
    """
    try:
        import sre_parse, sre_constants as sc
        ast = sre_parse.parse(pattern)
        flags = re.compile(pattern).flags
    except Exception:
        return True
    repeat_ops=[sc.MAX_REPEAT,sc.MIN_REPEAT]+([sc.POSSESSIVE_REPEAT] if hasattr(sc,'POSSESSIVE_REPEAT') else [])
    def consumes(seq,flags):
        for op,av in seq:
            if op is sc.LITERAL:
                if av==10:return True
                continue
            if op is sc.NOT_LITERAL:
                return True  # any char except one: newline always admissible
            if op is sc.ANY:
                return bool(flags & re.S)
            if op is sc.IN:
                for kind,val in av:
                    if kind is sc.NEGATE:return True
                    if kind is sc.LITERAL and val==10:return True
                    if kind is sc.RANGE and val[0]<=10<=val[1]:return True
                    if kind is sc.CATEGORY and val in (sc.CATEGORY_SPACE,
                            sc.CATEGORY_NOT_SPACE, sc.CATEGORY_NOT_WORD):return True
                continue
            if op in repeat_ops:
                if consumes(av[2],flags):return True
                continue
            if op is getattr(sc,'ATOMIC_GROUP',None):
                if consumes(av,flags):return True
                continue
            if op is sc.SUBPATTERN:
                _group,add,delet,sub=av
                if consumes(sub,(flags|add)&~delet):return True
                continue
            if op is sc.BRANCH:
                if any(consumes(sub,flags) for sub in av[1]):return True
                continue
            if op in (sc.ASSERT,sc.ASSERT_NOT):
                if consumes(av[1],flags):return True
                continue
            if op is sc.AT:
                continue
            if op is sc.GROUPREF:
                return True
            return True
        return False
    return consumes(ast,flags)


def regex_match_positions(compiled, hard, body, deadline_seconds):
    """First regex match of body as (line_number, column, matched_line_text).

    Per-line scan first (grep parity for ^ $ anchors); if the pattern can
    match a newline character, also try whole-body string search so (?s)
    classes and literal \\n are represented honestly. Bounded: each engine
    call carries the remaining deadline when the hard engine is available;
    without it, per-line calls stay deadline-checked between lines.
    Raises TimeoutError when the deadline is exhausted.
    """
    def remaining():
        return deadline_seconds - time.monotonic()
    if remaining() <= 0:
        raise TimeoutError('regex deadline')
    newline_capable = regex_can_span_newlines(compiled.pattern)
    for number, line in enumerate(body.splitlines(), 1):
        if remaining() <= 0:
            raise TimeoutError('regex deadline')
        m = None
        if hard is not None:
            m = hard.search(line, timeout=remaining())
        else:
            m = compiled.search(line)
        if m is not None:
            return number, m.start(), line
    if newline_capable is not None:
        if remaining() <= 0:
            raise TimeoutError('regex deadline')
        m = None
        if hard is not None:
            m = hard.search(body, timeout=remaining())
        else:
            m = compiled.search(body)
        if m is not None:
            nl = body.rfind('\n', 0, m.start())
            line_number = body.count('\n', 0, m.start()) + 1
            end = body.find('\n', m.start())
            text = body[nl + 1:end] if end != -1 else body[nl + 1:]
            return line_number, m.start() - (nl + 1), text
    return None


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


def quadgram_bloom_for(texts):
    """Optional secondary filter: every casefolded four-gram is retained."""
    bits=bytearray(QUADGRAM_BLOOM_BYTES)
    modulus=len(bits)*8
    for text in texts:
        folded=text.casefold()
        for g in {folded[i:i+4] for i in range(max(0,len(folded)-3))}:
            h=zlib.crc32(g.encode('utf-8'))
            for shift in (0,10,20):
                bit=((h>>shift)^(h*0x9E3779B1))%modulus
                bits[bit>>3] |= 1<<(bit&7)
    return base64.b64encode(bits).decode('ascii')


def quadgram_might_match(encoded,query):
    bits=encoded if isinstance(encoded,bytes) else base64.b64decode(encoded,validate=True)
    if len(bits)!=QUADGRAM_BLOOM_BYTES:raise ValueError('Invalid quadgram filter length')
    folded=query.casefold()
    modulus=len(bits)*8
    for g in {folded[i:i+4] for i in range(max(0,len(folded)-3))}:
        h=zlib.crc32(g.encode('utf-8'))
        for shift in (0,10,20):
            bit=((h>>shift)^(h*0x9E3779B1))%modulus
            if not bits[bit>>3] & (1<<(bit&7)):return False
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
    def __init__(self,store,root,shard_text_bytes=8*MIB, max_shard_documents=512):
        if type(max_shard_documents) is not int or not 1 <= max_shard_documents <= 8192:
            raise ValueError('Invalid shard document limit')
        self.store=store;self.root=P(root);self.target=max(64*1024,min(shard_text_bytes,16*MIB))
        self.max_shard_documents=max_shard_documents
    def shard(self,docs):
        scratch=self.root/('build-'+os.urandom(8).hex()+'.sqlite')
        c=sqlite3.connect(scratch)
        try:
            c.executescript("PRAGMA journal_mode=OFF; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-4096; CREATE TABLE bodies(sha TEXT PRIMARY KEY,compressed BLOB NOT NULL); CREATE TABLE docs(id INTEGER PRIMARY KEY,path TEXT UNIQUE NOT NULL,size INTEGER,mtime_ns INTEGER,sha TEXT NOT NULL); CREATE VIRTUAL TABLE terms USING fts5(path,body,content='',tokenize='trigram',detail=none,columnsize=0);")
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
    def path_index(self,entries):
        """One small path-only cloud object makes filename lookups independent of text shards."""
        scratch=self.root/('path-'+os.urandom(8).hex()+'.sqlite')
        c=sqlite3.connect(scratch)
        try:
            # Glob search scans paths and does its own case folding. A primary
            # key plus a folded copy and index stored the same long path three
            # times; duplicates were already rejected by publish().
            c.executescript('''PRAGMA journal_mode=OFF; PRAGMA temp_store=MEMORY;
                CREATE TABLE paths(path TEXT NOT NULL,size INTEGER,mtime_ns INTEGER,sha TEXT);''')
            c.executemany('INSERT INTO paths VALUES(?,?,?,?)', entries)
            c.commit()
            if c.execute('PRAGMA quick_check').fetchone()[0]!='ok':
                raise RuntimeError('Path index integrity failed')
            c.close()
            if scratch.stat().st_size>MAX_RAW_SHARD:
                raise ValueError('Path index too large; split the source scope')
            raw=scratch.read_bytes();sha=digest(raw)
            packed=gzip.compress(raw,compresslevel=3,mtime=0)
            key='objects/'+sha+'.sqlite.gz'
            self.store.put(key,packed)
            return {'key':key,'sha256':sha,'packed_sha256':digest(packed),
                    'raw_bytes':len(raw),'packed_bytes':len(packed),
                    'documents':len(entries),'kind':'path_index'}
        finally:
            with contextlib.suppress(Exception):c.close()
            scratch.unlink(missing_ok=True)

    def publish(self,scope,lines):
        safe_scope(scope);started=time.monotonic();parts=[];docs=[];paths=[];size=0;count=0;seen=set();h=hashlib.sha256();end=None
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
            seen.add(d['path']);h.update(json_bytes([d['path'],d['sha256']]));paths.append((d['path'],d['size'],d['mtime_ns'],d['sha256']))
            docs.append(d);size+=len(d['body'].encode());count+=1
            if size>=self.target or len(docs)>=self.max_shard_documents:
                parts.extend(self.shard(docs));docs=[];size=0
                if len(parts)>500:raise ValueError('Scope needs further partitioning; manifest not published')
        if not end or not end.get('complete_for_policy') or end['count']!=count or end['logical_sha256']!=h.hexdigest():
            raise RuntimeError('Incomplete source stream; existing cloud generation preserved')
        if any(line.strip() for line in it):raise ValueError('Unexpected records after source footer')
        if docs:parts.extend(self.shard(docs))
        path_descriptor=self.path_index(paths)
        manifest={'path_index':path_descriptor,'index_layout':INDEX_LAYOUT,'format':1,'scope':scope,'source_host':head['source_host'],'source_root':str(source),'policy':head['policy'],'skipped':end['skipped'],'documents':count,'logical_sha256':h.hexdigest(),'parts':parts,'generated_at':time.time(),'complete_for_policy':True,'storage':'cloud immutable shards / bounded RAM cache','build_seconds':round(time.monotonic()-started,3)}
        blob=json_bytes(manifest)
        if len(blob)>MAX_MANIFEST:raise ValueError('Manifest too large')
        key='manifests/'+scope+'/'+digest(blob)+'.json';self.store.put(key,blob)
        # Publish the pointer last. Complete old generations remain valid for rollback.
        pointer={'manifest':key,'sha256':digest(blob),'documents':count,'source_host':head['source_host'],'source_root':str(source)}
        self.store.put('scopes/'+scope+'/latest.json',json_bytes(pointer))
        return manifest


def prefetched_shards(store, parts, cached, started, deadline_seconds, workers=6):
    """Fetch a bounded lookahead of cloud blobs concurrently, preserving order.

    Only compressed bytes are fetched by workers. A single owning query thread
    verifies checksums, opens SQLite and mutates its bounded tmpfs LRU cache.
    This never admits a partially downloaded shard as a search result.
    """
    if not parts:
        return
    queue = collections.deque()
    remaining = iter(parts)
    pool = ThreadPoolExecutor(max_workers=min(workers, len(parts)),
                              thread_name_prefix='grepmesh-cloud-get')
    def submit_one():
        try:
            part = next(remaining)
        except StopIteration:
            return
        if part['sha256'] in cached:
            queue.append((part, None))
        else:
            future = pool.submit(store.get, part['key'], part['packed_bytes'])
            queue.append((part, future))
    try:
        for _ in range(min(workers, len(parts))):
            submit_one()
        while queue:
            part, future = queue.popleft()
            if time.monotonic()-started < deadline_seconds:
                submit_one()
            if future is None:
                yield part, None
                continue
            budget = deadline_seconds - (time.monotonic()-started)
            if budget <= 0:
                yield part, TimeoutError('cloud shard deadline')
                continue
            try:
                payload = future.result(timeout=budget)
            except Exception as error:
                payload = error
            yield part, payload
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


class Reader:
    def __init__(self,store,root,scope,cache_bytes=512*MIB,allowed_host=None,allowed_root=None,quadgram_overlay=None):
        self.store=store;self.root=P(root);self.scope=safe_scope(scope);self.limit=cache_bytes
        self.allowed_host=allowed_host;self.allowed_root=allowed_root;self.manifest=None
        self.quadgram_overlay=quadgram_overlay;self.quadgram_filters={}
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
        if m.get('path_index') and m['path_index'].get('documents')!=m['documents']:
            raise ValueError('Path index row accounting mismatch')
        for part in m['parts']+([m['path_index']] if m.get('path_index') else []):
            if not re.fullmatch(r'[0-9a-f]{64}',part['sha256']) or part['raw_bytes']>MAX_RAW_SHARD or part['raw_bytes']<4096:raise ValueError('Invalid shard descriptor')
            if part['key']!='objects/'+part['sha256']+'.sqlite.gz':raise ValueError('Shard identity mismatch')
            if part['packed_bytes']>MAX_RAW_SHARD:raise ValueError('Packed shard too large')
            if part.get('kind')!='path_index':might_match(part['bloom'],'')
        filters={}
        if self.quadgram_overlay is not None:
            spec=self.quadgram_overlay
            if (not isinstance(spec,dict) or set(spec)!={'key','sha256'}
                or not isinstance(spec['key'],str) or not spec['key'].startswith('overlays/quadgram/')
                or not re.fullmatch(r'[0-9a-f]{64}',str(spec['sha256']))):
                raise ValueError('Invalid quadgram overlay reference')
            raw=self.store.get(valid_key(spec['key']),MAX_MANIFEST)
            if digest(raw)!=spec['sha256']:raise ValueError('Quadgram overlay checksum mismatch')
            overlay=json.loads(raw)
            if (overlay.get('format')!=1 or overlay.get('scope')!=self.scope
                or overlay.get('source_host')!=m['source_host']
                or overlay.get('manifest_sha256')!=pointer['sha256']
                or overlay.get('documents')!=m['documents']
                or not isinstance(overlay.get('filters'),list)
                or len(overlay['filters'])!=len(m['parts'])):
                raise ValueError('Quadgram overlay source binding mismatch')
            for part,item in zip(m['parts'],overlay['filters']):
                if not isinstance(item,dict) or item.get('shard_sha256')!=part['sha256']:
                    raise ValueError('Quadgram overlay shard binding mismatch')
                bits=base64.b64decode(item.get('quadgram',''),validate=True)
                if len(bits)!=QUADGRAM_BLOOM_BYTES:raise ValueError('Invalid quadgram filter length')
                if part['sha256'] in filters:raise ValueError('Duplicate quadgram shard')
                filters[part['sha256']]=bits
        self.quadgram_filters=filters;self.manifest=m;return m
    def clear(self):
        with self.lock:
            for path in self.cache.values():path.unlink(missing_ok=True)
            self.cache.clear();self.used=0
    def load(self,part,packed=None):
        ident=part['sha256']
        if ident in self.cache:
            path=self.cache.pop(ident);self.cache[ident]=path;return path
        if part['raw_bytes']>self.limit:raise ValueError('Shard exceeds RAM cache admission limit')
        while self.used+part['raw_bytes']>self.limit and self.cache:
            _,old=self.cache.popitem(last=False);self.used-=old.stat().st_size;old.unlink()
        if packed is None:packed=self.store.get(part['key'],part['packed_bytes'])
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
    def read_text(self,path,start_line=1,end_line=None,authorized_roots=None,deadline_seconds=25,source_host=None):
        """Read immutable cloud body for an exact historical path, without host filesystem IO.

        A candidate path is authorized before any body shard is downloaded.
        Historical deleted-file text is deliberately readable from the archive.
        Missing/corrupt/timeout cases are errors, never authoritative empty text.
        """
        if (not isinstance(path,str) or not path.startswith('/') or
            chr(0) in path or len(path.encode('utf-8'))>4096 or
            '..' in P(path).parts or os.path.normpath(path)!=path):
            raise ValueError('A normalized absolute historical path is required')
        if (not isinstance(start_line,int) or isinstance(start_line,bool) or
            start_line<1 or start_line>100000000):
            raise ValueError('Invalid start_line')
        if end_line is not None and (not isinstance(end_line,int) or
                isinstance(end_line,bool) or end_line<1 or end_line>100000000):
            raise ValueError('Invalid end_line')
        if deadline_seconds<=0 or deadline_seconds>120:
            raise ValueError('Invalid read deadline')
        def under(p,root):
            return p==root or p.startswith(root.rstrip('/')+'/')
        started=time.monotonic()
        with self.lock:
            if not self.manifest:self.refresh()
            m=self.manifest
            if source_host is not None and source_host != m['source_host']:
                raise ValueError('source_host does not match this reader')
            if not under(path,m['source_root']):
                raise PermissionError('Path outside admitted source root')
            if authorized_roots is not None:
                if (not isinstance(authorized_roots,list) or
                    not all(isinstance(x,str) and x.startswith('/') and
                            '..' not in P(x).parts for x in authorized_roots)):
                    raise PermissionError('Invalid verified filesystem read roots')
                if not any(under(path,root) for root in authorized_roots):
                    raise PermissionError('Filesystem ACL denies historical path')
            candidates=[part for part in m['parts'] if might_match(part['bloom'],path)
                        and (part['sha256'] not in self.quadgram_filters
                             or quadgram_might_match(self.quadgram_filters[part['sha256']],path))]
            for part,downloaded in prefetched_shards(
                    self.store,candidates,self.cache,started,deadline_seconds,workers=2):
                if time.monotonic()-started>=deadline_seconds:
                    raise TimeoutError('Historical cloud read deadline')
                if isinstance(downloaded,Exception):
                    raise RuntimeError('Historical cloud shard unavailable') from downloaded
                shard=self.load(part,packed=downloaded)
                c=sqlite3.connect('file:'+quote(str(shard))+'?mode=ro&immutable=1',uri=True)
                c.row_factory=sqlite3.Row
                try:
                    row=c.execute('SELECT sha FROM docs WHERE path=?',(path,)).fetchone()
                    if row is None:continue
                    payload=c.execute('SELECT compressed FROM bodies WHERE sha=?',
                                      (row['sha'],)).fetchone()
                    if payload is None:raise RuntimeError('Historical body missing')
                    decoder=zlib.decompressobj()
                    raw=decoder.decompress(payload['compressed'],MAX_DOCUMENT+1)
                    if (not decoder.eof or len(raw)>MAX_DOCUMENT or
                            digest(raw)!=row['sha']):
                        raise RuntimeError('Historical body integrity failure')
                    body=raw.decode('utf-8')
                    lines=body.splitlines()
                    stop=max(start_line,end_line if end_line is not None else len(lines))
                    selected=[{'line_number':i+1,'text':text}
                              for i,text in enumerate(lines)
                              if start_line<=i+1<=stop]
                    if sum(len(x['text'].encode('utf-8')) for x in selected)>MAX_DOCUMENT:
                        raise RuntimeError('Historical text range exceeds 1 MiB')
                    return {'path':path,'source_host':m['source_host'],
                            'body_sha256':row['sha'],
                            'start_line':start_line,
                            'end_line':min(stop,len(lines)) if len(lines)>=start_line else start_line,
                            'chunks':[{'start_line':start_line,
                                       'end_line':min(stop,len(lines)) if len(lines)>=start_line else start_line,
                                       'lines':selected}],
                            'snapshot_at':m['generated_at']}
                finally:c.close()
        raise FileNotFoundError('Historical path is not in the searchable cloud snapshot')

    def query(self,query,limit=20,glob=False,case_sensitive=False,roots=None,path_globs=None,deadline_seconds=25,authorized_roots=None,regex=False):
        if not query or len(query)>2048:raise ValueError('Query must be 1..2048 characters')
        limit=max(1,min(limit,200));hits=[];incomplete=[];started=time.monotonic();reads0=self.store.gets
        hard=None
        if regex:
            compiled=compile_bounded_regex(query)
            if compiled is None:raise ValueError('Invalid regular expression: not an empty result')
            std_compiled,hard=compiled
            # Sound routing/prefilter: a body without the required literal cannot match.
            gram_source=sre_literal_required(query) or ''
        else:
            std_compiled=None;gram_source='' if glob else query
        with self.lock:
            if not self.manifest:self.refresh()
            m=self.manifest
            if roots and not any(r in ['local',self.scope,m['source_root']] for r in roots):
                permitted=[str(P(r)) for r in roots if P(r).is_absolute() and P(r).is_relative_to(m['source_root'])]
                if not permitted:raise ValueError('No authorized root in this corpus')
            else:permitted=[m['source_root']]
            if glob and m.get('path_index'):
                try:
                    path=self.load(m['path_index'])
                    c=sqlite3.connect('file:'+quote(str(path))+'?mode=ro&immutable=1',uri=True)
                    c.row_factory=sqlite3.Row
                    c.execute('PRAGMA cache_size=-2048')
                    c.set_progress_handler(lambda: time.monotonic()-started>deadline_seconds,1000)
                    try:
                        for row in c.execute('SELECT path,size,mtime_ns,sha FROM paths'):
                            if time.monotonic()-started>deadline_seconds:
                                raise TimeoutError('path-only lookup deadline')
                            rp=row['path']
                            if not any(rp==v or rp.startswith(v.rstrip('/')+'/') for v in permitted):continue
                            if authorized_roots is not None and not any(rp==v or rp.startswith(v.rstrip('/')+'/') for v in authorized_roots):continue
                            if path_globs and not any(fnmatch.fnmatchcase(rp,g) for g in path_globs):continue
                            pattern=query.casefold()
                            if not (fnmatch.fnmatchcase(rp.casefold(),pattern)
                                    or fnmatch.fnmatchcase(P(rp).name.casefold(),pattern)):continue
                            hits.append({'host_id':HOST,'source_host':m['source_host'],
                                'path':rp,'line_number':0,'column':0,'context':[],'text':'',
                                'size':row['size'],'modified_ms':row['mtime_ns']//1000000,
                                'created_ms':None,'body_sha256':row['sha'],
                                'snapshot_at':m['generated_at']})
                            if len(hits)>limit:break
                    finally:c.close()
                except Exception as e:incomplete.append(type(e).__name__+': '+str(e)[:180])
                return {'results':hits[:limit],'partial':bool(incomplete),
                    'truncated':len(hits)>limit,'warnings':incomplete,'content_indexed':True,
                    'scope':self.scope,'source_documents':m['documents'],
                    'cache_bytes':self.used,'cache_limit_bytes':self.limit,
                    'cloud_fetches':self.store.gets-reads0,
                    'elapsed_ms':round((time.monotonic()-started)*1000,2),
                    'complete_for_policy':m['complete_for_policy']}
            candidates=[part for part in m['parts'] if glob or (
                might_match(part['bloom'],gram_source.casefold()) and
                (part['sha256'] not in self.quadgram_filters or
                 quadgram_might_match(self.quadgram_filters[part['sha256']],gram_source)))]
            prefetched=prefetched_shards(self.store,candidates,self.cache,started,deadline_seconds)
            for part,downloaded in prefetched:
                if time.monotonic()-started>deadline_seconds:incomplete.append('query deadline reached; remaining shards not searched');break
                try:
                    if isinstance(downloaded,Exception):raise downloaded
                    path=self.load(part,packed=downloaded)
                    c=sqlite3.connect('file:'+quote(str(path))+'?mode=ro&immutable=1',uri=True)
                    c.row_factory=sqlite3.Row;c.execute('PRAGMA cache_size=-2048');c.execute('PRAGMA temp_store=MEMORY')
                    c.set_progress_handler(lambda: time.monotonic()-started>deadline_seconds,1000)
                    try:
                        gram_query=fts_gram_query(gram_source)
                        if (gram_query and not part.get('expanding_casefold')
                            and gram_source.casefold()==gram_source.lower()):
                            sql='SELECT d.* FROM terms t JOIN docs d ON d.id=t.rowid WHERE terms MATCH ?'
                            rows=c.execute(sql,(gram_query,))
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
                                body=raw.decode('utf-8');line=0;column=0;text=''
                                if regex:
                                    found=regex_match_positions(std_compiled,hard,body,time.monotonic()+deadline_seconds)
                                    if found is None:
                                        remaining_path=started+deadline_seconds-time.monotonic()
                                        path_match=(hard.search(r['path'],timeout=remaining_path) if hard is not None and remaining_path>0
                                                    else std_compiled.search(r['path']) if remaining_path>0 else None)
                                        if path_match is None:continue
                                        line,column,text=0,0,'[path match] '+r['path']
                                    else:
                                        line,column,text=found
                                else:
                                    needle=query if case_sensitive else query.casefold()
                                    lines=body.splitlines();matched=None
                                    for number,value in enumerate(lines,1):
                                        if needle in (value if case_sensitive else value.casefold()):matched=(number,value);break
                                    if matched:line,column,text=matched[0],0,matched[1]
                                    elif needle in (r['path'] if case_sensitive else r['path'].casefold()):line,text=0,'[path match] '+r['path']
                                    elif needle in (body if case_sensitive else body.casefold()):line,text=1,body[:1000]
                                    else:continue
                                hits.append({'host_id':HOST,'source_host':m['source_host'],'path':r['path'],'line_number':line,'column':column,'context':[],'text':text[:3000],'size':r['size'],'modified_ms':r['mtime_ns']//1000000,'created_ms':None,'body_sha256':r['sha'],'snapshot_at':m['generated_at']})
                            if len(hits)>=limit+1:break
                    finally:c.close()
                except Exception as e:incomplete.append(type(e).__name__+': '+str(e)[:180])
                if len(hits)>limit:break
            prefetched.close()
        return {'results':hits[:limit],'partial':bool(incomplete),'truncated':len(hits)>limit,'warnings':incomplete,'content_indexed':True,'scope':self.scope,'source_documents':m['documents'],'cache_bytes':self.used,'cache_limit_bytes':self.limit,'cloud_fetches':self.store.gets-reads0,'elapsed_ms':round((time.monotonic()-started)*1000,2),'complete_for_policy':m['complete_for_policy']}


def envelope(args,partial=False,error=None):
    return {'request_id':args.get('request_id') or os.urandom(12).hex(),'origin_host':args.get('origin_host') or HOST,'hop_count':int(args.get('hop_count') or 0),'host_id':HOST,'partial':partial,'truncated':False,'host_status':[{'host_id':HOST,'ok':not partial,'state':'partial' if partial else 'ok','error':error}]}


def forwarded_acl_roots(args,transport=None):
    """Return the verified read roots for this call, or None for a local read.

    A filesystem ACL is NEVER taken from the tool arguments: those are
    caller-controlled, so trusting them would let any client widen its own
    access to the historical corpus. A forwarded read (hop_count >= 1)
    therefore requires a policy verified over an authenticated transport
    (see verify_transport_acl); without such a policy the read is refused.
    A verified policy only ever narrows access.
    """
    if args.get('filesystem_acl') is not None:
        raise ValueError('Caller-supplied filesystem ACL is not trusted')
    hop=int(args.get('hop_count') or 0)
    if transport is None:
        if hop==0:return None
        raise PermissionError('Forwarded text requires a verified GPTAdmin filesystem ACL')
    if hop!=1 or not isinstance(args.get('origin_host'),str) or not args['origin_host']:
        raise PermissionError('Invalid authenticated peer origin')
    acl=transport
    if not isinstance(acl,dict) or acl.get('version')!=1:
        raise PermissionError('Forwarded text requires a verified GPTAdmin filesystem ACL')
    if (not isinstance(acl.get('profile_id'),str) or not acl['profile_id'] or
        not isinstance(acl.get('profile_version'),int) or acl['profile_version']<1 or
        not isinstance(acl.get('job_id'),str) or not acl['job_id'] or
        not isinstance(acl.get('target'),str) or not acl['target'] or
        acl.get('tool')!='mcp_call'):
        raise PermissionError('Incomplete delegated ACL identity')
    for field in ('roots_digest','args_sha256'):
        value=acl.get(field)
        if not isinstance(value,str) or not re.fullmatch('[0-9a-fA-F]{64}',value):
            raise PermissionError('Invalid delegated ACL digest')
    roots=acl.get('read_roots')
    if (not isinstance(roots,list) or not roots or
        not all(isinstance(root,str) and root.startswith('/') and
                '..' not in P(root).parts and os.path.normpath(root)==root
                for root in roots)):
        raise PermissionError('No valid delegated read roots')
    if any(root == '/' or root == '/dev' or root.startswith('/dev/') for root in roots):
        # Historical source_root='/' never authorizes filesystem read_roots='/'.
        # Device tree access is forbidden even if a relay signs the payload.
        raise PermissionError('Unrestricted root or device tree is not a permitted delegated read root')
    return roots

ACL_POLICY_HEADER='X-GrepMesh-Filesystem-Acl'
ACL_SIGNATURE_HEADER='X-GrepMesh-Acl-Signature'
ACL_SECRET_ENV='GREPMESH_CLOUD_ACL_SECRET'

def acl_secret():
    value=os.environ.get(ACL_SECRET_ENV,'').strip()
    return value or None

PRIVATE_REQUIRE_VERIFIED_ACL_ENV='GREPMESH_CLOUD_REQUIRE_VERIFIED_ACL'

def private_acl_required():
    # Private historical corpora may NEVER serve unsigned local MCP tool calls.
    # Default off preserves the existing low-risk standalone pilot contract.
    mode=os.environ.get(PRIVATE_REQUIRE_VERIFIED_ACL_ENV,'0')
    if mode not in ('0','1'):
        raise RuntimeError('Invalid private cloud ACL mode')
    if mode=='1' and (not acl_secret() or len(acl_secret())<32):
        raise RuntimeError('Private cloud reader requires a strong configured ACL signer')
    return mode=='1'

def enforce_private_transport_acl(required,method,verified_acl):
    if required and method=='tools/call' and verified_acl is None:
        raise PermissionError('Private historical cloud tool call requires a signed filesystem ACL')

def verify_transport_acl(headers,body):
    """Verify a filesystem ACL delivered over an authenticated transport.

    The policy is accepted only when an HMAC-SHA256 signature over the exact
    request body matches the shared secret that the authorized relay holds.
    Without a configured secret no forwarded request can be admitted, and a
    policy found in tool arguments is never considered (see
    forwarded_acl_roots). The signature covers the whole body, so the policy
    is bound to exactly these arguments and cannot be replayed for others.
    """
    secret=acl_secret()
    if not secret:return None
    raw=(headers.get(ACL_POLICY_HEADER) or '').strip()
    signature=(headers.get(ACL_SIGNATURE_HEADER) or '').strip().lower()
    if not raw and not signature:return None
    if not raw or not signature:
        raise PermissionError('Incomplete authenticated filesystem ACL transport')
    message=body+b'\n'+raw.encode('utf-8')
    expected=hmac.new(secret.encode('utf-8'),message,hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected,signature):
        raise PermissionError('Filesystem ACL transport signature mismatch')
    try:policy=json.loads(raw)
    except ValueError:raise PermissionError('Malformed authenticated filesystem ACL')
    if not isinstance(policy,dict):
        raise PermissionError('Malformed authenticated filesystem ACL')
    return policy

def tools():
    common={'hosts':{'anyOf':[{'type':'string'},{'type':'array','items':{'type':'string'}}]},'roots':{'type':'array','items':{'type':'string'}},'request_id':{'type':'string'},'origin_host':{'type':'string'},'hop_count':{'type':'integer'},'limit':{'type':'integer'},'max_matches':{'type':'integer'},'mode':{'type':'string'},'path_globs':{'type':'array','items':{'type':'string'}}}
    result=[]
    for name,desc in [('search','Search text and paths in the declared cloud text corpus. Supports literal text and bounded regex; regex latency is bounded and partial coverage is reported honestly when the deadline stops shard scanning.'),('search_text','Native GrepMesh peer alias for literal text search; bounded regex when mode is regex.'),('find_paths','Find paths in the declared cloud text corpus.'),('read_text','Read an exact cloud-snapshot text by path with filesystem ACL.'),('list_locations','Discover this cloud-backed text corpus.'),('search_status','Cloud coverage and bounded RAM cache status.')]:
        props=dict(common)
        if name in ['search','search_text']:props['query']={'type':'string'}
        if name=='find_paths':props.update(query={'type':'string'},pattern={'type':'string'})
        if name=='read_text':props.update(host={'type':'string'},source_host={'type':'string','description':'Original source host from the search result; required for ambiguous paths.'},path={'type':'string'},start_line={'type':'integer','minimum':1},end_line={'type':'integer','minimum':1},byte_offset={'type':'integer','minimum':0,'description':'Oversize document byte offset; returns base64 bytes and next offset.'},byte_limit={'type':'integer','minimum':1,'maximum':262144},expected_body_sha256={'type':'string','description':'Required for nonzero byte_offset; pins continuation to the same body.'})
        required=['host','path'] if name=='read_text' else (['query'] if name in ['search','search_text'] else [])
        result.append({'name':name,'description':desc,'inputSchema':{'type':'object','properties':props,'required':required},'annotations':{'readOnlyHint':True,'destructiveHint':False}})
    return result


def serve(reader,port):
    require_signed_acl=private_acl_required()  # Fail before touching cloud data.
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
                raw=self.rfile.read(length)
                # A filesystem ACL is only trusted when it arrives over an
                # authenticated transport; the tool arguments never carry it.
                transport_acl=verify_transport_acl(self.headers,raw)
                req=json.loads(raw);method=req.get('method');par=req.get('params',{})
                enforce_private_transport_acl(require_signed_acl,method,transport_acl)
                if method=='initialize':result={'protocolVersion':'2024-11-05','capabilities':{'tools':{}},'serverInfo':{'name':'grepmesh','version':'cloud-index-v1'}}
                elif method=='notifications/initialized':self.send_response(202);self.end_headers();return
                elif method=='tools/list':result={'tools':tools()}
                elif method=='ping':result={}
                elif method=='tools/call':
                    args=par.get('arguments') or {};name=par.get('name');m=reader.manifest
                    selected=args.get('hosts') or ['local'];selected=[selected] if isinstance(selected,str) else selected
                    if not any(x in ['local','*',HOST] for x in selected):raise ValueError('Wrong cloud host')
                    if int(args.get('hop_count') or 0)>1:raise ValueError('Mesh hop limit exceeded')
                    if name=='list_locations':out={**envelope(args),'target_host_id':HOST,'locations':getattr(reader,'locations',[{'host':HOST,'name':reader.scope,'path':m['source_root']}]),'source_host':m['source_host'],'content_indexed':True,'coverage_policy':m['policy']}
                    elif name=='search_status':
                        local={'host_id':HOST,'root':m['source_root'],'roots':[reader.scope],'backend':'cloud shards / contentless FTS / bounded RAM','file_count':m['documents'],'indexed_files':m['documents'],'index_db_bytes':reader.used,'index_state':'Ready','index_generation':int(m['generated_at']),'index_last_error':None,'index_directory_activity':[]}
                        out={**envelope(args),'local':local,'nodes':[local],'topology':{'freshness':None,'generation':1,'last_refresh_error':None},'persistent_index_bytes':0,'cache_limit_bytes':reader.limit,'cloud_shards':len(m['parts']),'scope':reader.scope,'coverage_policy':m['policy']}
                    elif name=='read_text':
                        if args.get('host') not in ('local',HOST):raise ValueError('Wrong cloud read host')
                        allowed=forwarded_acl_roots(args,transport_acl)
                        byte_args={k:args[k] for k in ('byte_offset','byte_limit','expected_body_sha256') if k in args}
                        if 'source_host' in args:byte_args['source_host']=args['source_host']
                        out=reader.read_text(args.get('path'),args.get('start_line',1),args.get('end_line'),authorized_roots=allowed,**byte_args)
                        out.update(envelope(args))
                        out['target_host_id']=HOST
                    elif name in ['search','search_text','find_paths']:
                        if name=='find_paths' and args.get('mode')=='regex':
                            raise ValueError('find_paths matches globs only; regex belongs to search')
                        allowed=forwarded_acl_roots(args,transport_acl)
                        out=reader.query(str(args.get('pattern',args.get('query',''))),int(args.get('limit',args.get('max_matches',20))),glob=name=='find_paths',case_sensitive=args.get('mode')=='literal',roots=args.get('roots'),path_globs=args.get('path_globs'),authorized_roots=allowed,regex=args.get('mode')=='regex')
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
    p=argparse.ArgumentParser();p.add_argument('action',choices=['scan','publish','search','serve']);p.add_argument('--root',default=str(DEFAULT_ROOT));p.add_argument('--scope',default='grepmesh-source');p.add_argument('--source-root',default=os.environ.get('GREPMESH_SOURCE_ROOT',''));p.add_argument('--source-host',default=os.environ.get('GREPMESH_SOURCE_HOST',''));p.add_argument('--cache-mib',type=int,default=512);p.add_argument('--shard-kib',type=int,default=8192);p.add_argument('--max-shard-documents',type=int,default=512);p.add_argument('--query');p.add_argument('--port',type=int,default=19435);a=p.parse_args()
    if hasattr(os,'sched_setaffinity'):os.sched_setaffinity(0,sorted(os.sched_getaffinity(0))[:2])
    os.nice(10)
    if not a.source_root or not a.source_host:raise ValueError('Explicit --source-root and --source-host are required')
    if a.action=='scan':scan_git(P(a.source_root),a.source_host,sys.stdout);return
    ensure_tmpfs(P(a.root))
    if a.action == 'publish' and os.environ.get('GREPMESH_REUSE_VERIFIED_SHARDS') == '1':
        from publish_native100_rolling_private import VerifiedImmutableReuseStore
        store=VerifiedImmutableReuseStore()
    else:
        store=CloudStore()
    if a.action=='publish':
        import fcntl
        with open(P(a.root)/'publisher.lock','w') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            m=Publisher(store,a.root,a.shard_kib*1024,a.max_shard_documents).publish(a.scope,sys.stdin)
            report={k:v for k,v in m.items() if k!='parts'};report.update(shards=len(m['parts']),cloud_bytes=sum(x['packed_bytes'] for x in m['parts']),raw_index_bytes=sum(x['raw_bytes'] for x in m['parts']),cloud_uploads=store.puts)
            (P(a.root)/'published.json').write_bytes(json_bytes(report));print(json.dumps(report,ensure_ascii=False))
    else:
        reader=Reader(store,a.root,a.scope,a.cache_mib*MIB,a.source_host,a.source_root)
        if a.action=='serve':serve(reader,a.port)
        else:print(json.dumps(reader.query(a.query or ''),ensure_ascii=False))

if __name__=='__main__':main()
