#!/usr/bin/env python3
"""Tests for oversized-body publishing and reading in the private cloud FTS.

Publishes with MemoryStore; nothing touches a native index or the default
mesh. Parity contract: reading an oversized body chunk by chunk must return
exactly body.splitlines() with honest line numbering for any range.
"""
import json,os,pathlib,time
from cloud_index import MAX_DOCUMENT,MIB,ensure_tmpfs,digest,json_bytes,valid_key,MemoryStore
from oversize_support import (CHUNK_TARGET_BYTES,MAX_CHUNK_BYTES,
    MAX_OVERSIZE_DOCUMENT,OVERSIZE_FORMAT,OversizeIndex,byte_slices,
    oversize_pointer_key,plan_chunks,publish_oversize,strip_terminator)
BASE=pathlib.Path('/run/user')/str(os.getuid())/'grepmesh-cloud-oversize-tests'
ensure_tmpfs(BASE)
checks=[]
def check(label,ok):
    assert ok,label;checks.append(label)


class CounterStore:
    """Counts downloads so an early return provably fetches zero chunks."""
    def __init__(self,inner):
        self.inner=inner;self.gets=0
    def get(self,key,limit=MAX_CHUNK_BYTES):
        self.gets+=1;return self.inner.get(key,limit)
    def put(self,key,data):self.inner.put(key,data)


class SlowStore(CounterStore):
    def get(self,key,limit=MAX_CHUNK_BYTES):
        time.sleep(0.05);return CounterStore.get(self,key,limit)


def assert_chunk_bookkeeping(chunks,expected_raw=None):
    previous=None
    for k,chunk in enumerate(chunks):
        assert chunk['index']==k+1
        assert chunk['raw_bytes']==len(chunk['text'].encode('utf-8'))
        assert chunk['sha256']==digest(chunk['text'].encode('utf-8'))
        if previous is None:
            assert chunk['starts_mid_line'] is False and chunk['start_byte']==0
        else:
            assert chunk['starts_mid_line']==previous['ends_mid_line']
            assert chunk['start_byte']==previous['start_byte']+previous['raw_bytes']
        previous=chunk
    assert chunks[-1]['ends_mid_line'] is False
    if expected_raw is not None:
        assert sum(x['raw_bytes'] for x in chunks)==expected_raw


def assert_read_parity(index,path,body):
    """Random ranges must equal body.splitlines() slices and respect guards."""
    import random
    from bisect import bisect_right
    expected=body.splitlines()
    rng=random.Random(7)
    sizes=[len(t.encode('utf-8')) for t in expected]
    prefix=[0]
    for size in sizes:prefix.append(prefix[-1]+size)
    if prefix[-1]<=MAX_DOCUMENT:
        full=index.read_text(path,start_line=1,deadline_seconds=25)
        got=full['chunks'][0]['lines']
        assert got==[{'line_number':i+1,'text':t} for i,t in enumerate(expected)]
        assert full['end_line']==len(expected) and full['start_line']==1
        assert full['body_sha256']==digest(body.encode('utf-8'))
    budget=MAX_DOCUMENT//2
    for _ in range(40):
        if not expected:break
        start=rng.randint(1,len(expected))
        # Keep the requested range inside the response byte guard.
        stop_cap=bisect_right(prefix,prefix[start-1]+budget)-1
        # The end-None suffix variant is only valid from a late enough start.
        tail_start=bisect_right(prefix,prefix[-1]-budget)  # first suffix-safe start
        if stop_cap<start:
            continue
        stop=rng.randint(start,stop_cap)
        if rng.random()<0.5 or start<tail_start:
            result=index.read_text(path,start_line=start,end_line=stop,
                                   deadline_seconds=25)
        else:
            result=index.read_text(path,start_line=start,deadline_seconds=25)
            stop=len(expected)
        assert result['chunks'][0]['lines']==[
            {'line_number':i+1,'text':expected[i]}
            for i in range(start-1,stop)],(start,stop)
        assert result['start_line']==start
        assert result['end_line']==min(stop,len(expected))


def make_body():
    """Multi-chunk body: unicode lines, exotic terminators, huge and empty lines."""
    parts=[]
    for i in range(4000):
        parts.append('line %d alpha%s data' % (i,'\u00e9' if i%3==0 else '')+'\n')
    parts[500]='\n'
    parts[501]='broken\u2028'+'\x85'+'\n'
    big='X'*700000
    parts[800]=big+'\n'
    parts[900]=big.replace('X','Y')+'\n'
    return ''.join(parts)+'tail-unterminated'


def publish_for(body,path='/src/big/oversized.log',chunk_bytes=64*1024,
                host='server-100',snapshot='snap-20261101a'):
    st=MemoryStore()
    result=publish_oversize(st,host,snapshot,[{'path':path,'body':body}],
                            chunk_bytes=chunk_bytes)
    return st,result,OversizeIndex(st,host,snapshot),path


BODY=make_body()
ST,RESULT,INDEX,PATH=publish_for(BODY)
assert RESULT['documents']==1
RAW=len(BODY.encode('utf-8'))
assert RESULT['raw_bytes']==RAW
check('oversize manifest and pointer keys shaped',
      oversize_pointer_key('server-100','snap-20261101a')==RESULT['pointer_key'] and
      RESULT['pointer_key']=='oversize/server-100/snap-20261101a/latest.json' and
      RESULT['manifest_key'].endswith('/manifest.json') and
      json.loads(ST.objects[RESULT['pointer_key']])['manifest']==RESULT['manifest_key'])
check('oversize manifest format declared',OVERSIZE_FORMAT==1)
check('oversize packing shrinks bytes',0<RESULT['packed_bytes']<RESULT['raw_bytes'])
check('oversize chunk count bounded',
      RESULT['chunks']>=RESULT['raw_bytes']//CHUNK_TARGET_BYTES and
      RESULT['chunks']<=RESULT['raw_bytes']//(64*1024)+MAX_DOCUMENT//64)
INDEX.refresh()
record=INDEX.by_path[PATH]
check('record byte accounting equals body',
      sum(x['raw_bytes'] for x in record['chunks'])==record['size_bytes']==RAW)
check('record line count equals splitlines',
      record['line_count']==len(BODY.splitlines()) and record['line_count']>1000)
assert_chunk_bookkeeping(plan_chunks(BODY,64*1024),RAW)
assert_read_parity(INDEX,PATH,BODY)
check('oversize read parity across whole body and 40 random ranges',True)

# Exotic terminators at every chunk cut position, several chunk targets.
seps=['\n','\r\n','\r','\v','\f','\x1c','\x1d','\x1e','\x85','\u2028','\u2029']
pieces=[]
for i in range(3000):
    pieces.append('sep %d ' % i + seps[i%len(seps)])
pieces.append('tail\x85'+'W'*(64*1024*2)+'\u2029')
exotic=(''.join(pieces))*8
assert len(exotic.encode('utf-8'))>MAX_DOCUMENT
for target in (CHUNK_TARGET_BYTES,64*1024,4096,1024,256):
    st,result,index,path=publish_for(exotic,'/src/big/exotic.log',target)
    assert_chunk_bookkeeping(plan_chunks(exotic,target))
    assert_read_parity(index,path,exotic)
check('exotic terminators survive every chunk target with line parity',True)

# Unterminated, empty and single-line tails.
for tail in ('','\n','abc','abc\r\n'):
    body='head\n'+('x'*(64*1024+5)+'\n')*20+tail
    st,result,index,path=publish_for(body,'/src/big/tail.log')
    assert_read_parity(index,path,body)
check('unterminated and empty tails read with line parity',True)

# strip_terminator mirrors splitlines terminator handling.
for piece,expect in (('a\n','a'),('a\r\n','a'),('a\r','a'),('a\u2028','a'),
                     ('a','a'),('',''),('a\x85','a')):
    assert strip_terminator(piece)==expect,piece
check('terminator strip parity',True)

# byte_slices: bounded UTF-8 pieces, keeping characters intact.
s='\u2028ab'+'\u00e9'*9  # 3-byte chars force a mid-character-group cut
assert b''.join(x.encode('utf-8') for x in byte_slices(s,8))==s.encode('utf-8')
assert all(len(x.encode('utf-8'))<=8 and x for x in byte_slices(s,8))
check('byte_slices respects UTF-8 bounds',True)

# Early return beyond the end never downloads a chunk nor fabricates lines.
counter=CounterStore(ST)
beyond=OversizeIndex(counter,'server-100','snap-20261101a')
empty=beyond.read_text(PATH,start_line=10**8,deadline_seconds=25)
check('start beyond line count returns empty result without chunk downloads',
      empty['chunks'][0]['lines']==[] and counter.gets==2 and
      empty['end_line']==10**8 and empty['start_line']==10**8)

# Byte guard parity with the shard reader.
try:INDEX.read_text(PATH,start_line=1,end_line=record['line_count'],
                    deadline_seconds=25)
except RuntimeError as error:check('range beyond 1 MiB guard rejected','1 MiB' in str(error))
else:raise AssertionError('1 MiB guard bypass')

# ACL fail-closed.
try:INDEX.read_text(PATH,1,2,authorized_roots=['/elsewhere'],deadline_seconds=25)
except PermissionError:check('filesystem ACL denies oversized historical path',True)
else:raise AssertionError('oversize ACL bypass')
try:INDEX.read_text(PATH,1,2,authorized_roots=['./bad'],deadline_seconds=25)
except PermissionError:check('invalid authorized roots rejected fail-closed',True)
else:raise AssertionError('invalid roots accepted')
ok_read=INDEX.read_text(PATH,1,2,authorized_roots=['/src'],deadline_seconds=25)
check('authorized root admits oversized path',
      len(ok_read['chunks'][0]['lines'])==2)

# Tamper: pointer/manifest identity, checksum, chunk key redirect, bookkeeping.
pointer_key=RESULT['pointer_key']
manifest_key=RESULT['manifest_key']
original_pointer=ST.objects.get(pointer_key)
original_manifest=ST.objects[manifest_key]

def reset_manifest():
    ST.objects[manifest_key]=original_manifest
    ST.objects[pointer_key]=original_pointer
    INDEX.clear();INDEX.refresh()

reset_manifest()
forged=json.loads(original_pointer)
forged['manifest']=valid_key('oversize/server-100/other/manifest.json')
ST.objects[pointer_key]=json_bytes(forged)
INDEX.clear()
try:INDEX.refresh()
except ValueError:check('pointer manifest redirect rejected',True)
else:raise AssertionError('oversize manifest redirect accepted')
ST.objects[pointer_key]=original_pointer;INDEX.clear();INDEX.refresh()
forged=json.loads(original_pointer);forged['sha256']='0'*64
ST.objects[pointer_key]=json_bytes(forged);INDEX.clear()
try:INDEX.refresh()
except ValueError:check('manifest checksum mismatch rejected',True)
else:raise AssertionError('corrupt manifest accepted')
ST.objects[pointer_key]=original_pointer;INDEX.clear();INDEX.refresh()
forged=json.loads(original_pointer);forged['kind']='other'
ST.objects[pointer_key]=json_bytes(forged);INDEX.clear()
try:INDEX.refresh()
except ValueError:check('wrong pointer kind rejected',True)
else:raise AssertionError('wrong pointer kind accepted')
ST.objects[pointer_key]=original_pointer;INDEX.clear();INDEX.refresh()
tampered=json.loads(original_manifest)
tampered['records'][0]['chunks'][2]['key']=valid_key(
    'oversize/server-100/snap-20261101a/' + tampered['records'][0]['body_sha256']+
    '/chunk-0009.zlib')
ST.objects[manifest_key]=json_bytes(tampered)
pforged=json.loads(original_pointer);pforged['sha256']=digest(json_bytes(tampered))
ST.objects[pointer_key]=json_bytes(pforged)
INDEX.clear()
try:INDEX.refresh()
except ValueError:check('tampered chunk key identity rejected',True)
else:raise AssertionError('redirected chunk key accepted')

for field,payload,label in (('size_bytes',record['size_bytes']+1,
                             'byte accounting mismatch'),
                            ('line_count',1,'line count regression'),
                            ('body_sha256','0'*64,'body digest identity')):
    ST.objects[manifest_key]=original_manifest
    ST.objects[pointer_key]=original_pointer
    broken=json.loads(original_manifest)
    broken['records'][0]=dict(broken['records'][0]);broken['records'][0][field]=payload
    ST.objects[manifest_key]=json_bytes(broken)
    pforged=json.loads(original_pointer);pforged['sha256']=digest(json_bytes(broken))
    ST.objects[pointer_key]=json_bytes(pforged)
    fresh=OversizeIndex(ST,'server-100','snap-20261101a')
    try:fresh.read_text(PATH,1,2,deadline_seconds=25)
    except ValueError:check(label+' caught by manifest validation',True)
    else:raise AssertionError(label+' accepted')
reset_manifest()
full_after=INDEX.read_text(PATH,5,9,deadline_seconds=25)
check('recovered index still reads exactly',
      full_after['chunks'][0]['lines'][-1]['text']==BODY.splitlines()[8])
# Corrupt chunk fails loudly even with a valid index structure.
chunk_keys=sorted(k for k in ST.objects if k.endswith('.zlib'))
victim=chunk_keys[0]
original=ST.objects.get(victim)
ST.objects.pop(victim)
INDEX.clear();INDEX.refresh()
try:INDEX.read_text(PATH,1,record['line_count'],deadline_seconds=25)
except (RuntimeError,KeyError):check('missing chunk fails loudly',True)
else:raise AssertionError('missing chunk silently accepted')
ST.objects[victim]=original
ST.objects[victim]=original[:-1]+bytes([original[-1]^0xff])
try:INDEX.read_text(PATH,1,3,deadline_seconds=25)
except RuntimeError as error:check('corrupt chunk integrity failure','integrity' in str(error))
else:raise AssertionError('corrupt chunk silently accepted')
ST.objects[victim]=original

# Missing index and argument validation.
fresh=OversizeIndex(MemoryStore(),'server-100','snap-missing')
try:fresh.refresh()
except (FileNotFoundError,KeyError):check('missing oversize pointer fails honestly',True)
else:raise AssertionError('missing pointer silently refreshed')
try:publish_oversize(MemoryStore(),'server-100','s',
                     [{'path':'/a','body':'x'*MAX_DOCUMENT}])
except ValueError:check('sub-shard-limit body rejected',True)
else:raise AssertionError('publisher accepted small body')
nearly=publish_oversize(MemoryStore(),'server-100','s',
                        [{'path':'/a','body':'x'*(MAX_DOCUMENT+1)}])
check('minimal oversized body accepted above the limit',
      nearly['documents']==1 and nearly['chunks']>=2)
try:publish_oversize(MemoryStore(),'server-100','s',
                     [{'path':'/a','body':'x'*(MAX_OVERSIZE_DOCUMENT+1)}])
except ValueError:check('oversize cap enforced',True)
else:raise AssertionError('publisher accepted an over-cap body')
try:publish_oversize(MemoryStore(),'server-100','s',
                     [{'path':PATH,'body':BODY,'sha256':'0'*64}])
except ValueError:check('declared digest mismatch rejected',True)
else:raise AssertionError('digest mismatch accepted')
try:publish_oversize(MemoryStore(),'bad/host','s',[{'path':PATH,'body':BODY}])
except ValueError:check('host identity validated',True)
else:raise AssertionError('bad host accepted')
try:publish_oversize(MemoryStore(),'server-100','s',
                     [{'path':'../relative','body':BODY}])
except ValueError:check('publisher path validation',True)
else:raise AssertionError('bad path accepted')
try:plan_chunks(BODY,0)
except ValueError:check('chunk bound validated',True)
else:raise AssertionError('invalid chunk bound accepted')
try:plan_chunks(None)
except ValueError:check('non-string body rejected',True)
else:raise AssertionError('non-string body accepted')
try:INDEX.read_text('relative/path',1,2,deadline_seconds=25)
except ValueError:check('relative path rejected',True)
else:raise AssertionError('relative path accepted')
try:INDEX.read_text(PATH,0,2,deadline_seconds=25)
except ValueError:check('start_line validation',True)
else:raise AssertionError('bad start_line accepted')
try:INDEX.read_text(PATH,1,2,deadline_seconds=0)
except ValueError:check('deadline validation',True)
else:raise AssertionError('bad deadline accepted')

# Deadline is honored between chunk downloads.
slow=OversizeIndex(SlowStore(ST),'server-100','snap-20261101a')
try:slow.read_text(PATH,1,record['line_count'],deadline_seconds=0.02)
except TimeoutError:check('oversize read deadline honored mid-chain',True)
else:raise AssertionError('deadline not honored')

# Deterministic re-publish restores the same chunk-object bytes.
again=publish_oversize(ST,'server-100','snap-20261101a',
                       [{'path':PATH,'body':BODY}],chunk_bytes=64*1024)
check('republish keeps every manifest identity field',
      again['manifest_key']==RESULT['manifest_key'] and
      again['pointer_key']==RESULT['pointer_key'] and
      again['chunks']==RESULT['chunks'] and
      again['raw_bytes']==RESULT['raw_bytes'])
INDEX.clear();INDEX.refresh()
assert INDEX.read_text(PATH,1,1,deadline_seconds=25)['chunks'][0]['lines'][0][
    'text']==BODY.splitlines()[0]
check('republished index still reads',True)

print(str(len(checks))+' passed')
print('PASS')
