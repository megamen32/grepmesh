#!/usr/bin/env python3
"""Fail-closed filesystem ACL tests for the cloud MCP peer.

A filesystem ACL must never be trusted from client-controlled tool
arguments. These tests prove that:

- a fabricated ACL inside the arguments is refused;
- a forwarded read (hop_count >= 1) without an authenticated transport
  policy is refused;
- an authenticated transport policy (HMAC over body+policy) is accepted
  and only narrows the readable corpus;
- a forged signature, a swapped policy and a replayed signature for
  different arguments are refused.

Everything runs against an in-process loopback MCP server and a temporary
tmpfs corpus. No cloud, no native index, no production service.
"""
import hashlib,hmac,json,os,pathlib,socket,threading,time,urllib.error,urllib.request
from cloud_index import (MemoryStore,Publisher,Reader,document,ensure_tmpfs,json_bytes,
                         serve,ACL_POLICY_HEADER,ACL_SIGNATURE_HEADER,ACL_SECRET_ENV,
                         forwarded_acl_roots,verify_transport_acl)
BASE=pathlib.Path('/run/user')/str(os.getuid())/'grepmesh-cloud-acl-tests'
ensure_tmpfs(BASE)
checks=[]
def check(label,ok):
    assert ok,label;checks.append(label)
SECRET='unit-test-shared-secret'
os.environ[ACL_SECRET_ENV]=SECRET

def policy(read_roots):
    return {'version':1,'profile_id':'p1','profile_version':1,'roots_digest':'0'*64,
            'args_sha256':'0'*64,'job_id':'j1','target':'shell:test','tool':'mcp_call',
            'read_roots':list(read_roots)}

def signed_headers(body,pol):
    raw=json.dumps(pol,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    message=body+b'\n'+raw.encode('utf-8')
    sig=hmac.new(SECRET.encode(),message,hashlib.sha256).hexdigest()
    return {ACL_POLICY_HEADER:raw,ACL_SIGNATURE_HEADER:sig}

# Unit level: arguments never carry a trusted ACL.
try:forwarded_acl_roots({'filesystem_acl':policy(['/'])})
except ValueError:check('caller-supplied ACL in arguments is refused',True)
else:raise AssertionError('caller-supplied ACL accepted')
try:forwarded_acl_roots({'hop_count':1,'origin_host':'peer'},None)
except PermissionError:check('forwarded read without transport policy refused',True)
else:raise AssertionError('unauthenticated forwarded read accepted')
check('local read without ACL stays allowed',
      forwarded_acl_roots({'hop_count':0},None) is None)
roots=forwarded_acl_roots({'hop_count':1,'origin_host':'peer'},policy(['/fixture']))
check('verified transport policy yields its read roots',roots==['/fixture'])
try:forwarded_acl_roots({'hop_count':1,'origin_host':'peer'},{'version':2})
except PermissionError:check('wrong ACL version refused',True)
else:raise AssertionError('bad ACL version accepted')
try:forwarded_acl_roots({'hop_count':1,'origin_host':'peer'},
                        dict(policy(['/fixture']),read_roots=[]))
except PermissionError:check('empty read roots refused',True)
else:raise AssertionError('empty read roots accepted')
try:forwarded_acl_roots({'hop_count':1,'origin_host':'peer'},
                        dict(policy(['/fixture']),read_roots=['relative']))
except PermissionError:check('relative read root refused',True)
else:raise AssertionError('relative read root accepted')
try:forwarded_acl_roots({'hop_count':1,'origin_host':''},policy(['/fixture']))
except PermissionError:check('missing peer origin refused',True)
else:raise AssertionError('missing origin accepted')

# Transport verification: signature covers body AND policy.
body=b'{"jsonrpc":"2.0"}'
pol=policy(['/fixture'])
headers=signed_headers(body,pol)
check('valid signed transport policy verified',
      verify_transport_acl(headers,body)==pol)
try:verify_transport_acl({ACL_POLICY_HEADER:headers[ACL_POLICY_HEADER],
                          ACL_SIGNATURE_HEADER:'0'*64},body)
except PermissionError:check('forged signature refused',True)
else:raise AssertionError('forged signature accepted')
try:verify_transport_acl(headers,body+b'x')
except PermissionError:check('signature bound to the exact body refused',True)
else:raise AssertionError('body substitution accepted')
wider=signed_headers(body,policy(['/']))
check('a separately signed policy is valid for its own policy',
      verify_transport_acl(wider,body)['read_roots']==['/'])
# A policy swapped under a valid signature for another policy must fail:
# the signature covers the policy bytes, not only the body.
narrow_raw=json.dumps(policy(['/fixture']),ensure_ascii=False,sort_keys=True,
                      separators=(',',':'))
wider_raw=json.dumps(policy(['/']),ensure_ascii=False,sort_keys=True,
                     separators=(',',':'))
swapped={ACL_POLICY_HEADER:wider_raw,
         ACL_SIGNATURE_HEADER:hmac.new(
             SECRET.encode(),body+b'\n'+narrow_raw.encode(),hashlib.sha256).hexdigest()}
try:verify_transport_acl(swapped,body)
except PermissionError:check('policy swapped under a valid signature refused',True)
else:raise AssertionError('swapped wider policy accepted')
try:verify_transport_acl({ACL_POLICY_HEADER:headers[ACL_POLICY_HEADER]},body)
except PermissionError:check('missing signature refused',True)
else:raise AssertionError('unsigned policy accepted')

# End to end over the loopback MCP server.
import io,tempfile
def stream(root,host,paths):
    rows=[{'kind':'begin','format':1,'source_host':host,'source_root':root,'policy':'fixture'}]
    dig=hashlib.sha256()
    for p,b in paths:
        d=document(root+'/'+p,b);rows.append(d);dig.update(json_bytes([d['path'],d['sha256']]))
    rows.append({'kind':'end','count':len(paths),'logical_sha256':dig.hexdigest(),
                 'skipped':{},'complete_for_policy':True})
    return io.StringIO(''.join(json.dumps(x,ensure_ascii=False)+'\n' for x in rows))
with tempfile.TemporaryDirectory(dir=BASE) as tmp:
    store=MemoryStore();pub=Publisher(store,tmp,64*1024)
    pub.publish('acl',stream('/fixture','server-100',[('one.md','alpha secret one'),
                                                      ('two.md','alpha secret two')]))
    reader=Reader(store,tmp,'acl',64*1024*1024,'server-100','/fixture')
    reader.refresh()
    probe=socket.socket();probe.bind(('127.0.0.1',0));port=probe.getsockname()[1];probe.close()
    server=threading.Thread(target=serve,args=(reader,port),daemon=True);server.start()
    for _ in range(100):
        try:urllib.request.urlopen('http://127.0.0.1:%d'%port,timeout=1).read();break
        except urllib.error.URLError:time.sleep(0.05)
    def rpc(name,arguments,extra_headers=None,raw_body=None):
        body=raw_body if raw_body is not None else json_bytes(
            {'jsonrpc':'2.0','id':7,'method':'tools/call',
             'params':{'name':name,'arguments':arguments}})
        headers={'Content-Type':'application/json'};headers.update(extra_headers or {})
        out=urllib.request.urlopen(urllib.request.Request(
            'http://127.0.0.1:%d'%port,data=body,headers=headers),timeout=10).read()
        return json.loads(out)
    def inner(response):
        return json.loads(response['result']['content'][0]['text'])
    def is_error(response):
        return response['result'].get('isError') is True
    # 1) Fabricated ACL inside the arguments must not grant access.
    forged=dict(policy(['/fixture']),read_roots=['/fixture'])
    response=rpc('search',{'query':'alpha','hosts':['local'],'hop_count':1,
                           'origin_host':'attacker','filesystem_acl':forged})
    check('fabricated ACL in arguments is refused at the peer',
          is_error(response) and 'not trusted' in response['result']['content'][0]['text'])
    response=rpc('read_text',{'host':'local','path':'/fixture/one.md',
                              'hop_count':1,'origin_host':'attacker',
                              'filesystem_acl':forged})
    check('fabricated ACL cannot unlock a historical read',
          is_error(response) and 'not trusted' in response['result']['content'][0]['text'])
    # 2) Forwarded read without any transport policy is refused.
    response=rpc('search',{'query':'alpha','hosts':['local'],'hop_count':1,
                           'origin_host':'peer'})
    check('forwarded search without authenticated policy refused',
          is_error(response) and 'verified GPTAdmin filesystem ACL'
          in response['result']['content'][0]['text'])
    response=rpc('find_paths',{'pattern':'*.md','hosts':['local'],
                               'hop_count':1,'origin_host':'peer'})
    check('forwarded find_paths without authenticated policy refused',
          is_error(response) and 'verified GPTAdmin filesystem ACL'
          in response['result']['content'][0]['text'])
    # Path listing must be narrowed by the same signed ACL as text search.
    path_args={'pattern':'*.md','hosts':['local'],'hop_count':1,'origin_host':'peer'}
    path_body=json_bytes({'jsonrpc':'2.0','id':7,'method':'tools/call',
                          'params':{'name':'find_paths','arguments':path_args}})
    response=rpc('find_paths',path_args,
                 signed_headers(path_body,policy(['/fixture/one.md'])),path_body)
    check('signed find_paths lists only the verified file root',
          not is_error(response) and
          [r['path'] for r in inner(response)['results']]==['/fixture/one.md'])
    # 3) A correctly signed transport policy is accepted and narrows access.
    arguments={'query':'alpha','hosts':['local'],'hop_count':1,'origin_host':'peer'}
    body=json_bytes({'jsonrpc':'2.0','id':7,'method':'tools/call',
                      'params':{'name':'search','arguments':arguments}})
    response=rpc('search',arguments,signed_headers(body,policy(['/fixture'])),body)
    result=inner(response)
    check('authenticated policy is accepted for a forwarded search',
          not is_error(response) and len(result['results'])==2)
    # Narrowing to a single file must hide the other one.
    body=json_bytes({'jsonrpc':'2.0','id':7,'method':'tools/call',
                      'params':{'name':'search',
                                'arguments':dict(arguments,path_globs=['*/two.md'])}})
    response=rpc('search',dict(arguments,path_globs=['*/two.md']),
                 signed_headers(body,policy(['/fixture'])),body)
    result=inner(response)
    check('authenticated policy does not broaden beyond the corpus',
          len(result['results'])==1 and result['results'][0]['path']=='/fixture/two.md')
    # 4) A signature issued for other arguments must not be replayed.
    other_body=json_bytes({'jsonrpc':'2.0','id':7,'method':'tools/call',
                           'params':{'name':'search',
                                     'arguments':dict(arguments,query='alpha secret one')}})
    response=rpc('search',arguments,signed_headers(other_body,policy(['/fixture'])),
                 json_bytes({'jsonrpc':'2.0','id':7,'method':'tools/call',
                             'params':{'name':'search','arguments':arguments}}))
    check('signature replay for different arguments refused',
          is_error(response) and 'signature mismatch' in response['result']['content'][0]['text'])
    # 5) Local read (hop_count 0) keeps working without any ACL.
    response=rpc('search',{'query':'alpha','hosts':['local']})
    check('local read without ACL still works',not is_error(response) and
          len(inner(response)['results'])==2)

print(str(len(checks))+' passed')
print('PASS')
