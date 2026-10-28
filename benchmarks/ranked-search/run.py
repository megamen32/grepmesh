#!/usr/bin/env python3
"""Reproducible stdlib-only MCP search benchmark; never retries a request."""
from __future__ import annotations
import argparse, json, math, os, statistics, sys, time, urllib.error, urllib.request, subprocess, threading, datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent

def percentile(values, p):
    if not values: return None
    xs = sorted(values); k = (len(xs)-1)*p/100
    lo = math.floor(k); hi = math.ceil(k)
    return xs[lo] if lo == hi else xs[lo] + (xs[hi]-xs[lo])*(k-lo)

def compact_text(payload):
    if isinstance(payload, dict):
        bits=[]
        for c in payload.get('content', []):
            if isinstance(c, dict) and c.get('type') == 'text': bits.append(c.get('text',''))
        return '\n'.join(bits) or json.dumps(payload, ensure_ascii=False)
    return str(payload)

def parse_wire(raw, content_type):
    if 'text/event-stream' in content_type:
        data=[]
        for line in raw.decode('utf-8','replace').splitlines():
            if line.startswith('data:'): data.append(line[5:].lstrip())
        if not data: raise RuntimeError('MCP returned no SSE data event')
        raw='\n'.join(data).encode()
    return json.loads(raw)

def host_snapshot():
    snap={'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    try:
        a=open('/proc/loadavg',encoding='utf-8').read().split(); snap['loadavg']={'1m':float(a[0]),'5m':float(a[1]),'15m':float(a[2]),'runnable':a[3]}
    except Exception: pass
    try:
        vals={line.split(':',1)[0]:int(line.split()[1]) for line in open('/proc/meminfo',encoding='utf-8') if ':' in line and len(line.split())>1}
        snap['mem_available_kib']=vals.get('MemAvailable')
    except Exception: pass
    try:
        row=open('/proc/stat',encoding='utf-8').readline().split(); nums=[int(x) for x in row[1:]]
        snap['cpu_total_ticks']=sum(nums); snap['cpu_idle_ticks']=sum(nums[3:5])
    except Exception: pass
    for label,path in [('cpu','/proc/pressure/cpu'),('memory','/proc/pressure/memory'),('io','/proc/pressure/io')]:
        pressure={}
        try:
            for line in open(path,encoding='utf-8'):
                parts=line.split(); kind=parts[0]; fields={k:(int(v) if k=='total' else float(v)) for k,v in (part.split('=',1) for part in parts[1:] if '=' in part)}
                pressure[kind]=fields
            snap['psi_'+label]=pressure
        except Exception: pass
    return snap

class HostSampler:
    def __init__(self,interval=1.0):
        self.interval=interval; self.before=host_snapshot(); self.samples=[self.before]; self.stop_event=threading.Event(); self.thread=threading.Thread(target=self._run,daemon=True)
    def start(self): self.thread.start()
    def _run(self):
        while not self.stop_event.wait(self.interval): self.samples.append(host_snapshot())
    def stop(self):
        self.stop_event.set(); self.thread.join(timeout=3); self.after=host_snapshot(); self.samples.append(self.after)
        for prev,cur in zip(self.samples,self.samples[1:]):
            total=cur.get('cpu_total_ticks',0)-prev.get('cpu_total_ticks',0); idle=cur.get('cpu_idle_ticks',0)-prev.get('cpu_idle_ticks',0)
            if total>0: cur['cpu_busy_percent']=round(100*(total-idle)/total,2)
        return {'before':self.before,'after':self.after,'samples':self.samples,'sample_count':len(self.samples),'interval_seconds':self.interval}

class StdioMCP:
    """Persistent newline-delimited JSON-RPC MCP client for a local stdio server."""
    def __init__(self, command, endpoint, label):
        env=os.environ.copy(); env['XERJ_URL']=endpoint
        if os.getenv('XERJ_AUTH'): env['XERJ_AUTH']=os.environ['XERJ_AUTH']
        self.proc=subprocess.Popen([command,'mcp'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True,env=env,bufsize=1)
        self.label=label; self.n=0
    def call(self,method,params=None,notification=False):
        self.n+=1; msg={'jsonrpc':'2.0','method':method,'params':params or {}}
        if not notification: msg['id']=self.n
        self.proc.stdin.write(json.dumps(msg)+'\n'); self.proc.stdin.flush()
        if notification: return None
        while True:
            line=self.proc.stdout.readline()
            if not line: raise RuntimeError('MCP stdio '+self.label+' exited without response')
            try: response=json.loads(line)
            except json.JSONDecodeError: continue
            if response.get('id')==self.n:
                if 'error' in response: raise RuntimeError('MCP '+self.label+' error: '+json.dumps(response['error'])[:500])
                return response.get('result',response)
    def initialize(self):
        self.call('initialize',{'protocolVersion':'2025-03-26','capabilities':{},'clientInfo':{'name':'ranked-search-benchmark','version':'1.0'}})
        self.call('notifications/initialized',notification=True)
    def tool(self,name,args): return self.call('tools/call',{'name':name,'arguments':args})
    def close(self):
        if self.proc.poll() is None: self.proc.terminate()
        self.proc.wait(timeout=5)

class MCP:
    def __init__(self, endpoint, token, label):
        self.endpoint=endpoint; self.label=label; self.n=0; self.session_id=None; self.protocol_ready=False
        self.headers={'Content-Type':'application/json','Accept':'application/json, text/event-stream'}
        if token: self.headers['Authorization']=token if token.lower().startswith(('bearer ','basic ')) else 'Bearer '+token
    def call(self, method, params=None, notification=False):
        self.n += 1
        msg={'jsonrpc':'2.0','method':method,'params':params or {}}
        if not notification: msg['id']=self.n
        headers=dict(self.headers)
        if self.session_id: headers['Mcp-Session-Id']=self.session_id
        if self.protocol_ready: headers['MCP-Protocol-Version']='2025-03-26'
        req=urllib.request.Request(self.endpoint,data=json.dumps(msg).encode(),headers=headers,method='POST')
        with urllib.request.urlopen(req,timeout=60) as resp:
            body=resp.read(); ctype=resp.headers.get('Content-Type',''); self.session_id=resp.headers.get('Mcp-Session-Id',self.session_id)
        if notification: return None
        result=parse_wire(body,ctype)
        if 'error' in result: raise RuntimeError('MCP '+self.label+' error: '+json.dumps(result['error'])[:500])
        return result.get('result',result)
    def initialize(self):
        self.call('initialize',{'protocolVersion':'2025-03-26','capabilities':{},'clientInfo':{'name':'ranked-search-benchmark','version':'1.0'}})
        self.call('notifications/initialized',notification=True); self.protocol_ready=True
    def tool(self,name,args): return self.call('tools/call',{'name':name,'arguments':args})

def normalize_path(s):
    return str(s).replace('\\','/').lstrip('./')

def paths_in(value, out):
    if isinstance(value, dict):
        for k,v in value.items():
            if k in ('path','ax_path','file_path') and isinstance(v,str): out.append(normalize_path(v))
            else: paths_in(v,out)
    elif isinstance(value,list):
        for x in value: paths_in(x,out)

def locators_in(value, out):
    if isinstance(value,dict):
        path=value.get('path') or value.get('ax_path') or value.get('file_path')
        if isinstance(path,str):
            hit={'path':normalize_path(path),'line':value.get('match_line') or value.get('line_number') or value.get('line') or value.get('loc') or value.get('start_line')}
            for k in ('score','_score','freshness','coverage','coverage_status','coverage_ratio','strength','match_quality','match_strength','match_kind','matched_terms','index_generation','host_id','strong','weak'):
                if k in value and isinstance(value[k],(str,int,float,bool,list,dict)): hit['score' if k=='_score' else k]=value[k]
            out.append(hit)
        for v in value.values(): locators_in(v,out)
    elif isinstance(value,list):
        for v in value: locators_in(v,out)

def unwrap_json(value):
    if isinstance(value,dict):
        for c in value.get('content',[]):
            if isinstance(c,dict) and c.get('type')=='text':
                try: return json.loads(c['text'])
                except Exception: pass
    return value

def score(task, result):
    obj=unwrap_json(result); found=[]; paths_in(obj,found); locs=[]; locators_in(obj,locs)
    # Stable de-dup while preserving retrieval order.
    found=list(dict.fromkeys(found)); unique_locs=[]; seen=set()
    for hit in locs:
        if hit['path'] not in seen: unique_locs.append(hit); seen.add(hit['path'])
    gold=[normalize_path(p) for p in task['gold_paths']]
    ranks=[i+1 for i,p in enumerate(found) if any(p.endswith(g) for g in gold)]
    rr=1/min(ranks) if ranks else 0.0
    strong_ranks=[i+1 for i,p in enumerate(found) if any(p.endswith(g) for g in gold) and i<len(unique_locs) and unique_locs[i].get('match_quality')=='strong_match']
    weak_ranks=[i+1 for i,p in enumerate(found) if any(p.endswith(g) for g in gold) and i<len(unique_locs) and unique_locs[i].get('match_quality')=='weak_match']
    strong_hits=[h for h in unique_locs if h.get('match_quality')=='strong_match']; weak_hits=[h for h in unique_locs if h.get('match_quality')=='weak_match']
    dcg=sum(1/math.log2(r+1) for r in ranks if r<=10)
    ideal=sum(1/math.log2(i+2) for i in range(min(len(gold),10)))
    return {'paths':found[:10], 'locators':unique_locs[:10], 'relevant_ranks':ranks, 'strong_relevant_ranks':strong_ranks, 'weak_relevant_ranks':weak_ranks, 'strong_negative_hit':bool(strong_hits) if not task['answerable'] else None, 'weak_only_negative_hit':bool(weak_hits) and not bool(strong_hits) if not task['answerable'] else None, 'rr':rr, 'ndcg10':dcg/ideal if ideal else None,
            'recall5':len([r for r in ranks if r<=5])/max(len(gold),1) if gold else None,
            'negative_hit':bool(found) if not task['answerable'] else None}

def summarize(records):
    quality=[r for r in records if r.get('ok') and r.get('rep')==0]
    good=[r for r in records if r.get('ok') and r.get('temperature')=='warm']; lats=[r['elapsed_ms'] for r in good]
    server=[r['server_ms'] for r in good if isinstance(r.get('server_ms'),(int,float))]
    overhead=[r['elapsed_ms']-r['server_ms'] for r in good if isinstance(r.get('server_ms'),(int,float))]
    mrr=[r['score']['rr'] for r in quality]
    ndcg=[r['score']['ndcg10'] for r in quality if r['score']['ndcg10'] is not None]
    recall=[r['score']['recall5'] for r in quality if r['score']['recall5'] is not None]
    negative=[r['score']['negative_hit'] for r in quality if r['score']['negative_hit'] is not None]
    strong_rr=[1/min(r['score']['strong_relevant_ranks']) if r['score']['strong_relevant_ranks'] else 0.0 for r in quality if r['score'].get('strong_relevant_ranks') is not None]
    weak_rr=[1/min(r['score']['weak_relevant_ranks']) if r['score']['weak_relevant_ranks'] else 0.0 for r in quality if r['score'].get('weak_relevant_ranks') is not None]
    negative_strong=[r['score']['strong_negative_hit'] for r in quality if r['score'].get('strong_negative_hit') is not None]
    negative_weak_only=[r['score']['weak_only_negative_hit'] for r in quality if r['score'].get('weak_only_negative_hit') is not None]
    coverages=[str(r.get('coverage')) for r in quality if r.get('coverage') is not None]
    return {'quality_n':len(quality),'latency_n':len(good),'errors':sum(not r.get('ok') for r in records),
            'latency_ms':{'p50':round(percentile(lats,50),2) if lats else None,'p95':round(percentile(lats,95),2) if lats else None,'p99':round(percentile(lats,99),2) if lats else None},
            'mrr':round(statistics.mean(mrr),4) if mrr else None,
            'ndcg10':round(statistics.mean(ndcg),4) if ndcg else None,
            'recall5':round(statistics.mean(recall),4) if recall else None,
            'negative_false_positive_rate':round(statistics.mean(negative),4) if negative else None,
            'strong_mrr':round(statistics.mean(strong_rr),4) if strong_rr else None,
            'weak_mrr':round(statistics.mean(weak_rr),4) if weak_rr else None,
            'negative_strong_false_positive_rate':round(statistics.mean(negative_strong),4) if negative_strong else None,
            'negative_weak_only_rate':round(statistics.mean(negative_weak_only),4) if negative_weak_only else None,
            'response_coverage_counts':{k:coverages.count(k) for k in sorted(set(coverages))} if coverages else {},
            'engine_ms':{'p50':round(percentile(server,50),2) if server else None,'p95':round(percentile(server,95),2) if server else None},
            'client_minus_engine_ms':{'p50':round(percentile(overhead,50),2) if overhead else None,'p95':round(percentile(overhead,95),2) if overhead else None}}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--corpus',default=str(HERE/'corpus.json')); ap.add_argument('--out',default=str(HERE/'results.jsonl'))
    ap.add_argument('--repetitions',type=int,default=30); ap.add_argument('--limit',type=int,default=0,help='benchmark first N tasks only'); ap.add_argument('--latency-tasks',default='a01,a02,d04',help='comma-separated task IDs to repeat for latency; other tasks run once')
    ap.add_argument('--grepmesh-url',default=os.getenv('GREPMESH_MCP_URL')); ap.add_argument('--xerj-url',default=os.getenv('XERJ_URL','http://127.0.0.1:19200')); ap.add_argument('--xerj-transport',choices=['stdio','http'],default='stdio'); ap.add_argument('--xerj-bin',default=os.getenv('XERJ_BIN','xerj'))
    ap.add_argument('--grepmesh-tool',choices=['search','ranked_search'],default='search'); ap.add_argument('--grepmesh-label',default=None); ap.add_argument('--grepmesh-mode',choices=['literal','case_insensitive_literal','regex'],default='case_insensitive_literal'); ap.add_argument('--grepmesh-limit',type=int,default=10); ap.add_argument('--grepmesh-max-matches',type=int,default=100); ap.add_argument('--grepmesh-wait-ms',type=int,default=1000)
    ap.add_argument('--grepmesh-auth-env',default='GREPMESH_MCP_AUTH'); ap.add_argument('--xerj-auth-env',default='XERJ_MCP_AUTH'); ap.add_argument('--skip-xerj',action='store_true',help='run only GrepMesh mode when a fixed XERJ baseline receipt already exists')
    ap.add_argument('--index',default='serversadmin-docs'); ap.add_argument('--host-metrics-out',default=None)
    a=ap.parse_args()
    if a.repetitions < 30: ap.error('--repetitions must be >=30 for reported latency')
    if not a.grepmesh_url: ap.error('set GREPMESH_MCP_URL to the supported GrepMesh MCP Streamable HTTP endpoint')
    corpus=json.load(open(a.corpus,encoding='utf-8')); tasks=corpus['tasks'][:a.limit or None]; latency_tasks=set(filter(None,a.latency_tasks.split(',')))
    gm=MCP(a.grepmesh_url,os.getenv(a.grepmesh_auth_env), 'GrepMesh'); gm_label=a.grepmesh_label or ('grepmesh' if a.grepmesh_tool=='search' else 'grepmesh-'+a.grepmesh_tool); xe=None if a.skip_xerj else (StdioMCP(a.xerj_bin,a.xerj_url,'XERJ') if a.xerj_transport=='stdio' else MCP(a.xerj_url,os.getenv(a.xerj_auth_env),'XERJ'))
    # Initialize and gate GrepMesh before opening the XERJ client.
    init={}; t=time.perf_counter(); gm.initialize(); init['grepmesh']=round((time.perf_counter()-t)*1000,2)
    status=unwrap_json(gm.tool('search_status',{'hosts':['local']})); node=status.get('local') or {}
    expected=corpus['corpus_scope']['grepmesh_scope']['root']
    if node.get('index_state')!='Ready' or int(node.get('indexed_files') or 0)<=0 or node.get('root')!=expected:
        raise SystemExit(f"baseline/candidate preflight rejected: index_state={node.get('index_state')}, indexed_files={node.get('indexed_files')}, root={node.get('root')}")
    probe=unwrap_json(gm.tool('search',{'query':'s21-lease','hosts':['local'],'mode':'case_insensitive_literal','max_matches':5,'wait_ms':1000}))
    while probe.get('state')=='running' and probe.get('job_id'):
        time.sleep(min(max(probe.get('next_poll_after_ms',250)/1000,0.1),1.0)); probe=unwrap_json(gm.tool('search_status',{'job_id':probe['job_id'],'hosts':['local'],'page_size':10}))
    if not any(str(hit.get('path','')).endswith('docs/inventory/devices/android.md') for hit in probe.get('results',[])) or probe.get('partial') or probe.get('truncated'):
        raise SystemExit('baseline/candidate preflight rejected: indexed literal s21-lease probe did not return the expected Android docs path completely')
    if xe is not None:
        t=time.perf_counter(); xe.initialize(); init['xerj']=round((time.perf_counter()-t)*1000,2)
    preflight={'index_state':node['index_state'],'indexed_files':node['indexed_files'],'generation':node.get('index_generation'),'root':node['root'],'literal_probe_hit':True,'literal_probe_truncated':probe.get('truncated',False)}
    sampler=HostSampler(); sampler.start()
    Path(a.out).parent.mkdir(parents=True,exist_ok=True); rows=[]
    with open(a.out,'w',encoding='utf-8') as out:
        engine_runs=[(gm_label,'grepmesh',gm)]
        if xe is not None: engine_runs.append((('xerj' if gm_label!='xerj' else 'xerj-control'),'xerj',xe))
        for engine,backend,client in engine_runs:
            for task in tasks:
                if backend=='grepmesh' and a.grepmesh_tool=='ranked_search':
                    args={'query':task['query'],'hosts':['local'],'limit':a.grepmesh_limit,'context_lines':4}; tool=a.grepmesh_tool
                elif backend=='grepmesh':
                    args={'query':task['query'],'hosts':['local'],'mode':a.grepmesh_mode,'max_matches':a.grepmesh_max_matches,'context_lines':0,'wait_ms':a.grepmesh_wait_ms}; tool=a.grepmesh_tool
                else:
                    args={'index':a.index,'query':{'bool':{'must':[{'multi_match':{'query':task['query'],'fields':['title','body','headings']}}],'filter':[corpus['corpus_scope']['xerj_filter']]}},'_source':['ax_path','title','start_line','end_line'],'size':10}; tool='xerj_search'
                samples=[]; task_rows=[]; repetitions=a.repetitions if task['id'] in latency_tasks else 0
                # First call is recorded separately; selected latency tasks then get 30+ warm repeats.
                for rep in range(1+repetitions):
                    start=time.perf_counter(); ok=True; err=None; result=None
                    try:
                        result=client.tool(tool,args)
                        if isinstance(result,dict) and result.get('isError'): raise RuntimeError(compact_text(result)[:500])
                        if backend=='grepmesh':
                            obj=unwrap_json(result)
                            if isinstance(obj,dict) and obj.get('state')=='running' and obj.get('job_id'):
                                deadline=time.monotonic()+60
                                while obj.get('state')=='running' and time.monotonic()<deadline:
                                    time.sleep(min(max(obj.get('next_poll_after_ms',250)/1000,0.1),1.0))
                                    result=client.tool('search_status',{'job_id':obj['job_id'],'hosts':['local'],'page_size':100})
                                    if isinstance(result,dict) and result.get('isError'): raise RuntimeError(compact_text(result)[:500])
                                    obj=unwrap_json(result)
                                if obj.get('state')=='running': raise TimeoutError('GrepMesh job still running after 60s; not replayed')
                    except (urllib.error.URLError,TimeoutError,RuntimeError,ValueError) as e: ok=False; err=f'{type(e).__name__}: {e}'[:500]
                    elapsed=(time.perf_counter()-start)*1000
                    row={'engine':engine,'task_id':task['id'],'class':task['class'],'rep':rep,'temperature':'first_or_uncached' if rep==0 else 'warm','elapsed_ms':round(elapsed,3),'ok':ok}
                    if ok:
                        result_obj=unwrap_json(result)
                        if isinstance(result_obj,dict):
                            row['partial']=result_obj.get('partial'); row['truncated']=result_obj.get('truncated'); row['timed_out']=result_obj.get('timed_out')
                            row['shards_failed']=(result_obj.get('_shards') or {}).get('failed')
                            for k in ('coverage','coverage_status','coverage_ratio','strength','match_quality','strong','weak','strong_hits','weak_hits','strong_match_count','weak_match_count','index_generation','index_status','warnings','host_status'):
                                if k in result_obj: row[k]=result_obj[k]
                        sc=score(task,result); row['score']=sc
                        text=compact_text(result)
                        row['server_ms']=(unwrap_json(result).get('took') if backend=='xerj' and isinstance(unwrap_json(result),dict) else None)
                        # Preserve bounded transport metadata only; response content is never logged.
                        row['response_bytes']=len(text.encode())
                    else: row['error']=err
                    out.write(json.dumps(row,ensure_ascii=False)+'\n'); out.flush(); rows.append(row); task_rows.append(row)
                    if rep: samples.append(row)
                print(json.dumps({'engine':engine,'task_id':task['id'],'summary':summarize(task_rows)},ensure_ascii=False),flush=True)
    host_metrics=sampler.stop()
    host_path=a.host_metrics_out or (a.out+'.host.json')
    with open(host_path,'w',encoding='utf-8') as hf: json.dump(host_metrics,hf,ensure_ascii=False,indent=2); hf.write('\n')
    if xe is not None and a.xerj_transport=='stdio': xe.close()
    timing_note=('client elapsed includes GrepMesh Streamable HTTP transport; the tool exposes no per-search engine duration' if xe is None else 'client elapsed includes MCP transport; XERJ server took is recorded when supplied, with client-minus-engine as an approximate adapter/transport delta; GrepMesh exposes no per-search engine duration')
    print(json.dumps({'corpus_id':corpus['corpus_id'],'tasks':len(tasks),'latency_tasks':sorted(latency_tasks),'repetitions_per_latency_task':a.repetitions,'quality_observations_per_task':1,'grepmesh_tool':a.grepmesh_tool,'grepmesh_label':gm_label,'init_ms':init,'preflight':preflight,'host_metrics_file':host_path,'host_sample_count':host_metrics['sample_count'],'results_file':a.out,'mcp_overhead':timing_note,'summary':{e:summarize([r for r in rows if r['engine']==e]) for e in sorted({r['engine'] for r in rows})}},ensure_ascii=False,indent=2))
if __name__=='__main__': main()
