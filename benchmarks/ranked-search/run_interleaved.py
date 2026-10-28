#!/usr/bin/env python3
"""Interleaved exact-search latency comparison of baseline and candidate MCP endpoints."""
from __future__ import annotations
import argparse, json, statistics, time, urllib.error
from pathlib import Path
from run import MCP, HostSampler, percentile, unwrap_json, compact_text, score

HERE=Path(__file__).resolve().parent

def call_tool(client,name,args):
    r=client.tool(name,args)
    if isinstance(r,dict) and r.get('isError'): raise RuntimeError(compact_text(r)[:500])
    return r

def finish_search(client,query,wait_ms):
    args={'query':query,'hosts':['local'],'mode':'case_insensitive_literal','max_matches':100,'context_lines':0,'wait_ms':wait_ms}
    r=call_tool(client,'search',args); obj=unwrap_json(r)
    deadline=time.monotonic()+60
    while isinstance(obj,dict) and obj.get('state')=='running' and obj.get('job_id') and time.monotonic()<deadline:
        time.sleep(min(max(obj.get('next_poll_after_ms',250)/1000,.1),1.0))
        r=call_tool(client,'search_status',{'job_id':obj['job_id'],'hosts':['local'],'page_size':100}); obj=unwrap_json(r)
    if isinstance(obj,dict) and obj.get('state')=='running': raise TimeoutError('search remained running for 60 seconds; not replayed')
    return r,obj,args

def check_endpoint(client,label,expected_root):
    status=unwrap_json(call_tool(client,'search_status',{'hosts':['local']})); node=status.get('local') or {}
    if node.get('index_state')!='Ready' or int(node.get('indexed_files') or 0)<=0 or node.get('root')!=expected_root:
        raise SystemExit(f'{label} preflight failed: state={node.get("index_state")} indexed={node.get("indexed_files")} root={node.get("root")}')
    r,obj,_=finish_search(client,'s21-lease',1000)
    hit=any(str(x.get('path','')).endswith('docs/inventory/devices/android.md') for x in obj.get('results',[]))
    if not hit or obj.get('partial') or obj.get('truncated'):
        raise SystemExit(f'{label} literal s21-lease preflight incomplete: hit={hit} partial={obj.get("partial")} truncated={obj.get("truncated")}')
    return {'index_state':node.get('index_state'),'indexed_files':node.get('indexed_files'),'generation':node.get('index_generation'),'root':node.get('root'),'probe_hit':True}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--baseline-url',default='http://127.0.0.1:19429/mcp')
    ap.add_argument('--candidate-url',default='http://127.0.0.1:19430/mcp')
    ap.add_argument('--corpus',default=str(HERE/'corpus.json'))
    ap.add_argument('--tasks',default='a01,a02,d04')
    ap.add_argument('--repetitions',type=int,default=30)
    ap.add_argument('--wait-ms',type=int,default=1000)
    ap.add_argument('--baseline-sha',default='3ba1c671')
    ap.add_argument('--candidate-sha',default='unknown')
    ap.add_argument('--out',default=str(HERE/'latency-interleaved-v2.jsonl'))
    ap.add_argument('--host-metrics-out',default=None)
    a=ap.parse_args()
    if a.repetitions<30: ap.error('--repetitions must be >=30')
    corpus=json.load(open(a.corpus,encoding='utf-8')); tasks={t['id']:t for t in corpus['tasks']}
    ids=[x for x in a.tasks.split(',') if x]
    if not set(ids)<=set(tasks): ap.error('unknown task id in --tasks')
    base=MCP(a.baseline_url,None,'baseline'); cand=MCP(a.candidate_url,None,'candidate')
    t=time.perf_counter(); base.initialize(); base_init_ms=round((time.perf_counter()-t)*1000,3)
    t=time.perf_counter(); cand.initialize(); cand_init_ms=round((time.perf_counter()-t)*1000,3)
    expected=corpus['corpus_scope']['grepmesh_scope']['root']
    preflight={'baseline':check_endpoint(base,'baseline',expected),'candidate':check_endpoint(cand,'candidate',expected)}
    if preflight['baseline']['indexed_files']!=preflight['candidate']['indexed_files']:
        raise SystemExit('baseline/candidate indexed file counts differ')
    sampler=HostSampler(); sampler.start(); rows=[]; pair_deltas=[]
    Path(a.out).parent.mkdir(parents=True,exist_ok=True)
    with open(a.out,'w',encoding='utf-8') as out:
        for task_id in ids:
            task=tasks[task_id]
            for rep in range(1,a.repetitions+1):
                order=['baseline','candidate'] if rep%2 else ['candidate','baseline']
                pair={}
                for label in order:
                    client=base if label=='baseline' else cand
                    start=time.perf_counter(); ok=True; error=None; result=None; obj=None; args=None
                    try: result,obj,args=finish_search(client,task['query'],a.wait_ms)
                    except (urllib.error.URLError,TimeoutError,RuntimeError,ValueError) as e: ok=False; error=f'{type(e).__name__}: {e}'[:400]
                    elapsed=round((time.perf_counter()-start)*1000,3)
                    row={'engine':label,'task_id':task_id,'class':task['class'],'rep':rep,'pair_order':order,'elapsed_ms':elapsed,'ok':ok}
                    if ok:
                        row['partial']=obj.get('partial'); row['truncated']=obj.get('truncated'); row['host_status']=obj.get('host_status')
                        row['score']=score(task,result)
                        row['top_coverage']=[{k:h.get(k) for k in ('path','line','score','freshness','coverage','coverage_status','coverage_ratio','strength','match_strength','match_kind','matched_terms','index_generation','host_id','strong','weak') if k in h} for h in row['score']['locators'][:10]]
                        row['response_bytes']=len(compact_text(result).encode())
                    else: row['error']=error
                    out.write(json.dumps(row,ensure_ascii=False)+'\n'); out.flush(); rows.append(row); pair[label]=elapsed if ok else None
                if pair.get('baseline') is not None and pair.get('candidate') is not None:
                    pair_deltas.append({'task_id':task_id,'rep':rep,'candidate_minus_baseline_ms':round(pair['candidate']-pair['baseline'],3)})
    host= sampler.stop(); host_path=a.host_metrics_out or (a.out+'.host.json')
    with open(host_path,'w',encoding='utf-8') as f: json.dump(host,f,ensure_ascii=False,indent=2); f.write('\n')
    summaries={}
    for label in ('baseline','candidate'):
        rr=[r for r in rows if r['engine']==label and r['ok']]
        summaries[label]={'n':len(rr),'errors':sum(1 for r in rows if r['engine']==label and not r['ok']),'p50_ms':round(percentile([r['elapsed_ms'] for r in rr],50),2) if rr else None,'p95_ms':round(percentile([r['elapsed_ms'] for r in rr],95),2) if rr else None,'p99_ms':round(percentile([r['elapsed_ms'] for r in rr],99),2) if rr else None,'per_task':{tid:{'n':sum(r['engine']==label and r['task_id']==tid and r['ok'] for r in rows),'p50_ms':round(percentile([r['elapsed_ms'] for r in rr if r['task_id']==tid],50),2) if any(r['task_id']==tid for r in rr) else None,'p95_ms':round(percentile([r['elapsed_ms'] for r in rr if r['task_id']==tid],95),2) if any(r['task_id']==tid for r in rr) else None,'p99_ms':round(percentile([r['elapsed_ms'] for r in rr if r['task_id']==tid],99),2) if any(r['task_id']==tid for r in rr) else None} for tid in ids}}
    deltas=[x['candidate_minus_baseline_ms'] for x in pair_deltas]
    report={'corpus_id':corpus['corpus_id'],'task_ids':ids,'repetitions_per_task':a.repetitions,'total_pairs':len(pair_deltas),'baseline_sha':a.baseline_sha,'candidate_sha':a.candidate_sha,'preflight':preflight,'init_ms':{'baseline':base_init_ms,'candidate':cand_init_ms},'alternation':'odd reps baseline then candidate; even reps candidate then baseline','summary':summaries,'paired_candidate_minus_baseline_ms':{'n':len(deltas),'p50':round(percentile(deltas,50),2) if deltas else None,'p95':round(percentile(deltas,95),2) if deltas else None},'host_metrics_file':host_path,'host_sample_count':host['sample_count'],'result_file':a.out}
    print(json.dumps(report,ensure_ascii=False,indent=2))
if __name__=='__main__': main()
