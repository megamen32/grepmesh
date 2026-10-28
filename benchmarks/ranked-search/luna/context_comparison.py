#!/usr/bin/env python3
"""Bounded retrieval-context experiment. No source/gold facts go to the answer model."""
import json, random, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import run

HERE = Path(__file__).resolve().parent
IDS = ['a01', 'a02', 'a05', 'b02']
def main():
    corpus = json.loads((HERE.parent / 'corpus.json').read_text())
    tasks = [t for t in corpus['tasks'] if t['id'] in IDS]
    old = run.MCP('http://127.0.0.1:19429/mcp', None, 'old')
    new = run.MCP('http://127.0.0.1:19431/mcp', None, 'new')
    xe = run.StdioMCP('/home/roomhacker/.local/bin/xerj', 'http://127.0.0.1:19200', 'xerj')
    for c in [old, new, xe]: c.initialize()
    blocks, receipts = [], []
    try:
        for task in tasks:
            for engine, client in [('literal', old), ('ranked', new), ('xerj', xe)]:
                q = task['query']
                if engine == 'literal':
                    tool, args = 'search', dict(query=q, hosts=['local'], mode='case_insensitive_literal', max_matches=3, context_lines=4, wait_ms=1000)
                elif engine == 'ranked':
                    tool, args = 'ranked_search', dict(query=q, hosts=['local'], roots=['serversadmin'], path_globs=['**/docs/**'], limit=3, context_lines=4)
                else:
                    tool, args = 'xerj_search', {'index':'serversadmin-docs','query':{'bool':{'must':[{'multi_match':{'query':q,'fields':['title','body','headings']}}],'filter':[{'prefix':{'ax_path':'docs/'}}]}}, '_source':['ax_path','body','start_line','end_line'],'size':3}
                start = time.monotonic(); r = run.unwrap_json(client.tool(tool,args))
                if r.get('state') == 'running':
                    deadline = time.monotonic() + 60
                    while r.get('state') == 'running' and time.monotonic() < deadline:
                        time.sleep(.25); r = run.unwrap_json(client.tool('search_status', {'job_id':r['job_id'],'hosts':['local'],'page_size':100}))
                contexts = []
                if engine == 'xerj':
                    for h in r.get('hits',{}).get('hits',[]):
                        h = h.get('_source',{}); contexts.append(dict(path=h.get('ax_path'), start_line=h.get('start_line'),end_line=h.get('end_line'),text=str(h.get('body',''))[:16000]))
                else:
                    for h in r.get('results',[]):
                        loc=str(h.get('loc','')).split('-'); contexts.append(dict(path=h.get('path'),start_line=h.get('start_line',loc[0]),end_line=h.get('end_line',loc[-1]),text=h.get('text',h.get('data',''))[:16000]))
                receipts.append(dict(task=task['id'],engine=engine,elapsed_ms=round(1000*(time.monotonic()-start),2),contexts=len(contexts),partial=r.get('partial'),truncated=r.get('truncated')))
                question = 'What is the required lease workflow before interactive S21 use?' if task['id']=='a01' else q
                blocks.append(dict(task=task['id'],engine=engine,question=question,contexts=contexts))
    finally: xe.close()
    (HERE/'retrieval-receipts.json').write_text(json.dumps(receipts,indent=2)+'\n')
    mapping = {}
    for pass_id in [1,2]:
        rng=random.Random(9811+pass_id); shuffled=blocks.copy();rng.shuffle(shuffled); public=[]
        for n,b in enumerate(shuffled):
            alias=f'P{pass_id}-{n+1:02d}';mapping[alias]={'task':b['task'],'engine':b['engine']}
            public.append({'alias':alias,'question':b['question'],'contexts':b['contexts']})
        prompt='You are a fresh zero-knowledge documentation user. This is a read-only context comparison; do not use tools or any other knowledge. For EACH independent alias answer the question only from its supplied contexts. Cite path and physical lines only when supplied. If the requested fact is absent, say INSUFFICIENT_EVIDENCE. Never infer a credential value from related documentation. Answer concise English JSON array [{alias, answer, citations:[{path,lines}], sufficient_evidence:boolean}]. All aliases are independent; never borrow evidence across aliases.\n'+json.dumps(public,ensure_ascii=False)
        (HERE/f'blind-pass-{pass_id}.prompt.txt').write_text(prompt)
    (HERE/'blind-mapping.json').write_text(json.dumps(mapping,indent=2)+'\n')
    print(json.dumps({'retrievals':len(receipts),'packs':2,'context_count':sum(r['contexts'] for r in receipts)}))
if __name__=='__main__': main()
