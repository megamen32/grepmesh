#!/usr/bin/env python3
import json
from pathlib import Path
P=Path(__file__).resolve().parent
mapping=json.loads((P/'blind-mapping.json').read_text())
rows=[]; usage=[]; anomalies=[]
for n in (1,2):
    raw=(P/f'blind-pass-{n}.answer.txt').read_text()
    answers,end=json.JSONDecoder().raw_decode(raw)
    if raw[end:].strip(): anomalies.append({'pass':n,'trailing_characters':raw[end:]})
    events=[json.loads(x) for x in (P/f'blind-pass-{n}.events.jsonl').read_text().splitlines()]
    usage.extend({'pass':n,**r['usage']} for r in events if r.get('type')=='turn.completed')
    for answer in answers:
        case=mapping[answer['alias']]; text=answer['answer'].lower()
        # Narrow machine-checkable fact: own reverse SSH independent of USB host.
        correct=case['task']=='a02' and answer['sufficient_evidence'] and 'reverse ssh' in text and 'server-100' in text and ('independent' in text or 'does not depend' in text)
        abstained=not answer['sufficient_evidence']
        rows.append({'pass':n,**case,'alias':answer['alias'],'answerable':case['task']!='a05','fact_supported':correct,'abstained':abstained,'citations':answer['citations']})
summary={}
for engine in ('literal','ranked','xerj'):
    er=[r for r in rows if r['engine']==engine]; positive=[r for r in er if r['answerable']]; negative=[r for r in er if not r['answerable']]
    summary[engine]={'supported_answers':sum(r['fact_supported'] for r in positive),'answerable_observations':len(positive),'correct_abstentions':sum(r['abstained'] for r in negative),'unanswerable_observations':len(negative)}
out={'experiment':'retrieved-context-only; not an autonomous agent comparison','requested_model':'gpt-6-luna','native_execution':'codex exec --ignore-user-config --ephemeral --sandbox read-only; exact command in README','blind_passes':2,'contexts_shared_across_passes':True,'query_repetitions':1,'tasks':['a01','a02','a05','b02'],'summary':summary,'usage':usage,'format_anomalies':anomalies,'rows':rows}
(P/'scoreboard.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({'summary':summary,'usage':usage,'format_anomalies':anomalies}))
