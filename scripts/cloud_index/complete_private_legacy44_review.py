#!/usr/bin/env python3
"""Non-destructive private promotion of completed historical cloud FTS.

Run only under a bounded, owner-controlled systemd unit. It waits for the
existing publisher's REAL completion log, validates all immutable manifests,
then starts an unregistered, loopback-only reviewer. Never touches native DB.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import urlopen

HOST = 'server-44'
SNAPSHOT = '20261031T214414Z-ae84d605'
BUILDER = 'grepmesh-archive44-fulltext-workers-20261101.service'
PREVIEW = 'grepmesh-cloud-legacy44-review.service'
LOG = Path('/run/user/1000/grepmesh-archive44-fulltext-workers.log')
RECEIPT = Path('/run/user/1000/grepmesh-legacy44-private-review.json')


def run(command, seconds):
    return subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True,
                          text=True, check=True, timeout=seconds)


def state():
    r=run(['systemctl','--user','show',BUILDER,'-p','ActiveState','--value'],10)
    return r.stdout.strip()


def main():
    if os.getuid()!=1000:
        raise RuntimeError('Reviewer must run as existing unprivileged account')
    if RECEIPT.exists():
        raise RuntimeError('Existing review receipt; refuse repeated promotion')
    until=time.monotonic()+9*3600
    while True:
        status=state()
        if status not in ('active','activating','reloading'):
            break
        if time.monotonic()>until:
            raise TimeoutError('Historical cloud FTS publisher did not finish')
        time.sleep(20)
    if not LOG.is_file() or LOG.stat().st_size>8*1024*1024:
        raise RuntimeError('Missing/bounded publisher record')
    records=[json.loads(line) for line in LOG.read_text().splitlines() if line.startswith('{')]
    complete=[x for x in records if x.get('event')=='cloud_reindex_complete']
    if not complete or complete[-1].get('indexed_documents')!=849721:
        raise RuntimeError('No verified complete publisher receipt; refuse preview')
    if complete[-1].get('archived_documents')!=849796:
        raise RuntimeError('Publisher source archive count changed')
    cli=Path('/home/roomhacker/agents-projects/grepmesh/scripts/cloud_index/verify_reindexed_archive.py')
    if not cli.is_file():
        raise RuntimeError('Cloud admission verifier not installed')
    env=dict(os.environ)
    env['GREPMESH_CLOUD_GATEWAY']='local'
    env['GREPMESH_CLOUD_RCLONE']='/home/roomhacker/.local/bin/rclone'
    validated=subprocess.run([sys.executable,str(cli),'--host',HOST,'--snapshot',SNAPSHOT],
                             env=env,stdin=subprocess.DEVNULL,capture_output=True,text=True,
                             check=True,timeout=600)
    checks=[json.loads(line) for line in validated.stdout.splitlines() if line.startswith('{')]
    admission=[x for x in checks if x.get('event')=='historical_fulltext_manifest_verified']
    if not admission or not admission[-1].get('admitted_private'):
        raise RuntimeError('Eight private indexes failed count or hash gate')
    result=admission[-1]
    if result['searchable_documents']!=849721 or result['cold_only_oversize']!=75:
        raise RuntimeError('Private index/oversized accounting changed')
    config=Path('/home/roomhacker/.config/grepmesh-cloud-legacy44.json')
    with config.open() as f:
        scopes=json.load(f)
    if len(scopes)!=8 or any(x.get('source_host')!=HOST or x.get('source_root')!='/' for x in scopes):
        raise RuntimeError('Private review config is unsafe or incomplete')
    # Only private loopback. No GPTAdmin registration or default mesh changes.
    unit=run(['systemctl','--user','show',PREVIEW,'-p','UnitFileState','--value'],10).stdout.strip()
    if unit!='disabled':
        raise RuntimeError('Private review service must remain disabled in boot policy')
    run(['systemctl','--user','start','--no-block',PREVIEW],15)
    deadline=time.monotonic()+240
    while time.monotonic()<deadline:
        try:
            with urlopen('http://127.0.0.1:19438/health',timeout=3) as response:
                health=json.load(response)
            if health.get('ready') is True and health.get('documents')==849721:
                if health.get('persistent_index_bytes')!=0:
                    raise RuntimeError('Private search service spilled index to SSD')
                break
        except (TimeoutError,OSError):
            pass
        time.sleep(3)
    else:
        raise RuntimeError('Private cloud service was not ready')
    payload={'private_review_ready':True,'host':HOST,'source_snapshot':SNAPSHOT,
             'documents':849721,'oversized_cold_only':75,
             'persistent_index_bytes':0,'cache_limit_bytes':health['cache_limit_bytes'],
             'on_disk_native_database_deleted':False,
             'main_federation_cutover':False,
             'claim_full_search_parity':False,
             'checked_utc':time.time()}
    temp=RECEIPT.with_suffix('.tmp')
    temp.write_text(json.dumps(payload,sort_keys=True,indent=2)+'\n')
    temp.chmod(0o600)
    temp.replace(RECEIPT)
    print(json.dumps(payload),flush=True)


if __name__=='__main__':
    main()
