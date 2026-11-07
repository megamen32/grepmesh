#!/usr/bin/env python3
"""Run one configured source exporter, publish, and activate the cloud reader.

Policy contains command arrays and non-secret paths only. Source and reader
credentials remain with their existing services/SSH identities.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import time
import uuid


def activate(policy):
    config = Path(policy['reader_config'])
    remote = policy['reader_ssh']
    target = policy['remote_reader_config']
    temporary = target + '.incoming-' + uuid.uuid4().hex
    # Serialize remote activation with config updates from other source jobs.
    with open(str(config) + '.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        subprocess.run(['scp', '-q', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
                        str(config), remote + ':' + temporary], check=True, timeout=60)
        script = '''import hashlib,json,os,pathlib,subprocess,sys,socket,time
source,target,service=map(str,sys.argv[1:])
source=pathlib.Path(source);target=pathlib.Path(target)
incoming=source.read_bytes();current=target.read_bytes()
data=json.loads(incoming)
assert isinstance(data,list) and 1<=len(data)<=128
if incoming==current:
 source.unlink();print('reader unchanged');raise SystemExit(0)
backup=target.with_name(target.name+'.previous')
backup.write_bytes(current);backup.chmod(0o600)
source.chmod(0o600);os.replace(source,target)
try:
 subprocess.run(['systemctl','--user','restart',service],check=True,timeout=90)
 deadline=time.monotonic()+180
 while True:
  try:
   with socket.create_connection(('127.0.0.1',19461),timeout=2):pass
   break
  except OSError:
   if time.monotonic()>=deadline:raise TimeoutError('reader did not bind after loading cloud scopes')
   time.sleep(2)
except BaseException:
 os.replace(backup,target)
 subprocess.run(['systemctl','--user','restart',service],timeout=90)
 raise
print('reader activated '+hashlib.sha256(incoming).hexdigest())
'''
        command = shlex.join(['python3', '-', temporary, target, policy['reader_service']])
        subprocess.run(['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
                        remote, command], input=script.encode(), check=True, timeout=300)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--policy', type=Path, required=True)
    parser.add_argument('--host', required=True)
    args = parser.parse_args()
    policy = json.loads(args.policy.read_text())
    source = next(x for x in policy['sources'] if x['host'] == args.host)
    state = Path(policy['state_dir'])
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    with open(state / (args.host + '.lock'), 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        started = time.time()
        env = os.environ.copy()
        env.update(GREPMESH_CLOUD_GATEWAY='local',
                   GREPMESH_CLOUD_RCLONE=policy['rclone'], PYTHONDONTWRITEBYTECODE='1')
        exporter = subprocess.Popen(source['command'], stdout=subprocess.PIPE,
                                    start_new_session=True)
        try:
            command = [sys.executable, str(Path(__file__).with_name('refresh_source.py')),
                       '--host', args.host, '--source-root', '/',
                       '--root', policy['ram_root'] + '/' + args.host,
                       '--reader-config', policy['reader_config'],
                       '--buckets', str(source['buckets'])]
            result = subprocess.run(command, stdin=exporter.stdout, env=env,
                                    capture_output=True, timeout=source.get('timeout_seconds', 10800))
            exporter.stdout.close()
            exporter_code = exporter.wait(timeout=15)
            if result.returncode or exporter_code:
                # Exporter diagnostics go to the service journal; never print bodies.
                raise RuntimeError('Source/publish failed: ' + str(exporter_code) + '/' +
                                   str(result.returncode) + ' ' + result.stderr.decode(errors='replace')[-2000:])
            published = json.loads(result.stdout)
            activate(policy)
            receipt = {'host': args.host, 'ok': True, 'elapsed_seconds': round(time.time()-started, 1),
                       'publication': published, 'finished_at': time.time()}
            (state / (args.host + '-result.json')).write_text(json.dumps(receipt))
            print(json.dumps(receipt), flush=True)
        except BaseException as error:
            receipt = {'host': args.host, 'ok': False,
                       'elapsed_seconds': round(time.time()-started, 1),
                       'error': str(error), 'finished_at': time.time()}
            (state / (args.host + '-result.json')).write_text(json.dumps(receipt))
            raise
        finally:
            if exporter.poll() is None:
                os.killpg(exporter.pid, signal.SIGTERM)
                try:
                    exporter.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(exporter.pid, signal.SIGKILL)
                    exporter.wait()


if __name__ == '__main__':
    main()
