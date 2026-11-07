#!/usr/bin/env python3
"""Install persistent user units; --apply performs one coordinated service restart."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

def run(*args):
    return subprocess.check_output(args, text=True).strip()

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('role', choices=['reader', 'tunnel'])
    ap.add_argument('--scopes-source', type=Path)
    ap.add_argument('--apply', action='store_true')
    args = ap.parse_args()
    home = Path.home()
    config = home / '.config/grepmesh-full-reader'
    unit = 'grepmesh-full-reader' + ('-tunnel' if args.role == 'tunnel' else '') + '.service'
    template = Path(__file__).with_name(unit)
    destination = home / '.config/systemd/user' / unit
    if args.role == 'reader':
        if not args.scopes_source:
            ap.error('reader requires --scopes-source')
        scopes = json.loads(args.scopes_source.read_text())
        counts = {}
        for item in scopes:
            counts[item['source_host']] = counts.get(item['source_host'], 0) + 1
        if counts != {'server-100': 32, 'server-44': 16, 'server-88': 16}:
            raise ValueError('Expected the agreed complete 64-scope frozen archive')
        signer = config / 'acl-secret.env'
        if not signer.is_file():
            raise RuntimeError('Existing acl-secret.env required; never create or rotate signer here')
        if not (home / '.local/lib/grepmesh-full-reader/multi_cloud.py').is_file():
            raise RuntimeError('Install the agreed reader code before applying its unit')
    if run('loginctl', 'show-user', str(os.getuid()), '-p', 'Linger', '--value') != 'yes':
        raise RuntimeError('Enable linger for this user through the authorized host operator first')
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    backup = home / '.local/state/grepmesh-full-reader/rollback' / (stamp + '-' + args.role)
    backup.mkdir(parents=True, mode=0o700)
    (backup / unit).write_text(run('systemctl', '--user', 'cat', unit) + '\n')
    (backup / 'properties.txt').write_text(run('systemctl', '--user', 'show', unit,
        '-p', 'FragmentPath', '-p', 'MainPID', '-p', 'ActiveState', '-p', 'MemoryHigh',
        '-p', 'MemoryMax', '-p', 'MemorySwapMax', '-p', 'CPUQuotaPerSecUSec', '-p', 'TasksMax') + '\n')
    config.mkdir(parents=True, exist_ok=True, mode=0o700)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        shutil.copyfile(destination, backup / 'persistent-unit.before')
    if args.role == 'reader':
        target = config / 'scopes.json'
        if target.exists():
            shutil.copyfile(target, backup / 'scopes.before.json')
        # Preserve the original config too: the transient source disappears on reboot.
        shutil.copyfile(args.scopes_source, backup / 'scopes.source.json')
        raw = args.scopes_source.read_bytes()
        tmp = config / 'scopes.json.pending'
        tmp.write_bytes(raw)
        tmp.chmod(0o600)
        os.replace(tmp, target)
    staged = destination.with_suffix('.service.pending')
    shutil.copyfile(template, staged)
    staged.chmod(0o644)
    os.replace(staged, destination)
    if args.apply:
        # One writer coordinates the code update before this stop/start.
        subprocess.run(['systemctl', '--user', 'stop', unit], check=True)
        subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
        actual = run('systemctl', '--user', 'show', unit, '-p', 'FragmentPath', '--value')
        if Path(actual) != destination:
            raise RuntimeError('Persistent unit not loaded; stopped safely, inspect rollback receipt')
        subprocess.run(['systemctl', '--user', 'enable', '--now', unit], check=True)
    receipt = {'role': args.role, 'unit': str(destination), 'rollback': str(backup),
        'unit_sha256': hashlib.sha256(destination.read_bytes()).hexdigest(),
        'applied': args.apply, 'physical_reboot_tested': False}
    (backup / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(receipt))

if __name__ == '__main__':
    os.umask(0o077)
    main()
