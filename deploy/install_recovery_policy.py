#!/usr/bin/env python3
"""Apply the source unit's retry policy to an existing system deployment.

Run as root from the checkout. Preserve site-specific users, paths and env files.
No service restart is performed; the delivery caller owns its final canary.
"""
import argparse
import configparser
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[1] / 'grepmesh-mcp.service'
    config = configparser.ConfigParser(interpolation=None)
    config.read(source)
    interval = config['Unit']['StartLimitIntervalSec']
    restart = config['Service']['RestartSec']
    content = ('# Generated from grepmesh-mcp.service by deploy/install_recovery_policy.py\n'
               f'[Unit]\nStartLimitIntervalSec={interval}\n'
               f'[Service]\nRestartSec={restart}\n')
    target = Path('/etc/systemd/system/grepmesh-mcp.service.d/30-recovery.conf')
    if not args.apply:
        print(str(target))
        print(content, end='')
        return
    if os.geteuid() != 0:
        parser.error('--apply requires root')
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content)
    target.chmod(0o644)
    subprocess.run(['systemctl', 'daemon-reload'], check=True)
    print(f'Applied {target}; service not restarted')


if __name__ == '__main__':
    main()
