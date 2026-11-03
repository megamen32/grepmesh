#!/usr/bin/python3
"""systemd ExecCondition: skip while source builder runs or replica is already present."""
import os
import subprocess
import sys
from pathlib import Path

def state(unit):
    env=dict(os.environ,XDG_RUNTIME_DIR="/run/user/1000",
             DBUS_SESSION_BUS_ADDRESS="unix:path=/run/user/1000/bus")
    p=subprocess.run(["systemctl","--user","show",unit,"-p","ActiveState","--value"],
                     capture_output=True,text=True,timeout=6,env=env)
    if p.returncode: return None
    return p.stdout.strip()

def main():
    if os.getuid()!=1000: return 255
    source=state("grepmesh-native100-private-search-resume-20261101.service")
    transfer=state("grepmesh-offload44-immutable-transfer-20261103.service")
    if source not in ("inactive","failed") or transfer not in ("inactive","failed"):
        return 1
    ssh=["ssh","-T","-o","BatchMode=yes","-o","StrictHostKeyChecking=yes",
         "-o","ConnectTimeout=5","-o","ConnectionAttempts=1","roomhacker@192.168.2.5"]
    test=('test -f /run/user/1000/grepmesh-remote-reader-staged-20261103/sealed_offload_reader.py '
          '&& test "$(find /run/user/1000/grepmesh-remote-reader-staged-20261103/store -type f | wc -l)" = 165')
    probe=subprocess.run(ssh+[test],stdin=subprocess.DEVNULL,capture_output=True,timeout=12)
    if probe.returncode==0: return 1
    return 0
if __name__=="__main__":
    sys.exit(main())
