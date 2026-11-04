# server-44 immutable RAM mirror reboot recovery (2026-11-03)

This is the exact production-host *recovery support* source, **not** permission to cut over the public GrepMesh MCP. The existing production names-only units and indexers remain unchanged. No credentials, user documents, SQLite archives or index shards are stored here.

- Source release: `e540528`; four pinned source modules match the installed server-100 release manifest. The reader remains private on server-44; an authenticated Hub/client signed ACL and negative `/dev` proof are required before it can serve production.
- Source data: eight historical server-44 immutable cloud corpora, exactly 849,721 documents, 141 FTS shards, eight path indexes and 16 metadata objects (165 total). Only tmpfs holds restored objects.
- Trusted transport: existing `roomhacker` SSH from server-100 to server-44 with `BatchMode=yes` and `StrictHostKeyChecking=yes`. It does not change host keys or other principals.
- The restore `--user` systemd unit inherits the existing `roomhacker` user-manager principal (uid 1000) and the guard checks uid explicitly. It must not request a second `User=` credential transition: the installed unit had `ExecCondition` status `216/GROUP` before the guard ran.
- Scheduler: `grepmesh-remote44-immutable-restore.timer`, triggered after boot and hourly after the prior attempt. `ExecCondition` skips if the sole native100 builder or prior transfer is active, and skips when 165 objects plus the sealed reader already exist. No duplicate builder/rclone reader is launched.
- Otherwise, the unit verifies and restages four tiny Python source files in remote tmpfs, and uses the original SHA-checked, resumable immutable object copy script. Each object is checked for its exact hash; a conflict is an error, never an overwrite. Cgroup limits: 512 MiB high / 1 GiB max / swap 0, one CPU, 64 tasks, IOWeight 20.
- Independent real-corpus *offline* proof on server-44: 849,721 documents loaded from 165 verified objects; actual text search and `read_text` succeeded, direct `/dev` request denied. **This is not a signed Hub/client ACL acceptance.**

## Runtime reconciliation, 2026-11-04

The recovery unit and pinned release are installed on **server-100**, at
`~/.config/systemd/user/grepmesh-remote44-immutable-restore.service` and
`~/.local/lib/grepmesh-offload/releases/e540528-remote44-20261103/`.
Read-only verification found:

- The restore timer on server-100 is enabled and waiting; its next scheduled
  check was 15:40 MSK. The service is inactive because its `ExecCondition`
  skipped the 14:40 attempt. Running the installed guard directly, without
  changing state, returned exit 1 (skip).
- Installed guard SHA-256 is
  `5fd61844ee879cc1a5397362bd10238c0cb88c8af7ce2c14cb901f4febf26b57`.
  It skips only when the 165-file store and staged reader exist on server-44
  **and** `grepmesh-remote44-private-reader.service` is active there. Missing
  data (remote status 1) or inactive reader (3) permits restore; SSH failure,
  unknown unit and other ambiguous outcomes fail closed. This preserves the
  04.11 additions; a source-only draft that omitted the active-reader check
  was rejected and replaced with this installed behavior.
- The reader timer and service on server-44 are enabled/active. Its process was
  PID 30758 at inspection; 165 files were present in its tmpfs store. The four
  staged Python source hashes match the pinned manifest. Neither host was
  rebooted, and neither runtime was changed.
- The installed service unit removes `User=` (the user manager already runs as
  uid 1000; the old directive caused a 216/GROUP start failure) and starts then
  verifies the private reader after successful restoration. The installed
  restore and source-stage scripts also contain 04.11 hardening for private
  inode/mode checks, atomic no-overwrite publication, interrupted-link
  recovery, and a pinned manifest digest. These installed changes are now
  mirrored in this owned source directory; release and unit hashes were
  compared read-only.

The recovery chain is installed and its current healthy-state skip is proven;
**physical reboot plus read-after-reboot is still untested**. The next real
test is a controlled reboot window after Lead integration/review of the whole
source slice, followed by proof that server-100's timer restores the server-44
tmpfs objects and starts the private reader, then a read through that actual
consumer. No public production cutover is part of this test.

Rollback is host-specific: stop the controller on server-100, then stop the
private reader timer and service on server-44. These are operator instructions
only; do not run them during source verification. They preserve staged source
and tmpfs data.

On server-100:
```sh
systemctl --user disable --now grepmesh-remote44-immutable-restore.timer
systemctl --user stop grepmesh-remote44-immutable-restore.service
```

On server-44, invoked from server-100:
```sh
ssh -T -o BatchMode=yes -o StrictHostKeyChecking=yes roomhacker@192.168.2.5 /usr/bin/env XDG_RUNTIME_DIR=/run/user/1000 DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus /usr/bin/systemctl --user disable --now grepmesh-remote44-private-reader.timer
ssh -T -o BatchMode=yes -o StrictHostKeyChecking=yes roomhacker@192.168.2.5 /usr/bin/env XDG_RUNTIME_DIR=/run/user/1000 DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus /usr/bin/systemctl --user stop grepmesh-remote44-private-reader.service
```

Verify each host's unit state after a rollback. Disabling the controller timer
does not stop a currently running restore service, and disabling the server-44
reader timer does not stop its already-running service. Do not remove the
release, staged source, or store as part of rollback.

The copied files in this directory are a provenance mirror of the exact installed release and systemd templates. Keep server-specific pinning intentional until a new independently reviewed release is accepted.

## Guard integrity regression, 2026-11-04

`restore_builder_guard.py` now derives the same object catalog as the restore
worker and returns the healthy-state skip only after the remote verifier proves
the exact relative-name set, each file size and each SHA-256, then confirms the
reader unit is active. Hashing is streamed in 1 MiB chunks. Catalogs are capped
at 256 objects / 1 GiB and individual objects at 64 MiB; SSH has keepalive
failure detection and a 30-minute overall bound so the observed ~827 MB mirror
can be checked without loading it into memory. Any catalog, transport, timeout,
conflicting objects or reader ambiguity exits 255 and cannot report a healthy
skip. A missing store, incomplete non-conflicting store, or confirmed inactive
reader requests restoration. The guard reads metadata without the volatile
cache because the cache may not exist after boot. Unknown temporary objects
remain fail-closed and require reconciliation.

Focused synthetic regression command:
`python3 -m unittest discover -s deploy/remote44-immutable-restore/tests -v`
It covers the exact 165-object set, 165 wrong names and content, a disabled
reader, SSH failure and timeout. The tests execute the verifier against local
temporary fixtures; they do not contact or mutate either host. No runtime
operation, reboot, production cutover or profiler was run for this change.
