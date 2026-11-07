# Persistent full archive reader and loopback tunnel

This contract replaces the transient user units on server88 and server100.
It serves the existing frozen 64-scope Linux archive, not a source refresher.
It does not export, reindex, remove archives, or claim post-snapshot freshness.

- server88: `~/.local/lib/grepmesh-full-reader/` holds the agreed Python release.
  Install the exact reviewed code before the one coordinated restart.
- Durable config: `~/.config/grepmesh-full-reader/scopes.json`.
  Keep the existing `acl-secret.env` unchanged; never copy it into this repository.
- Reader cache stays in tmpfs at
  `/dev/shm/grepmesh-source-host-qa-I8waXR6X/cache`. The historical directory
  name is retained to reuse the warmed 16.9 GB cache without a move or rebuild.
  ExecStartPre recreates the directory after reboot. Cloud manifests and shards
  are fetched and verified during a cold start; empty tmpfs is not data loss.
- Both machines require `loginctl show-user roomhacker -p Linger` to report yes.
  Units are enabled under `default.target` so login is not required.
- server100 uses its existing SSH identity and known_hosts for
  `roomhacker@192.168.2.75`; only loopback19461 is exposed.
- Reader limits retain the measured transient CPU200%, MemoryHigh4G,
  MemoryMax6G and TasksMax384. Cache quota32768MiB is a tmpfs file budget,
  not an application-RSS guarantee. No other service limits are changed.

## Install

Coordinate one runtime writer and hold the project's release lease.
Copy this directory to a durable release directory on each target.
On server88, stage the reviewed reader code, then run:

```sh
python3 deploy/full-reader/install.py reader   --scopes-source /dev/shm/grepmesh-source-host-qa-I8waXR6X/all-scopes-with-oversize-qa.json   --apply
```

For subsequent installations use the durable scopes.json as the source.
On server100:

```sh
python3 deploy/full-reader/install.py tunnel --apply
```

The installer preserves the original unit and runtime properties, copies
the source scopes into a durable rollback directory, atomically stages config
and unit, then performs one stop/start. It refuses a missing signer, missing
reader code, disabled linger or a different source-host coverage. It does
not copy/rotate credentials, clear the cache, alter reader code or restart
other units. Without --apply it stages files only; use --apply only once at
the agreed code delivery boundary.

Inspect FragmentPath, is-enabled, MainPID and the actual process command;
then wait for real reader readiness and fetch metadata plus a known document
through the signed consumer/tunnel. An active process during manifest/cache
validation is not readiness. A controlled service restart validates these
units; it does not prove a physical-host reboot.

## Rollback

Installer receipts and original transient unit definitions are under:
`~/.local/state/grepmesh-full-reader/rollback/<timestamp>-<role>/`.

Stop only the affected unit, replace its persistent unit file with the saved
unit definition (or persistent-unit.before when present), daemon-reload, and
start it. The saved scopes.source.json survives a reboot: if restoring the
old transient command after reboot, recreate its old config path from this
copy before starting. Preserve the signer and cache. Restore the separately
recorded prior reader code if rolling back a code release. Verify the same
metadata/document path after rollback. No source SQLite or archive deletion
belongs to either installation or rollback.
