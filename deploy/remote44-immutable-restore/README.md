# server-44 immutable RAM mirror reboot recovery (2026-11-03)

This is the exact production-host *recovery support* source, **not** permission to cut over the public GrepMesh MCP. The existing production names-only units and indexers remain unchanged. No credentials, user documents, SQLite archives or index shards are stored here.

- Source release: `e540528`; four pinned source modules match the installed server-100 release manifest. The reader remains private on server-44; an authenticated Hub/client signed ACL and negative `/dev` proof are required before it can serve production.
- Source data: eight historical server-44 immutable cloud corpora, exactly 849,721 documents, 141 FTS shards, eight path indexes and 16 metadata objects (165 total). Only tmpfs holds restored objects.
- Trusted transport: existing `roomhacker` SSH from server-100 to server-44 with `BatchMode=yes` and `StrictHostKeyChecking=yes`. It does not change host keys or other principals.
- The restore `--user` systemd unit inherits the existing `roomhacker` user-manager principal (uid 1000) and the guard checks uid explicitly. It must not request a second `User=` credential transition: the installed unit had `ExecCondition` status `216/GROUP` before the guard ran.
- Scheduler: `grepmesh-remote44-immutable-restore.timer`, triggered after boot and hourly after the prior attempt. `ExecCondition` skips if the sole native100 builder or prior transfer is active, and skips when 165 objects plus the sealed reader already exist. No duplicate builder/rclone reader is launched.
- Otherwise, the unit verifies and restages four tiny Python source files in remote tmpfs, and uses the original SHA-checked, resumable immutable object copy script. Each object is checked for its exact hash; a conflict is an error, never an overwrite. Cgroup limits: 512 MiB high / 1 GiB max / swap 0, one CPU, 64 tasks, IOWeight 20.
- Independent real-corpus *offline* proof on server-44: 849,721 documents loaded from 165 verified objects; actual text search and `read_text` succeeded, direct `/dev` request denied. **This is not a signed Hub/client ACL acceptance.**

Current host receipt: `~/.local/state/grepmesh-remote44-reboot-restore-armed-20261103.json`. Timer is enabled, and the first invocation was intentionally skipped while native100's single recovery builder was active. **A physical host reboot and read-after-reboot have not been tested**. Do not treat armed recovery as a completed reboot gate.

Rollback (only affects this support timer, never public production):
```sh
systemctl --user disable --now grepmesh-remote44-immutable-restore.timer
```

The copied files in this directory are a provenance mirror of the exact installed release and systemd templates. Keep server-specific pinning intentional until a new independently reviewed release is accepted.
