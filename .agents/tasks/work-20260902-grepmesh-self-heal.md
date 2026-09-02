# GrepMesh search resilience

Status: complete

Started at 2026-09-02T14:19:06+03:00 (host `date --iso-8601=seconds`).
Estimate: 8 / 20 active minutes.

Wanted result: GrepMesh stays usable when configured roots gain inaccessible
descendant directories, and Linux restarts the daemon after any exit.

Shortest real canary: a five-host MCP search returns every reachable host as
healthy while still returning matches from readable paths.

Smallest YAGNI slice: preflight each selected root, treat permission-only
descendant traversal diagnostics as skipped scope, and use `Restart=always` in
the Linux installer unit.

Discarded now: a fleet control plane, automatic permission changes, a new
monitoring service, and redesign of Windows task supervision.

Evidence:

- Terra Reviewer: approved after three repair rounds.
- Rust and integration suites: 77 tests passed; installer contracts passed.
- Public main: code commit `4db3526`.
- Linux: server-100, server-88, and server-44 run the same built revision with
  `Restart=always` and active systemd services.
- macOS: mac-m1 and mac-mini run the same revision under launchd `KeepAlive`.
- Real MCP canary: `hosts="*"` returned all five hosts `ok: true` and
  `partial: false`; dedicated mac-m1 broad traversal and server-88 unreadable
  descendant canaries also returned `partial: false`.
