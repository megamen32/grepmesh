# GrepMesh search resilience

Status: reviewed and tested; deployment pending

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
