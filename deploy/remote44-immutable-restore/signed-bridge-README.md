# Hub-signed historical FTS bridge (private canary only)

This code is **not** a production cutover. It is a stdio-only MCP child intended to replace the *existing* GrepMesh child ref only after independent acceptance. Do not register it under another ref and interpret that as a successful Hub ACL test: Hub emits filesystem ACL only for the canonical `ref=grepmesh` and `search/read_text`.

## Architecture

1. An owner-issued, limited GPTAdmin client `grepmesh-private-fts-client-20261103` is bound to `grepmesh-private-fts-only-20261103` (only `shell:roomhacker-server-100`, `mcp_call`, 24 canonical read roots, no `/` or `/dev`, no writes). **Do not bind the global ChatGPT OAuth client.**
2. Hub signs/revalidates the actual job. ShellMCP injects `params._meta["io.gptadmin/filesystem-acl"]` through the private stdio child channel. Client arguments do not supply any ACL.
3. This bridge validates that delegated identity and exact read-root scope, then HMAC-signs the precise request bytes and policy for server-44. The per-reader secret resides in mode-0600 owner-owned files on 100 and 44 and is **never stored in Git**.
4. An existing authorized strict-hostkey SSH connection exposes remote 127.0.0.1:19440 to local 127.0.0.1:19441. No public HTTP bind, secrets in arguments, new SSH keys, identity substitution or rclone writes.
5. The RAM-only reader on 44 verifies the HMAC; unsigned `search`, `read_text`, `search_status` are all rejected in the **live** process.

## Current acceptance

- 849,721 real archived server-44 documents, 141 FTS shards, 8 path indexes, 16 metadata (165/165 SHA-verified) in tmpfs; live private reader at loopback:19440, cache 512 MiB, persistent_index_bytes=0.
- Actual historical offline query and read_text worked; `/dev` denied in that test. The SSH tunnel health and unsigned denial pass from 100. Unit tests 12/12 PASS in bounded cgroup.
- **NOT accepted:** real bound Hub/client positive + negative /dev through this stdio child; rank-before-topK semantics; regex/OCR/STT/delete-history/live-delta parity; full native100 completion; actual post-reboot read; rollback. Mixed-host queries deliberately fail closed. Private historical-only responses explicitly report partial.
- Do not change the existing production `GrepMesh` ref or port 9419 until those gates pass. This bridge source is useful for independent review and for a deliberately isolated next-stage canary.

## Native behavior and rollback

Unsigned requests use the installed native names-only MCP at 127.0.0.1:9419; explicit historical server-44 queries require verified ShellMCP metadata. The deprecated `ranked_search` and `search_text` names are not exposed by this private child. Public API remains native until an approved cutover.

The SSH tunnel is independently reversible via `systemctl --user disable --now grepmesh-private44-ssh-tunnel.service` on 100. The private RAM reader timer is independently reversible on 44 via `systemctl --user disable --now grepmesh-remote44-private-reader.timer` and `systemctl --user stop grepmesh-remote44-private-reader.service`. These do not stop or restart the production names-only services.
