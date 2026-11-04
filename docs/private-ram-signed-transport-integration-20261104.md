# GrepMesh private RAM reader: signed transport integration gate

**Status: NO_GO for production.** Source-only protocol reconciliation, 2026-11-04.
This document is not an authorization to install, reconfigure, restart, reboot,
read private data, extract a signer key, or retry a denied tool call.

## Grounded interfaces

The native GrepMesh entry point has one MCP `search`, `rank=true` by default,
`rank=false` without relevance reordering, and `read_text`. The full-feature
OCR+local-STT candidate from commit `6f2ceda6efb84765fd05a24a5ae547b7bd6d2b2b`
was compiled and passed seven independent **synthetic-only** loopback MCP checks.
Its build is not production and its synthetic acceptance says nothing about
historical cloud corpus routing or delegated signed ACL.

1. `src/server.rs::apply_filesystem_acl_metadata` accepts GPTAdmin-delegated ACL
   metadata into native tool arguments, and requires a verified ACL for a peer
   hop. It deliberately does **not** read or create signer credentials.
2. `src/mcp.rs::search_across` forwards `filesystem_acl` in JSON arguments
   to `search_text` on remote peers. `src/mcp.rs` forwards the same JSON
   argument to remote `read_text`. Standard direct peer transport,
   GPTAdmin-proxy fallback, and `GptAdminClient::call_relay` all receive those
   arguments. There is no special private RAM reader adapter in these calls.
3. `src/gptadmin.rs::send_relay_call` POSTs `tools/call` to its configured
   Hub relay with JSON arguments and the existing bearer credential; the
   visible client code does not itself generate private-reader HMAC headers.
   Whether an authorized Hub component provides a signer-bound translation
   remains **unproven** from the inspected source.
4. `scripts/cloud_index/cloud_index.py::forwarded_acl_roots` explicitly
   rejects a non-null `filesystem_acl` supplied in tool arguments. A forwarded
   read requires `hop_count=1`, the originating host, and a verified policy.
   Its `verify_transport_acl` requires a signer-validated HMAC policy over
   the **actual HTTP request bytes**. This cannot be replaced with JSON
   metadata, an unsigned local query, or fabricated `_meta`.
5. `deploy/remote44-immutable-restore/sealed_offload_reader.py` runs this
   private cloud reader on 44 loopback port 19440 with strict signer-required
   mode. Existing server100 tunnel 19441 forwards to it; the previously
   observed unsigned GET health 200/849721 documents is *not* an authorized
   `search`/`read_text` result. The service is in RAM and must stay so.

## Verified existing GPTAdmin Hub queue ACL (source-only; not installed-runtime proof)

A bounded read-only inspection on 2026-11-04 of the separate, existing GPTAdmin
source tree (no coordination ledger, runtime credentials or authorization data)
established an actual existing trust boundary; no change to that repository was
made. Relevant exact source files and SHA-256:

- \`go-hub/internal/hub/filesystem_acl.go\` SHA
  \`2d383ee94776a52a6ad7961459f18d6f93f0c7af7e4c9df8a759b55ac42027b8\`:
  \`filesystemACLForRequest\` binds ACL policy to the authenticated profile ID,
  version, roots digest and exact tool-argument SHA; \`validateFilesystemACLJobLocked\`
  checks job ID, executor target and current profile revision. ACL is held in
  a **Hub-owned queue envelope**, outside MCP tool arguments (lines 13-15).
- \`go-hub/internal/hub/server.go\` SHA
  \`b65fee452e702aa666717ee2151be7836e54af4be2fad4f8b34dadc5a4a67bc1\`:
  \`shellQueueFilesystemACLCheck\` (lines 3722-3766) permits a fresh policy
  only to the authenticated, matching, *running* executor job. The queue
  dispatcher (around lines 5153-5165 and 6115-6119) binds it as a job
  attribute and requires an approved executor with \`filesystem_acl_version>=1\`.
- \`go-shellmcp/internal/hub/client.go\` SHA
  \`abdb2c6c61a80d024938ccebdc098bdb05cd11959c2fc93eecc381d475cb1555\`:
  the executor receives the ACL in the authenticated queue (lines 172-187)
  and can revalidate via \`CheckFilesystemACL\` POST
  \`/queue/{name}/acl-check\` (lines 316-332).

A filename-limited search across the existing GPTAdmin source tree for the
exact private-reader headers \`X-GrepMesh-Filesystem-Acl\` and
\`X-GrepMesh-Acl-Signature\`, or the signer configuration name
\`GREPMESH_CLOUD_ACL_SECRET\`, found **no source implementation**. This is a
limited finding about the inspected checkout, not proof of an installed
runtime's capabilities or permission to read its secrets.

**Precise remaining link:** a properly authorized existing executor/relay
must take the Hub-verified job ACL *within the trusted execution boundary*,
narrow the policy, omit client-supplied ACL from the forwarded JSON tool
arguments, and produce the **already established** signed HTTP transport
accepted by the private reader. There is no evidence that the checked source
does this today. It must not be built as a second signer in the ordinary
GrepMesh client, nor implemented by forging policy/HMAC headers or changing
identity, ownership, grants, secrets or ACL policy. The owning GPTAdmin
integration must confirm an existing approved capability or obtain a separate
review for the security-boundary change before implementation or deployment.

**Compatibility gap:** Forwarding native JSON arguments unchanged to the private
reader will not satisfy its signer-verified ACL contract. Do not conclude that
the Hub already transforms and signs such requests without an authorized,
auditable receipt. Do not add any application-side signer secret, HMAC copying,
user-supplied `filesystem_acl`, broader OAuth, alternate identity, or direct
unauthenticated fallback to make a test pass.

## Required existing-authority integration

The **existing** authorized GPTAdmin signer/relay owner must establish and
document whether it already offers a supported `mcp_call` route to the private
reader that satisfies all of the following:

- Accepts only an actual authenticated client request under the existing
  dedicated private FTS profile. The relay verifies the originating job,
  profile, revision, tool, request body, and allowed filesystem read roots.
- In its trusted boundary, strips internal `filesystem_acl` from any
  caller-controlled tool argument, and forwards only the authorized, bounded
  parameters to the private reader. The receiver's signer checks must cover
  **exactly the forwarded bytes** and the narrowed read policy.
- Passes the existing policy to the reader through the **already supported**
  authenticated transport, never by giving the Rust MCP, browser, client,
  or a synthetic fixture the signing key. No new signer implementation or
  changes to ACL/identity, permissions, environment, keys, or global OAuth.
- Preserves `request_id` and legitimate `origin_host`/`hop_count`
  provenance, cancellation/timeout and no unintended retry of unknown
  tool calls. A missing/invalid signature and missing verified client identity
  must fail closed *before* fetching a private cloud document.
- Returns a response the native `SearchResponse<SearchHit>` and
  `ReadResponse` can decode. No reinterpretation of partial coverage as
  an authoritative empty answer; preserve host/status, exact path, line0
  path-only hits, OCR/STT and deleted-file history provenance.
- Keeps RAM/shard and resource caps, no persistent SQLite on 44, no full
  corpus copy onto 100, no second builder/service or parallel index.

If no existing supported relay implements this contract, **stop at NO_GO**,
obtain an independently reviewed integration design and its proper approval.
Do **not** implement a parallel signing/security boundary in GrepMesh.

## Additional mandatory blocker: mixed 16-way + 8-way under 512 MiB

The checked-in scripts/cloud_index/multi_cloud.py currently cannot serve
both the 16 independent native100 rolling partitions and the eight historical
server44 partitions as **one MultiReader**:

- The constructor enforces 1 <= len(scopes) <= 16, while the combined
  corpus requires **24** scopes. This is a hard constructor failure.
- Cache admission assigns quota = cache_mib * MiB // len(scopes) and rejects
  quotas below **64 MiB per scope**. At the required **512 MiB TOTAL**
  cache cap, at most eight such scopes fit. Even native100's 16 alone would
  require **1024 MiB** with the current partitioned design; 16+8 would
  require **1536 MiB**. Raising the server44 RAM/cache cap is NOT approved.
- read_text first selects a complete 16-way rolling map, using an
  elif for the historical eight-way map. Even if the constructor were
  naively relaxed, a mixed deployment would route reads only into rolling
  partitions and would not consult legacy partitions after a rolling miss.
  Reading an identical absolute path on both source hosts could silently
  return the wrong host's body. The source_host is currently available in
  private shard hits but is not preserved as a field of the native Rust
  SearchHit, so the public MCP route cannot prove host-qualified parity.

**Do not work around this by increasing reader limits, doubling independent
512-MiB caches, or treating path strings as globally unique.** A reviewed
target design needs *one aggregate* 512-MiB-or-smaller globally accounted
shard cache (including bounded in-flight buffers), an admission policy for
24 manifests and lazy path indexes, source-host-qualified exact read routing,
and explicit ambiguity denial rather than an arbitrary host preference.
The original 8-scope RAM reader must keep functioning unchanged meanwhile.

Required future acceptance includes a bounded synthetic mixed 24-scope
fixture, duplicate same-path files on two source hosts with distinct hashes,
an ACL negative for the wrong source, explicit partial/timeout reporting,
complete host-specific 8-way and 16-way text/regex/read_text coverage, and
actual process/cgroup accounting under a total 512-MiB cache cap.
These tests do not replace the already required genuine signed-client ACL,
full native100 FTS/metadata acceptance or real postboot rollback. The
original profiler and independent preflight test launches were explicitly
denied by safety checks; do not rerun them or any equivalent subset.

## Required production acceptance (not yet performed)

Through the genuine pre-existing authorized private client, and only after
normal permissions are in place, require positive signed literal, regex and
exact `read_text` across historical 44 and completed 16-way 100, including
deleted/oversized and OCR/STT samples; negative out-of-scope `/dev/null`
and `/`, unsigned/forged caller ACL and altered-request replay; parity of
`rank=true` / `rank=false` hit membership with only ordering different;
honest timeout/partial states; measured bounded RAM/cache and no disk index.

Independently still require successful original 16-way profiler admission and
complete immutable FTS manifest, scoped maintenance approval for the actual
shared server44 cold reboot, verified 165-object RAM restoration, and tested
rollback under the same existing trusted deployment operator. Existing
`deploy/mvp-paired-cutover.sh` and `deploy/mvp-deploy.sh` are **not** suitable:
their checked-in configs use persistent `remote_index_store` on 44 and their
scripts mutate root-owned system configuration/services.

## Verifiable source anchors

- `src/mcp.rs`: `search_across` peer `filesystem_acl` argument,
  remote `read_text` argument, `call_remote` direct/relay branches.
- `src/server.rs`: `apply_filesystem_acl_metadata`.
- `src/gptadmin.rs`: `send_relay_call`.
- `scripts/cloud_index/cloud_index.py`: `forwarded_acl_roots`,
  `verify_transport_acl`, `serve`.
- `deploy/remote44-immutable-restore/sealed_offload_reader.py`: signer gate.

**Gate status:** NO_GO; no real signed ACL E2E, complete 100 FTS, scoped cold
reboot or production cutover. Preserve the working reader and old runtime.
