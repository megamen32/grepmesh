# Step 3: fail-closed filesystem ACL on the cloud MCP peer

Date: 2026-11-01. Scope: `scripts/cloud_index/cloud_index.py`
(`forwarded_acl_roots`, `verify_transport_acl`), new
`scripts/cloud_index/test_cloud_acl.py`, `scripts/cloud_index/README.md`.

## The defect that was found

The cloud peer accepted a filesystem ACL **from the tool arguments**:

```python
acl = args.get('filesystem_acl')      # caller-controlled
roots = acl.get('read_roots')         # caller-controlled
return roots
```

`read_roots` was the only thing standing between a caller and the whole
historical corpus. Any local process that could reach the loopback peer
could therefore write `{"hop_count": 1, "origin_host": "x", "filesystem_acl":
{"read_roots": ["/"]}}` and read any historical body.

Reproduced against the live, running `grepmesh-cloud-legacy44-review.service`
on 127.0.0.1:19438 before the fix:

| case | result before the fix |
| --- | --- |
| local read, no ACL | full body returned (expected) |
| **fabricated ACL in args, `read_roots:["/"]`** | **full body returned — ACL bypass** |
| `hop_count=2` | refused ("Mesh hop limit exceeded") |
| `hop_count=1`, no ACL | refused ("requires a verified GPTAdmin filesystem ACL") |

This contradicted the standing rule that no trusted authorization may come
from client arguments, and it also contradicted the code's own docstring,
which claimed the check was "transport-bound". It was not.

## What the code actually guarantees elsewhere (checked, unchanged)

The production federation path was already fail-closed and was **not**
modified:

- the Rust peer at 127.0.0.1:9419 relays to `server-44`
  (192.168.2.5:9419) and answers
  `remote search requires a verified GPTAdmin filesystem ACL` with
  `host_status.ok=false` — re-verified after the fix;
- the Hub binds the filesystem-ACL policy to the authenticated profile and
  job (`go-hub/internal/hub/filesystem_acl.go`), and the executor
  revalidates it against the Hub immediately before the callback
  (`go-shellmcp/internal/server/server.go`);
- the policy travels in a Hub-owned queue envelope and as
  `params._meta["io.gptadmin/filesystem-acl"]` for a child call — never as a
  plain tool argument;
- the cloud peers on 19437/19438 are **not registered** in the Hub or in
  any peer configuration: they are loopback-only review candidates.

So the production consumer path was never exposed; the defect was confined
to the private cloud review peers, where the ACL check was weaker than the
transport actually warranted.

## The fix

- `forwarded_acl_roots(args, transport=None)` **always** refuses a request
  whose arguments contain `filesystem_acl`
  (`Caller-supplied filesystem ACL is not trusted`). A forwarded read
  (`hop_count >= 1`) is refused unless a policy is supplied by the
  **transport**, not by the arguments.
- `verify_transport_acl(headers, body)` accepts a policy only when an
  HMAC-SHA256 signature over `body + "\n" + policy_bytes` matches the
  shared secret in `GREPMESH_CLOUD_ACL_SECRET` (headers
  `X-GrepMesh-Filesystem-Acl`, `X-GrepMesh-Acl-Signature`). The signature
  binds the policy to exactly these arguments: a modified body, a swapped
  policy and a signature replayed for other arguments all fail. With no
  secret configured, no forwarded request can be admitted at all.
- The HTTP handler now computes the transport policy from the raw request
  bytes before parsing, and passes it to the ACL check.
- The verified policy can only narrow access: it is used as
  `authorized_roots` for the shard reader and for the oversize index, and
  it never widens the admitted source root.

## Verification

`test_cloud_acl.py` — 21 checks, in-process loopback MCP server, MemoryStore
and a tmpfs corpus:

- unit: caller-supplied ACL refused; forwarded read without a transport
  policy refused; local read (hop_count 0) still allowed; wrong ACL
  version, empty read roots, relative read root and missing peer origin
  refused; a verified transport policy yields exactly its read roots;
- transport: valid signature verified; forged signature, body
  substitution, policy swap under a valid signature, missing signature and
  unsigned policy all refused;
- end to end over MCP: fabricated ACL in arguments refused for both
  `search` and `read_text`; forwarded search without a policy refused; a
  correctly signed policy accepted; narrowing honoured; a signature issued
  for different arguments refused; a local read still works.

Live re-check after restarting `grepmesh-cloud-legacy44-review.service`
with the fixed code:

| case | result after the fix |
| --- | --- |
| local read, no ACL | full body returned (unchanged) |
| fabricated ACL in args | **refused — "Caller-supplied filesystem ACL is not trusted"** |
| `hop_count=1`, no policy | refused — "requires a verified GPTAdmin filesystem ACL" |
| production peer → server-44 | still refused — "remote search requires a verified GPTAdmin filesystem ACL" |

`grepmesh-cloud-projects-100.service` and
`grepmesh-cloud-legacy44-review.service` are both active after the
restart; no native index, WAL, service configuration or mesh registration
was changed.

Full suite green: test_cloud_index 70, test_cloud_acl 21,
test_multi_cloud 43, test_publish_native_oversize 41,
test_publish_native_oversize_cli 7, test_scan_tree 21, test_refresh_git 26,
test_legacy_bootstrap 10, test_export_native_archive 17,
test_export_native_rolling 9, test_verify_native_rolling 11.

## Known limits (honest)

- The HMAC secret is a shared secret held by the authorized relay. It is
  not a per-request token and it does not expire; rotation is a manual
  operation. A relay that leaks the secret can mint policies.
- The cloud peers still listen on loopback without their own
  authentication, so local access to the port is the remaining trust
  boundary. The ACL check now only *narrows* what an authenticated caller
  may read; it is not a substitute for keeping the peers off public routes.
- The Rust production peer does not currently forward `_meta` policies to a
  cloud peer, so no authorized federation of the historical corpus exists
  yet: the fail-closed refusal is the correct current state, and enabling
  it requires a relay that signs the transport.
