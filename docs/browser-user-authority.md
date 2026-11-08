# Browser authority through GPTAdmin and GPT-Node

The protected Console uses the existing authenticated Hub task dispatcher and
registered GrepMesh child on GPT-Node. It never replaces the browser identity
with a shared service bearer. The private reader signing boundary remains at
the existing trusted Node adapter.

## Runtime configuration

Set these non-secret options for the protected Console:

- GREPMESH_BROWSER_REQUIRE_SSO=1
- GREPMESH_BROWSER_HUB_URL: the local HTTP Hub origin, normally http://127.0.0.1:9001
- GREPMESH_BROWSER_ORIGIN: the exact public browser origin, without a trailing slash
- GREPMESH_BROWSER_SHELL_TARGETS: the existing host-to-Node GrepMesh child mapping

GREPMESH_BROWSER_HUB_TOKEN is not consumed. The Console forwards the caller's
cookie only to the fixed loopback Hub endpoint; redirects and environment
proxies are disabled. The Hub owns the SSO subject-to-existing-token binding
and revalidates the current profile. Missing or invalid protected configuration
fails closed without a direct local search/read fallback. An explicit 0 or
an absent protection setting retains the standalone local Console.

## Hub contract

The broker base path is /api/browser/grepmesh.

- GET /session with the browser cookie returns principal, profile_id,
  profile_version, roots_digest, authz_fingerprint, read_roots, and
  concrete allowed_targets.
- POST /server/<registered-child-slug>/mcp carries the same cookie, fixed
  Origin, native MCP headers and tool request. GrepMesh uses search and
  read_text with the existing task capability and idempotency contract.
- POST /mcp listens to the admitted native task. Reconnection and preview
  continuation observe the original task and do not submit a second operation.

The fingerprint covers the verified subject, managed credential binding and
current effective profile, including target/tool restrictions. The Console
does not interpret profile wildcard rules.

## Ownership and revocation

Search jobs and preview handles are bound to the verified principal and
authorization fingerprint. A different user or changed profile cannot reuse
them. The Console checks the session before and after request handling. Event
streams recheck before every result and while waiting, so revocation terminates
the stream without releasing later results.

Protected previews always authorize the exact read_text operation, including
the indexed-match view. Having obtained a search snippet does not grant a new
read operation. Frontend-provided ACL and peer metadata are rejected, including
explicit null fields.

The native local reader intersects canonical document paths with both configured
backend roots and delegated read roots. A symlink from an allowed root into
another configured root does not widen the delegated scope. Remote/cloud paths
are checked on their owning reader, not canonicalized against the Console host.

Ownership records are bounded to 256 jobs and one hour in memory; preview handles
retain their existing five-minute lifetime. Restarting the Console invalidates
the in-memory browser ownership records. It does not make persisted jobs public.

## Protected UI

The catalogue contains only the Hub-authorized roots and concrete targets.
Locations choose the search scope. Folder enumeration, global settings and
backup metadata do not fall back to unscoped native operations. Settings and
backup controls are hidden in this mode; standalone local browsing is unchanged.

## Regression evidence

tests/browser_authority.rs uses an isolated mock authority with a real Console
process to verify user isolation, current-rights changes, stream revocation,
forged metadata, exact read permission and missing-broker failure. It asserts
that the browser transport has no shared Authorization bearer.

tests/console_api.rs retains standalone Console coverage and exercises actual
native delegated reads with allowed, outside-root, parent-traversal and symlink
paths in a temporary corpus. web/console.test.js verifies that restricted
locations still allow scoped search without invoking folder enumeration.

These tests are isolated implementation checks. A real Hub/Node/browser
acceptance and the normal release process remain separate deployment gates.
