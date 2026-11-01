## Owner decision — 2026-11-01: supersedes standalone Ranked

One public search, rank=true by default; rank=false opts out. Ranking only
orders already-authorized matches and must not narrow literal/regex, OCR or
historical deleted-file coverage. The old ranked_search tool is removed, not
aliased. Do not install a second ranked index/service or reintroduce its MCP entry.
The independent source-manifest selector remains a corpus policy utility only.

Acceptance: default ranking before top-k; deterministic opt-out; identical match
sets within the reported candidate/deadline budget; compact/job output preserves
relevance order (compact fields remain unchanged); forged ACL denied; removed tool unavailable. Keep original indexers and
SQLite untouched during the ongoing cloud migration. A source commit is NOT a
claim that the native production runtime has been switched.

Current step: 14 focused Rust checks (including real MCP, compact output and
no-index fallback), 1 verified-metadata ACL regression and 9 bridge/source-policy
checks passed: 24 targeted checks. The compact-output regression was fixed without
adding score fields to compact responses; verbose responses carry scores. Native
production release remains pending; the test binary deliberately has no OCR/STT
features and MUST NOT be deployed as a replacement runtime. The owner explicitly retired the standalone variant; only its isolated
server-100-ranked listener may be disabled. Keep canonical port 9419 and its
original indexers running until the approved migration/release gate. No original SQLite/WAL/SHM removal is authorized by this change.

Additional regression note: the old broad mesh fixture suite is not accepted: four
cases failed and an unauthenticated fake-peer fixture stalled. Its isolated test
unit was stopped; no live service was touched. Do not count this as mesh/ACL
production acceptance. Focused new ACL and API contract checks are separate.

Live retirement receipt (2026-11-01): grepmesh-ranked.service is disabled and
inactive. Its dedicated port 19431 was retired; original port 9419 retained PID
3068 without restart. The standalone index.sqlite (496373760 bytes) is preserved.
This is intentional retirement, NOT an incident to auto-repair or re-enable.
Receipt: ~/.local/state/grepmesh-ranked-retired-20261101.json.
Native production rank=true API is NOT installed yet. Do not deploy the no-feature
test binary or stop original migration indexers to make the API look complete.
The legacy asynchronous/fanout fixture suite is still unaccepted; the isolated
async fixture expects a running job but receives an immediate response. No ACL
policy was loosened to make these older fixtures green.

---

# Ranked search: bounded first implementation

Decision receipt, 2026-10-28. Source inspected on canonical `main`, commit
`730dee7820264bbe6df6c203afe0b0bc15e4f9e0`. This is a proposed implementation,
not an installed or benchmarked result.

## Decision

Add a separate `ranked_search` MCP tool backed by the existing SQLite FTS5
documents. Keep `search`, `search_text`, their defaults, literal and regex
semantics, result order, and response rendering unchanged. Do not create a
second broad index, change OCR placement, or make indexing synchronous.

The first vertical is lexical retrieval with deterministic local scoring,
not semantic retrieval. Existing `tokenize='trigram'` can retrieve quoted
substrings, but a whole natural-language question is currently one literal
phrase. Split the ranked query into bounded terms; preserve full identifiers
and add snake_case/camelCase components. Build an OR of correctly quoted terms
of at least three Unicode characters. Explicitly report unsupported all-short
queries instead of returning a complete empty result. Cap terms and query bytes.

Use authorized candidate paths first, bodies second, score and context third,
and final top-k last. Score term coverage, exact identifier matches, definition
lines, path relevance and documentation headings. The exact score is an
implementation detail to calibrate against the fixed task corpus. Do not use
global `bm25()` as the final score: trigram corpus statistics include documents
outside a caller's ACL. Scores computed from the permitted bodies alone avoid
making forbidden documents influence relevance.

## Exact seams

- `src/server.rs`: tool list/schema/dispatch currently handle `search` and
  `search_text`. Add a distinct ranked schema and dispatch. Crucially add the
  new name to `apply_filesystem_acl_metadata`'s supported whitelist. Strip
  caller-supplied ACL exactly as today; accept trusted shell metadata or
  authenticated peer propagation. Bearer authentication remains unchanged.
- `src/mcp.rs`: add `RankedSearchArgs`, `RankedSearchHit` and
  `McpService::call_ranked_search`; preserve `normalize_request`, request IDs,
  hop rules and per-host status. `call_search_with_overall_timeout` currently
  sorts host/path/line and therefore cannot be reused as the ranked finalizer.
  `render_search_response` also groups compact file ranges; use a separate
  ranked response preserving rank, score, host/path and precise line ranges.
- `src/backend.rs`: add `LocalBackend::ranked_search_bounded`, following
  `search_text_with_overall_timeout` for root selection, intersection with
  verified `read_roots`, exclusion policy, deadline and blocking SQLite work.
  Do not add a new `SearchMode` variant: old exhaustive matches and callers
  retain their contracts.
- `src/index.rs`: add
  `IndexManager::ranked_search_hits_filtered_until`. Reuse
  `PersistentIndex::read_connection` (read-only snapshot, busy timeout and
  progress deadline), root-readiness checks and path/glob exclusion helpers.
  `visit_matching_documents_with_paths` is the existing path-before-body
  pattern; its literal phrase construction and early result stop need a
  separate ranked visitor, not a modification to old literal traversal.
- `src/remote_index.rs`: add an authorized ranked reader only after the local
  canary works. Pattern after `search_filtered_authorized`, never the denied
  `search_filtered` entrypoint. Validate source host/root/path and caller
  `read_roots` before loading the body, scoring or extracting context. Preserve
  source host identity rather than attributing hits to the index host.

Authorization and excludes must precede candidate budgets that could otherwise
let forbidden hits starve permitted hits. Apply source-host/root restrictions
in SQL where possible and retain lexical path validation before body loading.
Use the existing limits as outer response/deadline bounds; return explicit
partial/truncated coverage when an admitted candidate/body budget is exhausted.
Never silently fall back to exact ripgrep and present that as ranked coverage.

## Context and freshness

Return one compact best passage per file initially, with 1-based start/end and
match lines, host, original source path, score, and index state/generation.
Expose a body fingerprint and available size/mtime metadata if practical.
For local source text compare metadata before emitting a passage; changed or
deleted files must be marked stale or omitted with an explicit coverage
warning. Matching metadata is not proof of identical content; exact citation
acceptance must compare the passage with the current source. Cached OCR/PDF
text is an extraction and must not claim plain source-file line numbers.

Remote copies expose epoch/update time and per-document mtime/hash, but cannot
prove current source availability without readback. Do not present replication
time as source freshness. No-index, building and timeout states are distinct
from a complete zero-hit answer.

## Why not an immediate XERJ adapter

The connected native XERJ map was queried for `*grepmesh*`; it returned
`matched=0` on this run. This establishes no catalogued matching dataset, not
absence of an uncatalogued index. No new corpus was created and no engine
configuration or runtime was mutated. HTTP port/location and endpoint ACLs
were not independently inspected, so `19200` is not asserted as verified here.

XERJ may already provide better tokenization and passage retrieval, but a safe
adapter still needs a pinned corpus-to-host/root mapping, ACL filters applied
inside retrieval before ranking/top-k, proof that those filters constrain all
passages, stale-index reporting, query deadlines, response normalization and
source citation verification. Post-filtering XERJ top-k is insufficient: denied
documents can affect scores and starve allowed results. A thin HTTP proxy alone
does not satisfy the contract. Reconsider after a bounded existing project
corpus demonstrates these properties and beats the SQLite candidate at the
same quality floor. Do not autoindex broad roots merely to enable comparison.

## Smallest real canary and rejection criteria

Use one explicitly scoped test project with definition, reference, documentation,
snake/camel identifiers and a denied sibling containing many query terms.
Run real MCP `ranked_search` with a multiword question and top-k=3; verify the
expected definition or document, current exact passage, host/path/line and
nonempty score. Repeat with the denied sibling added: permitted ranking and
context must remain unchanged, and no forbidden text/path may be returned.
Separately verify forged ACL stripping, empty ACL denial, peer hop preservation,
and explicit failed-peer status. Keep existing literal/regex expected output
unchanged. Modify, rename and delete the owned fixture through the normal
indexer and poll within the agreed freshness window; preserve each intermediate
stale/incomplete status rather than treating it as a zero-hit success.

First prove local SQLite/MCP. Then test a single authorized remote-index source,
and a controlled failed peer. A local-only candidate must label its scope and
must not claim full fleet acceptance. Candidate/body/deadline exhaustion must
produce partial/truncated results. Quality, P95, memory and cost comparisons
remain unmeasured until a representative fixed corpus is run.
