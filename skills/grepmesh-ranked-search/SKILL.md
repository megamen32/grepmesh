---
name: grepmesh-ranked-search
description: Use the additive GrepMesh ranked_search corpus through native grepmesh-ranked or GPTAdmin GrepMeshRanked when explicitly selected, comparing retrieval, or retrieving source-verified passages from its admitted project/reference roots. Keep XERJ the default for ranked code questions and external Hub search.
metadata:
  version: "1.0.1"
---

# GrepMesh ranked search

## Choose the existing route

- Exact local path, identifier or literal: use bounded host-local `rg`.
- Ranked code/documentation questions or external reference discovery: keep
  XERJ as the current default; inspect its map and freshness first.
- Unknown project/host location across the fleet: use the existing mesh
  GrepMesh discovery route, then switch to the known project's local search.
- Use this additive ranked corpus when the owner selects GrepMesh ranked,
  when comparing it with the existing route, or when its admitted roots and
  source-verified physical passages fit the task. Do not silently replace XERJ
  or broaden an incomplete ranked result into a fleet-wide completeness claim.

## Call the installed tool

The native MCP server is `grepmesh-ranked`; its tool is `ranked_search`.
GPTAdmin exposes the same additive service as `GrepMeshRanked`. Discover its
current target/schema and follow the
effective GPTAdmin profile instructions. Its local HTTP endpoint is
`http://127.0.0.1:19431/mcp`. Native clients use the installed stdio bridge,
which supplies the dynamic HTTP method/tool headers. Do not reconfigure the
working clients merely to perform a query.

Start with a small limit and an explicit configured root when known:

```json
{"query":"HTTP transport connection pool proxy SSL configuration","hosts":["local"],"roots":["httpx-reference"],"limit":3,"context_lines":3}
```

Use `list_locations` on the same service if the configured root name is
unknown. Root names are API identities, not directory basenames. Read a chosen
passage with that same service's `read_text` using its returned host/path.
Do not fabricate caller ACL metadata or borrow an unrelated service bearer.

## Interpret evidence

Inspect `partial`, `truncated`, `host_status`, `index_status`, `coverage` and
warnings. Missing/unavailable/stale index coverage is not proof that source or
an implementation is absent. Narrow the root/query or use the existing route
when coverage is insufficient; retain the failure in a comparison receipt.

`strong_match` describes lexical retrieval coverage, **not answer confidence**.
Verify the proposed fact in the passage and relevant surrounding source before
answering. A high score or complete host response does not establish the fact.

This is lexical retrieval, without translation or semantic embeddings. If a
long question yields weak or irrelevant passages, try a short query containing
two or three distinctive source identifiers or terms. Use the language of the
retrieved documentation when it differs from the question. Read the relevant
section before answering; related snippets alone can be insufficient. Keep
these follow-up searches bounded and report an unsupported answer honestly.

Hits include source host/path, physical `start_line`/`end_line` and match line,
source freshness and a body fingerprint. Cite those actual physical source
lines. `source_verified` means the indexed plain-text body matched current
source bytes at query time; it is not an immutable Git revision or a guarantee
against a later edit. Re-read when exact current citations matter. Do not
invent physical file lines for OCR/PDF extractions.

## Corpus and admission limits

The local source manifest selects eligible tracked files from the existing
13-project inventory, plus an explicitly imported HTTPX reference root.
Coverage is the admitted paths, not every file in those projects. Do not admit
credentials, private data, runtime state, logs, backups or generated trees to
make a query succeed. Path admission is not a secret-content classifier: a
secret inside an already admitted source file remains sensitive content.

The positive `source_manifest` is immutable during one indexer run. Existing
admitted paths update through the watcher; new or renamed paths require
regenerating the source manifest and restarting the owned indexer. Revocation
likewise requires regeneration/restart and verification that its own cached
rows were pruned. A query is not authorization to change corpus policy or
restart services. Missing/invalid manifests must report an explicit degraded
index rather than silently ingesting unrestricted files.

The stable external root is `httpx-reference`. It contains five selected public
HTTPX files at commit `b5addb64f0161ff6bfe94c124ef76f6a1fba5254`, plus
`corpus-manifest.json`. Read that manifest and its `LICENSE.md` before claiming
upstream provenance/license; verify file SHA256 when provenance matters.
This is neither all HTTPX nor all GitHub. Use XERJ Hub for its configured
external corpora; never silently claim Hub coverage as local GrepMesh coverage.

Source policy/import details are maintained in
`/home/roomhacker/agents-projects/grepmesh/docs/external-corpora.md`.
