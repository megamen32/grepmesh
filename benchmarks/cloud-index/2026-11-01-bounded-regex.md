# Bounded regex over the cloud FTS history (server-44 private review) — step 1 of the parity gates

## What changed (code only; nothing deployed, no production process touched)

- `scripts/cloud_index/cloud_index.py`:
  - `sre_literal_required()` extracts a sound contiguous required literal from the regex parse tree (anchors skipped, classes/alternations stop the run). Absence of that literal in a shard's Bloom filter or body guarantees the regex cannot match, so pruning never loses results.
  - `compile_bounded_regex()` compiles once with `re` (exact semantics) and, when the installed `regex` engine is present, with the same pattern for hard per-call timeouts.
  - `regex_match_positions()` scans per line first (grep parity for `^`/`$` anchors) and, only when the pattern can consume a newline (conservative parse-tree classification, unknown constructs = "can"), also scans the whole body for cross-line `(?s)`/literal-`\n` matches. Every engine call carries the remaining deadline when the hard engine is available.
  - `Reader.query(..., regex=True)` and the loopback MCP handler accept `mode=regex`; `find_paths` + regex is rejected explicitly (globs only, no silent coercion). Invalid regex fails with `Invalid regular expression` — never an authoritative empty result.
  - FTS trigram prefilter now derives from the regex literal (fold-safe condition unchanged); legacy literal/glob behavior bit-for-bit unchanged.
- `scripts/cloud_index/multi_cloud.py`: `MultiReader.query` forwards `regex` and validates the pattern once (invalid input fails fast, instead of per-scope warnings).
- `scripts/cloud_index/README.md`: capability status updated honestly (bounded regex implemented in the private reviewer; relevance ranking still not; no production switch).

## Verification (all synthetic, tmpfs-only, bounded CPU; MemoryStore cloud transport)

- `test_cloud_index.py`: 70/70 pass in 0.7 s, including 12 new regex cases: alternation hit set, zero cloud fetches for a pruned literal, inline `(?i)`, line anchors (hit and non-hit), unicode body hit, `(?s)` cross-line, literal `\n` cross-line, invalid-regex explicit failure, optional-class pruning, and a catastrophic `(?:a+)+b` probe interrupted by the hard engine under a 0.2 s deadline (< 2 s wall measured), plus end-to-end regex through the real loopback MCP JSONRPC envelope (search hit, invalid-regex error, literal mode unchanged).
- `test_multi_cloud.py`: 19/19 pass, including federated-scope regex, inline flag, fail-fast invalid regex on the federation, and a missing-literal regex touching zero shards.
- Uncommitted previous-stage work (`read_text`, forwarded ACL shape checks, native inventory scripts) preserved untouched; no third-party file modified.

## Honest limits

- Nothing was deployed: the running `grepmesh-cloud-projects-100.service` and the disabled legacy44 review service keep the previous code until an accepted rollout. The 55.61 s/6.64 s production numbers cover literal queries, not regex.
- The hard per-call bounding uses the `regex` engine timeout; on the deployed runtime it is importable (`/usr/lib/python3/dist-packages/regex`), but if a future host lacks it, per-line scans remain deadline-checked between lines while one engine call on a pathological line could run longer than the deadline — this would be reported as a warning only after that call returns, and must be re-proven per host at rollout.
- Cross-line matches are a deliberate superset of per-line grep semantics (same policy the literal path already follows): a body-true match is never hidden, and per-line `^`/`$` anchors take priority.
- Relevance ranking, live text/OCR refresh, reboot/rollback fatigue tests, the 75 oversized cold-only texts, and real GPTAdmin filesystem ACL verification remain open gates. No default-mesh registration or cutover happened.

## Next gates (in the owner's priority order)

1. Make the 75 oversized server-44 documents searchable via bounded cloud objects (no SSD growth), with honest partial/error states.
2. Real filesystem ACL check for the history reviewer (fail-closed without a verified GPTAdmin ACL; no trusted authorization from client arguments).
3. Live text/OCR refresh and reboot/rollback tests as separate gates.
