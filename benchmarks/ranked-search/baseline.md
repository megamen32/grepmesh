# Isolated GrepMesh ranked-search baseline

Captured 2026-10-28 against the isolated baseline endpoint before the candidate was installed. Preflight passed: GrepMesh reported `Ready`, 299 indexed files, generation 310, and the configured `/home/roomhacker/ServersAdministartion/docs` root. A complete literal `s21-lease` probe returned `docs/inventory/devices/android.md` at lines 92–96. The endpoint used the existing release binary with SHA prefix `3ba1c671` (provided by the owner); no production service or index was modified.

The fixed set in `corpus.json` contains 24 tasks. Each engine received one quality observation per task. `a01` (exact identifier), `a02` (natural language), and `d04` (ambiguous) each received 30 additional warm latency repetitions. The raw 228-observation receipt is `baseline.jsonl`.

| Engine | MRR | nDCG@10 | Recall@5 | Negative false-positive rate | Warm client P50 | P95 | P99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| GrepMesh baseline | 0.2014 | 0.2029 | 0.2500 | 0/4 (0%) | 152.81 ms | 186.56 ms | 285.97 ms |
| XERJ lexical, docs-filtered | 0.3451 | 0.4003 | 0.5250 | 4/4 (100%) | 2.06 ms | 2.96 ms | 3.88 ms |

Latency uses 90 warm observations per engine across those three tasks. GrepMesh ran through local Streamable HTTP; XERJ ran through the installed stdio MCP client. Their client elapsed figures are therefore useful as separate baselines, not a transport-controlled speed comparison. GrepMesh does not return per-search engine time. XERJ returned integer `took: 0` on these calls, below its displayed millisecond resolution; the true sub-millisecond engine time and exact MCP overhead remain unknown. Native-agent token usage is unknown.

For the 24 one-shot quality tasks, GrepMesh had MRR 0.548 on exact identifiers, 0.250 on snake/camel spellings, and 0 on all seven natural-language prompts. XERJ had MRR 0.505 on exact identifiers, 0.250 on snake_case queries, 0.625 on camelCase queries, 0.393 on natural language, and 0.125 on ambiguous requests. All four unanswerable requests produced no GrepMesh hit; XERJ returned at least one result for each. These scores assess locator ranking only; they do not establish that a generated answer is factually correct.

All 228 calls completed without transport errors. GrepMesh reported `partial=false` throughout; one broad identifier task (`d01`, `automatic_health_diagnosis`) was marked `truncated=true` at the 100-match cap, so that query's locator ranking is retained but not treated as a complete result-set measurement. XERJ had no timeouts or failed shards. The XERJ request uses lexical `multi_match` over title/body/headings with an `ax_path: docs/` prefix filter, because `serversadmin-docs` also includes records outside the docs root. This differs from XERJ's plain-string definition-first mode and is documented in the runner.

The invalid zero-index capture is preserved separately as `zero-index-rg-fallback.invalid.jsonl`; it is excluded from all baseline statistics because GrepMesh reported zero indexed files and used its `rg` fallback.
