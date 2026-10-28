# Final ranked candidate v3

Captured 2026-10-28 against `http://127.0.0.1:19430/mcp`, running immutable
artifact SHA-256
`165195436014711d76e2217c5ea625554adf2b64551e50ca50ac72342072b339`. The
service reported Ready, 299 indexed files, generation 0, and root
`/home/roomhacker/ServersAdministartion/docs`; the literal `s21-lease` gate
passed. Generation is a process-local counter; 0 does not imply an empty
persisted index. The fixed `corpus.json` and all gold paths were left unchanged.

One quality request ran for each of the 24 tasks, followed by 30 warm requests
each for `a01`, `a02`, and `d04` (114 rows). All 114 calls succeeded with
`partial=false`; ranked results were capped at ten. Latency is MCP client
elapsed, since GrepMesh returns no per-request engine duration.

## Quality deltas

| Metric | Exact baseline | Ranked v2 | Final ranked v3 |
|---|---:|---:|---:|
| MRR | 0.2014 | 0.3722 | 0.4306 |
| nDCG@10 | 0.2029 | 0.4306 | 0.4868 |
| Recall@5 | 0.2500 | 0.4750 | 0.5750 |
| Strict natural-language gold hit | 0/7 | 4/7 | 4/7 |
| Any-hit negative rate | 0/4 | 4/4 | 4/4 |
| Negative strong-hit rate | unavailable | 2/4 | 2/4 |
| Negative weak-only rate | unavailable | 2/4 | 2/4 |
| Strong relevant MRR | unavailable | 0.1736 | 0.2639 |
| Weak relevant MRR | unavailable | 0.1986 | 0.1719 |

The final candidate improved MRR by .0584, nDCG by .0562, and recall@5 by .1000
over v2. Exact-identifier MRR increased .6190→.7143; snake-case MRR increased
.0500→.4167. Response coverage remained 14 `strong_match` and 10 `weak_match`
tasks. Natural-language strict recall did not move and remains below the prior
85% quality target. All four negative queries still return hits, including two
strong matches; no abstention improvement is demonstrated.

Actionable per-query changes and remaining misses:

| Task | V2 → v3 | Assessment |
|---|---|---|
| `c03`, `mcp_servers` | Gold runbook rank 10 → 2; MRR .10 → .50 | Clear improvement: the gold runbook scores 74.939 versus 75.0 for the general XERJ document at rank 1. The identifier anchor nearly ties, but does not take the lead. |
| `a03`, `agent-device devices --json` | No gold hit → rank 3; v3 coverage is weak | New relevant result, with unrelated paths still ahead. |
| `c01`, Airlock bridge setup | Gold ranks 3/6 → 1/8 | Main target improved to rank 1. |
| `d03`, OmniRoute primary health | Gold stays rank 1; coverage weak → strong | Stronger match-quality label at the same rank. |
| `b03`, `TEST_ACCESS` | Still no gold path; top `inventory/sites/notice.bezrabotnyi.com.md:26` | The communications policy and Airlock bridge gold paths remain absent from top ten. |
| `c02`, remote MCP through Airlock | Still no gold path; top `inventory/sites/airlock.md:134` | The Airlock bridge runbook remains absent from top ten. |
| `e02`, server-100 failure availability | Still no gold path | Its sole gold file is outside the configured docs scope (see below). |
| `f02`, Windows workstation reachability | Still no gold path | Its sole gold file is outside the configured docs scope (see below). |

The negative `e04` router SSH-key-value query still returns a strong match at
`inventory/services/exmanager.md:165`; `f04` Windows account-password query
still returns a strong match at `inventory/devices/windows-beyondinfinity.md:9`.
The S21 PIN and personal Matrix-room queries remain weak-only hits. Receipts
store locators and match metadata, not returned file bodies or secret values.

## Gold paths outside the shared scope

The benchmark says GrepMesh is restricted to `/home/roomhacker/ServersAdministartion/docs`
and XERJ results are filtered to `ax_path` prefix `docs/`. The current gold set
also contains seven paths under the repository's top-level `infra/` directory,
outside that shared corpus:

| Gold path | Tasks | In-scope alternate gold |
|---|---|---|
| `infra/android-s21/README.md` | `a01`, `a02` | Yes: `docs/inventory/devices/android.md` |
| `infra/access-topology.md` | `e01`, `e02`, `e03`, `f01`, `f02` | `e01`, `e03`, `f01` also have an in-scope device path; `e02` and `f02` have no in-scope gold path |

Both top-level files exist in the source repository, but were not admitted by
the fixed docs-only benchmark scope. Raw quality metrics above retain the
unchanged gold; in particular, the `e02` and `f02` misses cannot be treated as
evidence of ranking failure within this corpus. No scope-adjusted score or
gold edit was substituted.

Further bounded source readback during delivery found that neither `b03` gold
file contains the literal `TEST_ACCESS` (`rg -n TEST_ACCESS` over both files
returns no matches). That gold therefore cannot measure exact-identifier
retrieval of a literal present in its target. For `c02`, the gold Airlock runbook
describes the MCP proxy in Russian, while the fixed query asks in English.
The new ranker has no translation/embedding stage; this is a cross-language
limitation. These observations do not change the raw scores or turn the 85%
target into a pass. The installed agent skill now explains bounded query
refinement and reading the actual source section; a workflow with follow-up
queries is a separate consumer test, not a replacement for the fixed benchmark.

## Latency and host conditions

| Query | Warm P50 | P95 | P99 |
|---|---:|---:|---:|
| `a01` | 277.56 ms | 367.31 ms | 394.36 ms |
| `a02` | 422.03 ms | 497.56 ms | 645.23 ms |
| `d04` | 303.53 ms | 353.28 ms | 370.40 ms |
| Aggregate (90) | 315.12 ms | 473.48 ms | 530.61 ms |

V2 aggregate ranked P95 was 252.39 ms; v3 was slower in this sample. Host
telemetry during v3 had load1 51.87 before / 47.14 after, minimum available
memory 29.33 GiB, average sampled CPU busy 31.79% (max 38.2%), and I/O PSI
`some avg10` 16.19–19.29%. The endpoint exposes no engine-duration field, so
the v3 increase cannot be attributed to the ranker separately from MCP and
shared-host work. No ranked-specific latency target was defined in this
benchmark; the prior 206.56 ms threshold applies to the exact-search comparison
and is not silently reused as a ranked-mode acceptance gate.

The post-run source hash inventory still had 299 files and 2,359,136 bytes;
all per-file hashes matched `docs-files-post-v2.json`. The ServersAdministartion
HEAD changed between captures (`6299c0e`→`4762c55`), while docs-scoped dirty
status and every docs-file hash remained identical. Therefore the fixed
document content stayed stable during the v3 run, although no pre-v3 commit
identity claim is made.

Receipts: `candidate-ranked-v3.jsonl`, `candidate-ranked-v3.host.json`,
`docs-files-post-v3.json`, `candidate-ranked-v2.jsonl`, and
`baseline.jsonl`. Native agent token use remains unknown; no token-savings
claim is supported.
