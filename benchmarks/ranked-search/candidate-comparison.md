# Candidate comparison

Captured 2026-10-28 against candidate endpoint `http://127.0.0.1:19430/mcp`. The candidate and baseline indexes both reported `Ready`, 299 indexed docs, generation 310, and the same `/home/roomhacker/ServersAdministartion/docs` root. Both passed a complete literal `s21-lease` probe to `docs/inventory/devices/android.md`. This comparison reused the unchanged 24-query `corpus.json`; it did not tune gold paths after seeing candidate output.

Each mode received one quality observation for all 24 tasks and 30 warm repeats each for `a01`, `a02`, and `d04` (exact identifier, natural-language question, ambiguous query). Each receipt has 114 rows and zero tool errors.

| Mode | MRR | nDCG@10 | Recall@5 | Negative false positives | Warm P50 | P95 | P99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Baseline `search` | 0.2014 | 0.2029 | 0.2500 | 0/4 | 152.81 ms | 186.56 ms | 285.97 ms |
| Candidate exact `search` | 0.2014 | 0.2029 | 0.2500 | 0/4 | 99.64 ms | 131.12 ms | 486.00 ms |
| Candidate `ranked_search` | 0.3477 | 0.4122 | 0.5000 | 4/4 | 70.41 ms | 112.77 ms | 118.36 ms |

The candidate exact output matched baseline exact output for all 24 quality tasks: identical returned path lists, gold ranks, reciprocal rank, nDCG, recall, and negative-hit result. Its aggregate P95 is below the requested 206.56 ms target. Its exact-ID `a01` latency tail is an outlier: per-task P95/P99 rose from baseline 154.07/242.86 ms to 438.81/558.75 ms, with 3 of 30 warm observations above 206.56 ms (max 593.92 ms). The other exact latency tasks improved: `a02` P95 184.72→118.42 ms and `d04` 221.31→113.47 ms. Host telemetry was not sampled during the run, so the `a01` tail cannot be attributed to shared-host noise; treat it as an observed per-query latency regression, not as a proven noise event.

Ranked search improved overall locator ranking over baseline exact: MRR +0.1463, nDCG@10 +0.2093, Recall@5 +0.2500. The latency P95 target is met. Negative-query behavior regressed: all four unanswerable prompts returned at least one document, versus zero for exact search. This is a locator-level false-positive measure; the receipt contains paths and line locators only, not secret values or full document text.

For natural-language tasks, ranked search returned a strict gold path in the top ten for 3/7 queries; baseline exact returned 0/7. One additional query (`f02`) is fact-equivalent despite missing the strict gold path: the candidate's top result `docs/inventory/devices/windows.md:11` is followed by line 12, which lists reachability to/from `88`, `100`, `44`, and `homeassistant`, matching the requested fact in `infra/access-topology.md`. Manual tally is therefore 4/7 natural-language queries with an answer-bearing top-ten path, still below an 85% target. Strict-path metrics remain unchanged in the table.

## Relevance failures to fix

| Task | Query | Expected path | Candidate ranked top hit | Assessment |
|---|---|---|---|---|
| `b03` | `TEST_ACCESS` | `docs/inventory/services/communications.md`; `docs/runbooks/airlock-app-bridge.md` | `docs/superpowers/plans/2026-07-22-autonomous-haos-recovery-edge.md:22` | Generic plan heading (“Define the autonomous controller contract with failing tests”); no Matrix voice-test policy. Gold paths absent from top ten. |
| `c02` | “How does a remote MCP request travel through the Airlock bridge?” | `docs/runbooks/airlock-app-bridge.md` | `docs/runbooks/fleet-codex-watch-20261007.md:142` | Scheduled monitor receipt, not the Airlock bridge route. Other early hits are the Android ShellMCP endpoint and GrepMesh peer routing. Gold runbook absent from top ten. |
| `d02` | “How does omniroute-guard decide whether its main route is healthy?” | `docs/runbooks/omniroute-fallback-guard.md` | `docs/inventory/devices/100.md:261` | Lists model choices; does not explain the guard's live probe or health decision. Gold runbook absent from top ten. |
| `e02` | “What remains available if server-100 fails?” | `infra/access-topology.md` | `docs/inventory/devices/88.md:49` | Describes the OpenCodex proxy, unrelated to the failure matrix. The later router DNS failover hit is a different recovery path. Gold path absent from top ten. |
| `b02` | “Where may test voice calls be placed, and which recipients are forbidden?” | `docs/inventory/services/communications.md` | `docs/inventory/devices/android.md:74` | Describes S21 UI control; communications contract is only at rank 6. |
| `a03` | `agent-device devices --json` | `docs/inventory/devices/android.md` | `docs/inventory/devices/mac-m1.md:58` | Correct Android path is only rank 5; unrelated Mac result outranks the exact device procedure. |

The four negative prompts also produce ranked hits: S21 unlock PIN → `docs/inventory/devices/android.md:190`; personal Matrix test room → `docs/inventory/devices/android.md:74`; router SSH key value → `docs/inventory/devices/vusa.md:121`; Windows password → `docs/inventory/devices/vusa.md:653`. No returned secret values were recorded. These cases need abstention or safe unanswerable handling at the answer layer.

All calls were `partial=false` with zero errors. Candidate exact reported `truncated=true` only for `d01` at its 100-match cap (one quality observation). Ranked search reported `truncated=true` on all 114 rows because it deliberately returned the top ten while more matches existed; this is top-k result capping, not partial coverage. Its `partial` flag remained false.

Latency is client-observed through local Streamable HTTP, includes MCP and service work, and has no per-search engine timing from GrepMesh. Host CPU/load/IO telemetry was not captured during either run. Native-agent token use remains unknown. No source, runtime, index, or configuration changes were made by this benchmark worker.

Receipts: `baseline.jsonl`, `candidate-exact.jsonl`, and `candidate-ranked.jsonl`. The zero-index fallback capture remains excluded as `zero-index-rg-fallback.invalid.jsonl`.
