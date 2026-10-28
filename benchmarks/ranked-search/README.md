# Ranked search baseline

`corpus.json` is a fixed 24-task evaluation set with six task styles: exact identifiers, natural-language questions, snake/camel spelling, ambiguous requests, and unanswerable requests. Gold paths and answer facts are grounded in `/home/roomhacker/ServersAdministartion/docs`; the source was checked against root `AGENTS.md`, `infra/access-topology.md`, and `docs/inventory/README.md`. The XERJ `serversadmin-docs` map reported 15,042 records at capture. The XERJ request filters `ax_path` to `docs/`; GrepMesh is explicitly scoped to the same repository `docs/` directory on `server-100`. The runner rejects a GrepMesh endpoint unless its index is `Ready`, has indexed files, matches the manifest root, and returns a complete literal `s21-lease` hit from `docs/inventory/devices/android.md`.

## Reproduce

Use the supported GrepMesh Streamable HTTP endpoint. The standard isolated baseline in this task is `http://127.0.0.1:19429/mcp`; replace it with the candidate endpoint for a candidate run. XERJ defaults to the installed local stdio MCP client and loopback node. Credentials, if required by a different endpoint, belong in environment variables only and are never written to the result file.

```sh
python3 benchmarks/ranked-search/run.py --repetitions 30 \
  --grepmesh-url http://127.0.0.1:19429/mcp \
  --xerj-url http://127.0.0.1:19200 \
  --out benchmarks/ranked-search/results.jsonl
```

For a different GrepMesh URL, set `GREPMESH_MCP_URL` or pass `--grepmesh-url`. GrepMesh auth defaults to the `GREPMESH_MCP_AUTH` environment variable. XERJ stdio reads `XERJ_URL` and `XERJ_AUTH` from its process environment. To use an XERJ Streamable HTTP MCP endpoint, set `--xerj-transport http` and pass that endpoint as `--xerj-url`; optional auth defaults to `XERJ_MCP_AUTH`.

Every task gets one quality observation. By default `a01`, `a02`, and `d04` then get 30 additional warm repetitions for exact-identifier, natural-language, and ambiguous-query latency samples. The first observation is labeled `first_or_uncached`; it is not guaranteed to be a cold process or index cache. Requests are sequential and never retried. GrepMesh async jobs are followed through `search_status` to terminal state; this is supported polling, not replay. `--limit 2` is a smoke run and omits the default ambiguous latency case.

`run.py` reports MRR, nDCG@10, recall@5, negative false-positive rate, and client-observed P50/P95/P99. It records only top locators and response byte counts, never full returned documents or auth headers. XERJ's server `took` is reported when present. MCP initialization latency is separately recorded. Server-side GrepMesh duration is not returned consistently by the tool response, so its reported latency is end-to-end. XERJ's `took` allows engine duration to be separated from client elapsed time; exact MCP overhead is not claimed because the tool response is not a complete transport trace.

## Candidate mode comparison

The fixed baseline exact receipt and candidate mode receipts are listed in `candidate-comparison.md`. To rerun only a candidate endpoint while comparing with the stored XERJ baseline, use `--skip-xerj` and keep the same corpus, latency task IDs, exact mode, and caps:

```sh
python3 benchmarks/ranked-search/run.py --repetitions 30 \
  --grepmesh-url http://127.0.0.1:19430/mcp \
  --grepmesh-tool search --grepmesh-label candidate_exact \
  --grepmesh-mode case_insensitive_literal --grepmesh-max-matches 100 \
  --skip-xerj --out benchmarks/ranked-search/candidate-exact.jsonl

python3 benchmarks/ranked-search/run.py --repetitions 30 \
  --grepmesh-url http://127.0.0.1:19430/mcp \
  --grepmesh-tool ranked_search --grepmesh-label candidate_ranked \
  --grepmesh-limit 10 --skip-xerj \
  --out benchmarks/ranked-search/candidate-ranked.jsonl
```

The ranked mode's `truncated=true` means more than the requested top ten exist; `partial=false` is the coverage signal. Exact mode retains the baseline's case-insensitive literal matching, 100-match cap and 1000 ms foreground wait.

XERJ is sent a lexical `multi_match` restricted by `ax_path` prefix because the shared index also contains paths outside `docs/`; this is necessary for the common corpus scope, though it differs from plain-string definition-first XERJ query mode.

The prior zero-index capture is preserved as `zero-index-rg-fallback.invalid.jsonl`; it is explicitly invalid because the endpoint was in `indexed+rg-fallback` with `indexed_files=0`. The prior three-query Luna probe is useful context, not a statistical baseline; see `/home/roomhacker/ServersAdministartion/ops/xerj/benchmark/REPORT.md`. Native agent token use is not available from these MCP calls and remains unknown.

## Isolated source-manifest lifecycle proof

`runtime_lifecycle.py` starts only the supplied immutable binary on an ephemeral
loopback port with a task-owned temporary root, manifest, config and SQLite
index. It exercises admitted delete/recreate/edit/rename/delete, denial of an
unadmitted harmless fake canary file, symlink denial, restart-time manifest
revocation, and malformed-manifest Degraded/partial behavior. It writes a
locator/count-only JSON receipt and removes its process and temporary state.
It does not run against a fleet service or production index. Run it with:

```sh
python3 benchmarks/ranked-search/runtime_lifecycle.py \
  --binary /path/to/immutable/grepmesh-mcp \
  --out benchmarks/ranked-search/runtime-lifecycle.json
```

The lifecycle receipt is separate from the ranked quality and latency
benchmarks; it proves only the listed source-policy transitions for that
binary.
