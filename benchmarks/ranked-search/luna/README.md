# Luna retrieval and native-consumer acceptance

## Bounded blind context experiment

`context_comparison.py` makes one retrieval for each of four fixed tasks from
`../corpus.json` (a01, a02, a05, b02) on each of three existing endpoints: old
literal GrepMesh on 19429, ranked GrepMesh on 19431 and XERJ on 19200. Both GrepMesh
and XERJ are restricted to ServersAdministartion docs. Top three results only;
no query rewrite, additional read or gold answer is supplied. The a01 identifier
query is presented to the reader as a lease-workflow question. Each returned
context is bounded to 16000 characters. Stable shuffled aliases hide the engine;
`blind-mapping.json` records the mapping before inference.

Two separate fresh native Codex sessions requested `gpt-6-luna` with the same
contexts in different orders. They had no MCPs/config loaded, no implementation,
gold facts, old reports or source files, and were instructed to answer solely
from their individual supplied contexts. This is a retrieved-context experiment,
not an autonomous agent benchmark or a human preference judgment.

Native command for each pass (substitute 1 or 2):

```sh
codex exec --ignore-user-config --skip-git-repo-check --ephemeral -C /tmp \
  --sandbox read-only -c 'approval_policy="never"' -m gpt-6-luna --json \
  -o "$LUNA_DIR/blind-pass-1.answer.txt" - \
  < "$LUNA_DIR/blind-pass-1.prompt.txt" \
  > "$LUNA_DIR/blind-pass-1.events.jsonl" \
  2> "$LUNA_DIR/blind-pass-1.stderr.txt"
```

Fresh native quota read before this batch: 2026-10-28T22:35:53.179849Z,
ordinaryUsageAllowed=true, pro, primary usedPercent 52 of 100,
windowDurationMins 10080, resetsAt 1793774157. No credit balance or PAYG fallback.
Requested model selection is explicit; JSONL usage attests successful native
inference but does not separately echo a server-resolved model identifier.

`scoreboard.json` records all 24 answer observations and exact native usage.
Both passes agreed: literal 0/6 supported answerable observations; ranked 2/6;
XERJ 2/6. Each engine correctly abstained on both unanswerable PIN observations.
Only the USB-independent reverse-SSH answer had sufficient retrieved evidence
in ranked and XERJ. Lease workflow and voice-test recipient questions were
insufficient in all three top-three context sets. This does not meet an 85 percent
quality target and does not establish ranked superiority over XERJ. It does show
that related-document hits do not by themselves justify a factual answer.

The mechanical rubric verifies the reverse-SSH fact and explicit abstention;
it is not a general semantic judge. The first pass emitted one extra trailing
`}`; the valid leading JSON array is used and the anomaly is preserved. Native
usage was 43741 input tokens per pass, with 616 and 552 output tokens respectively.
No token saving or cost claim follows from this small experiment.

`retrieval-receipts.json` retains timing/count/partial flags only. Captured prompts,
answers and event traces remain local and ignored because full contexts can
contain private infrastructure documentation. Regenerate with
`python3 benchmarks/ranked-search/luna/context_comparison.py`; summarize after
both native runs with `python3 benchmarks/ranked-search/luna/summarize.py`.

## Autonomous native MCP checks

The earlier `native-events.jsonl`, `native-answer.txt` and `native-stderr.txt`
are preserved failed evidence: the MCP tool required approval under policy
`never`, so no search executed. They are not counted as successful acceptance.
Fresh native read-only tests are recorded separately after the read-only tool
annotations are fixed; their outcome must not be inferred from this experiment.
