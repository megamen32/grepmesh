# Neural embedding decision — 2026-10-28

## Decision

Keep ranked search lexical; this bounded probe does not justify an embedding
dependency. The gateway returned a valid CPU embedding response, and its top
toy result matched the lexical overlap baseline.

## Evidence

- The canonical server-88 inventory says the embedding gateway is pinned to
  CPU so Whisper retains priority on the shared RTX 3080 Ti:
  `ServersAdministartion/docs/inventory/devices/88.md` (2026-10-21 entry).
- The owning gateway README specifies that `QWEN_EMBEDDING_FORCE_CPU=1` skips
  broker admission and sends embeddings to the CPU Ollama. GPU admission is
  restored only by changing that setting.
- Live read-only checks on server-88 showed `qwen-embedding-gateway.service`
  active, with `QWEN_EMBEDDING_FORCE_CPU=1`; NVIDIA reported 2293 MiB used and
  9617 MiB free. CPU load average was about 14 on 88 reported CPUs, available
  RAM was about 84.8 GB, and memory PSI was zero. No service settings were
  changed.
- The first request targeted `127.0.0.1:11440`; it returned `URLError` after
  0.003 s. The original handler recorded only the exception class, so its
  `reason` was not retained. A follow-up listener check showed the gateway bound
  specifically to `192.168.2.75:11440`; a TCP probe to loopback returned
  `ConnectionRefusedError(111)`, while the documented host address accepted a
  connection. This identifies the endpoint typo; no HTTP response was obtained
  from the loopback attempt, and it was not replayed at that address.
- One POST to `http://192.168.2.75:11440/v1/embeddings` carried three short
  public GrepMesh-description snippets and one question (four inputs, 233
  characters total), with a 30-second client timeout. A remote privileged
  process read the protected token file and used the token only in the request
  header; neither token nor vectors were printed. Provider response: HTTP 200,
  model `qwen3-embedding:4b`, device `cpu`, 2048 dimensions, four vectors,
  `prompt_tokens=43`; elapsed time was 1.21 s. Cosine similarities to the
  question vector were `[0.677309, 0.327341, 0.494493]` in snippet order.
  Token-overlap counts were `[1, 0, 0]`, so the toy top result agrees. This is
  one toy comparison, not a full ranking benchmark.
- The existing lexical candidate benchmark reports MRR 0.3477, nDCG@10
  0.4122, recall@5 0.5000, and warm P95 112.77 ms over its fixed 24-task
  corpus (`candidate-comparison.md`). This does not establish whether neural
  vectors improve ranking.

## Limits and next step

The successful probe provides one small cosine comparison only. It shows no
top-result gain over token overlap on these three snippets, while the gateway
call took 1.21 s. It does not establish broad quality or latency for production
queries. The existing 24-task lexical benchmark remains the meaningful
ranking-quality evidence (MRR 0.3477, nDCG@10 0.4122, recall@5 0.5000, warm
P95 112.77 ms). Together, the toy result and the substantial per-request cost
do not justify adding an embedding dependency to ranked search.
