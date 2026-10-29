# Ranked search rollout receipt

- Source: `3850010` (`src/ranked.rs` SHA-256 `613f1990300db1d38ce8e3c51a6e21ede67ce3ec001ba34a600c8cbf58107c34`). Focused checks: ranked 14/14, peer proxy 5/5; default suite completed in 76s.
- Measurement: 17/20 raw correct-file retrievals (85%). This is file retrieval, not answer accuracy.
- Installed runtime: immutable release `704a8aea695e9b75bb8c571c7c5cb934692d77f3aced5487d23528a41b0e8266`; binary SHA-256 matches the release ID.
- Consumer proof on `server-100-ranked`: `/health` returned 200; `search_status` was Ready; scoped `HTTPTransport` search in `httpx-reference` returned `strong_match`, `partial=false`, and source-verified citation `/home/roomhacker/.local/share/grepmesh-ranked/reference-httpx-b5addb6/httpx/_transports/default.py:133-137`.
- An earlier rollout instruction selected `RemoteIndexStore`, outside the admitted roots; that result was not a product-quality finding. The admitted-root canary above passed.
