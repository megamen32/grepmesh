# Index-health follow-up: literal-mode misses on some OLD non-userio docs

Status: open (recorded as its own task; explicitly out of scope of the
2026-09-10 userio-index-adapter delivery)
Date: 2026-09-10
Discovered: during userio adapter acceptance (pre-existing index state that
predates all userio work)

## Symptom

server-100 literal mode misses some old indexed documents. Confirmed example:
content of `/etc/grepmesh-mcp.service` (and similar old docs) is in the FTS
index and is findable via `regex` mode and readable via `read_text`, but a
matching literal query returns no hit for it. New userio rows are unaffected
(userio literal coverage is full: 414/414 with bodies, verified 2026-09-10).

## Known constraints

- The index DB is ~25 GB with the extraction cache inside it. A full rebuild
  re-extracts the corpus (~7-10 h observed) — do NOT prescribe a full rebuild
  as the first remedy.
- Mass mtime-touch bursts trip the hot-directory guard (one atomic write ≈
  3-5 inotify events; server-100 deploy threshold 400 events/60 s, 6 h hot
  cooldown degrades affected buckets to metadata-only). Any repair must move
  ≤ ~100 events per round with pauses (proven recipe: 20-file batches every
  ~45 s).
- Full-rebuild interval on server-100 is 7 days (`full_rebuild_min_interval_ms`
  604800000, reverted from the brief 3-day bootstrap experiment in 8c22528 by
  65f5d2f); a restart after the interval elapses triggers the rebuild at
  startup, so avoid unnecessary restarts near the due date.

## Candidate remedies (choose after root-causing, cheapest first)

1. Root-cause why literal (FTS phrase) misses rows whose bodies contain the
   phrase: compare failing paths' FTS rows vs corpus (b tokenizer? trigram vs
   unicode61 segmentation on old rows? metadata-only degradation history?).
2. If it is per-row body state: targeted repair = re-index ONLY the uncovered
   paths via bounded mtime-touch batches (blast-radius recipe above), then
   re-run literal coverage diff until 100%.
3. Only if a systemic tokenizer/config defect is proven: plan a maintenance
   rebuild deliberately (announce the multi-hour window), never as a default.

## Acceptance when picked up

- A coverage audit script/query lists every indexed doc whose literal hit is
  missing; the count is the success metric.
- After repair: literal canaries on the previously missed docs pass; regex and
  read_text unaffected; no hot-guard degradation of userio buckets during the
  repair window.
