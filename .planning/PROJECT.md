# GrepMesh

## What This Is
GrepMesh indexes files on the operator's Linux and Mac computers and provides cross-host search and OCR through native MCP and its Console.

## Core Value
Find real file contents across the configured computers with explicit coverage and real distributed execution.

## Requirements

### Validated
- Configured Linux and Mac indexes expose real files through MCP and the Console.
- Private CUDA workers use bounded native execution and preserve durable OCR acknowledgements.

### Active
- [ ] Explain and repair the retained OCR queue on server-44.
- [ ] Allow the explicitly authorized Mac M1 to perform local and distributed CPU OCR.
- [ ] Restore the Mac mini route when its physical host is reachable.

### Out of Scope
- Clearing unprocessed queues or claiming index completeness without durable commits.
- Stopping foreign GPU consumers, changing their limits, or bypassing authentication.

## Context
On 2026-10-12 server-100 reports 8 pending OCR inputs, server-44 26897, Mac M1 1; server-88 is Ready. GPU44 reports available, GPU88 has insufficient VRAM. The current ComputeWorker and ComputeClient accept only CUDA. Mac mini fails LAN ARP and SSH. Existing src/ocr.rs is foreign WIP and must be preserved and reconciled explicitly before any source collision.

## Constraints
- **Capacity**: preserve existing finite Linux budgets and Mac foreground capacity; one native OCR job per worker.
- **Delivery**: existing canonical checkout and main; scoped owned commits and real consumer proof.
- **Data**: preserve queues, indexes, rollback material, and unrelated work.

## Key Decisions
| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Explicit CPU worker support for Mac M1 | User requests local and shared Mac processing without CUDA | Pending |
| Real queued input plus indexed readback for acceptance | Capability status alone cannot prove extraction | Pending |

---
Last updated: 2026-10-12 after authorized OCR recovery request.
