# Roadmap: GrepMesh OCR recovery

## Overview
Repair the user's current distributed OCR failure on the existing real fleet, preserving retained work.

## Phases
- [x] **Phase 1: Distributed OCR recovery** - Diagnose retained work, enable explicit CPU execution, and verify extraction and mesh reachability.
- [ ] **Phase 2: Parallel resource-admitted OCR queue** - Distribute durable real work across44/M1/88 and enforce exclusions before inference.

## Phase Details
### Phase 1: Distributed OCR recovery
**Goal**: Real queued inputs can be processed by available authorized workers.
**Depends on**: Nothing
**Requirements**: OCR-01, OCR-02, OCR-03, MESH-01
**Success Criteria**:
  1. Real blocked input is diagnosed and extracted through the repaired path.
  2. Mac M1 processes local and foreign work without CUDA.
  3. Indexed text readback and durable queue acknowledgement are verified.
  4. Mac mini catalog is verified or its physical dependency is explicit.
**Plans**: 1
Plans:
- [x] 01-01: Diagnose, implement, deliver, and verify the authorized recovery.

## Progress
| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Distributed OCR recovery | 1/1 | Real CPU extraction, durable indexed readback and all five mesh routes accepted | 2026-10-13 |

### Phase 2: Parallel resource-admitted OCR queue
**Goal**: Real durable work runs independently across free44/M1/88 using measured resources and task/device performance.
**Depends on**: Phase 1
**Requirements**: PAR-01, PAR-02, PAR-03, PAR-04, PAR-05
**Success Criteria**:
  1. Up to3 inputs run with one host reservation each, without server100 fallback.
  2. Real ordinary queue completions and SQLite commits are verified on44/M1/88.
  3. Excluded paths never reach decoding or inference; files remain.
  4. Same-class/device measurements guide selection and new file versions remain pending.
**Plans**: 1
Plans:
- [ ] 02-01: Integrate device measurements, deliver the dispatcher and prove the real queue.

Phase2 executing; Phase1 results remain complete at their recorded snapshot.
