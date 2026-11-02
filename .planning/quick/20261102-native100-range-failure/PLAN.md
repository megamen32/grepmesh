# Native100 immutable source range diagnostics (read-only preparation)

## Baseline and scope
- 2026-11-02 16:23 UTC: existing sole resume service failed with source_parts_read=420/443. Saved failure receipt identifies line 232, but the subprocess error and actual failed offset were discarded.
- Do **not** restart, duplicate, or replace the builder, oversize guard, source archive, or production names-only service. No SSH/rclone/cloud calls in this change.
- Scope: one source module, one synthetic unittest module, this LHC ledger. Preserve strict length/SHA checks and fail-closed ACL/HTTP denials.

## Acceptance
1. Synthetic timeout includes validated part ordinal, offset and fixed category, with three bounded attempts.
2. Synthetic HTTP 429 includes category but no raw stderr or credential-like marker.
3. Other denial, corruption and pinned-checkpoint tests still pass under bounded test cgroup.
4. Separate independent review + existing-owner integration required before changing deployed builder. No cutover without full ACL, live parity, reboot/rollback gates.

## Scoped verified result (2026-11-02)
- 11 pinned checkpoint tests PASS and 19 verified reuse tests PASS, bounded user cgroup.
- No builder restart, no cloud read, no production configuration change.
- Bug remains open until owner integrates, independent review and safe existing-service resume; diagnostics only prepare future failure evidence.
