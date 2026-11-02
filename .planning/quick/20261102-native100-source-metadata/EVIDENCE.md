# Source verification, 2026-11-02

- Base: d902be04476353d2d8cdf14fb611866ca2dd779f.
- RED.txt: exact metadata timeout reproduced before the production change. One regression errored with Immutable source part metadata timed out from lsjson --stat --hash.
- GREEN.txt: focused source tests, 32 passed before adding the run-wiring test.
- FULL-TESTS.txt: final cloud-index suite, 33 passed in 10.81s; no test failures. Synthetic module-level assertions also collected. The explicit historical44 live acceptance script is not run.
- git diff --check: passed.
- New integrity coverage: changed receipt/checkpoint, duplicate or invalid part metadata, wrong size/key, short/appended/corrupt data, denied/missing range, fast-path SHA mismatch, and binding before publisher input reads.
- Source lookup is pinned to the existing completed replay receipt checkpoint SHA. Full reconstructed SHA and final-range EOF are mandatory. Legacy unbound MD5 metadata checks and all shard upload/reuse checks are unchanged.
- Independent review and live same-part canary pending. Builder restart, final candidate and broad migration acceptance are not yet claimed.

Independent REVIEW.md accepted source. Live unit-config canary PASS:12086993 bytes, expected SHA,443 bound source parts,EOF verified,99.418s,cloud writes0. First two canary harness failures are retained verbatim; exact unit gateway and rclone selectors corrected them without credential changes. Both scoped bugs resolved with native CAS and real proof; candidate builder still not resumed.
