# Native100 immutable-source metadata recovery

Owner: native board worker, 2026-11-02. Existing card 25fd6e97-6f52-4227-82bd-f4fdca3d9831.
Goal: finish the same private candidate builder from the already exported 443-part archive.
Source base d902be04476353d2d8cdf14fb611866ca2dd779f. Old builder and guard both failed/MainPID=0.
Defect: part424 range fallback first performs rclone lsjson --stat --hash, timing out after45s despite the independently SHA-verified immutable checkpoint.
Scope: pinned checkpoint metadata recovery, exact size/EOF/SHA verification, fast regression and integrity negatives.
No source export, native DB deletion, serving/ACL change, credential copying, foreign WIP or duplicate builder.
TEST_ACCESS.md is absent in primary checkout. Existing profile-scoped server100 shell and configured rclone are authorized; tests are synthetic and cloud-free.
Graphify query unavailable (executable absent in PATH); targeted source inspection used. AgentMemory project recall returned unrelated history, not relied on.
Plan: RED exact metadata timeout; smallest implementation; relevant suite; independent review; publish/delivery gate; authorized main integration; one guarded same-job resume and terminal proof.
Current status: regression preparation. A source fix is not candidate acceptance, and candidate acceptance is not full migration acceptance.
