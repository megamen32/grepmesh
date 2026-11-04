#!/usr/bin/env python3
"""Read-only, offline admission for a private Yandex + RAM-only GrepMesh release.

This tool cannot deploy, reboot, restart, sign, fetch data, grant ACLs, or approve
production. Even if every *local* input passes, independent signed-client,
complete-corpus and postboot/rollback receipts remain mandatory NO_GO gates.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat

MAX_METADATA = 128 * 1024
MAX_BINARY = 128 * 1024 * 1024
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
COMMIT = re.compile(r"[0-9a-f]{40}\Z")
CANARY_FIELDS = {
    "health_200", "mcp_initialize", "search.rank_boolean_default_true",
    "ranked_search_absent", "rank_search_parity", "read_text_synthetic_positive",
    "read_text_outside_root_denied",
}
EXTERNAL_GATES = (
    "native100_profiler_tests_permitted",
    "native100_full_16way_immutable_index_parity",
    "real_delegated_signed_acl_allow_deny",
    "server44_scoped_reboot_and_165_object_ram_restore",
    "production_root_owned_deploy_authorized",
    "postcutover_rollback_acceptance",
)


class InvalidInput(ValueError):
    """An input cannot be safely interpreted as a local release artifact."""


def regular_file(path: Path, limit: int) -> int:
    try:
        st = path.lstat()
    except OSError as exc:
        raise InvalidInput("file_unavailable") from exc
    if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
        raise InvalidInput("non_regular_or_shared_file")
    if not (0 < st.st_size <= limit):
        raise InvalidInput("file_size_out_of_bounds")
    return st.st_size


def limited_json(path: Path) -> dict:
    size = regular_file(path, MAX_METADATA)
    try:
        with path.open("rb") as stream:
            data = stream.read(size + 1)
        parsed = json.loads(data)
    except (OSError, UnicodeError, ValueError) as exc:
        raise InvalidInput("invalid_json") from exc
    if not isinstance(parsed, dict):
        raise InvalidInput("json_object_required")
    return parsed


def hash_binary(path: Path) -> tuple[str, int]:
    size = regular_file(path, MAX_BINARY)
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            block = stream.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest(), size


def validate_native_config(config: dict, host: str) -> list[dict]:
    """Check structural compatibility; never infer a live private reader exists."""
    checks: list[dict] = []
    def check(name: str, ok: bool, reason: str) -> None:
        checks.append({"name": f"{host}.{name}", "passed": bool(ok),
                       "failure": None if ok else reason})
    check("identity", config.get("host_id") == host, "host_id_mismatch")
    check("no_disk_fts", config.get("index_enabled") is False,
          "index_enabled_must_explicitly_be_false")
    check("no_disk_index_path", config.get("index_path") in (None, ""),
          "persistent_index_path_not_allowed")
    check("no_legacy_remote_source", not config.get("remote_index_source"),
          "legacy_remote_index_source_not_allowed")
    check("no_legacy_remote_store", not config.get("remote_index_store"),
          "legacy_persistent_remote_index_store_not_allowed")
    return checks


def assess(candidate: Path, receipt: Path, config100: Path, config44: Path,
           expected_sha: str, expected_commit: str) -> dict:
    checks: list[dict] = []

    def check(name: str, passed: bool, failure: str) -> None:
        checks.append({"name": name, "passed": bool(passed),
                       "failure": None if passed else failure})

    if not SHA256.fullmatch(expected_sha) or not COMMIT.fullmatch(expected_commit):
        raise InvalidInput("explicit_pins_required")
    digest, length = hash_binary(candidate)
    proof = limited_json(receipt)
    source = limited_json(config100)
    receiver = limited_json(config44)
    check("binary.explicit_sha_pin", digest == expected_sha, "binary_sha_mismatch")
    check("binary.receipt_sha", proof.get("binary_sha256") == expected_sha,
          "receipt_binary_sha_mismatch")
    check("binary.receipt_bytes", proof.get("binary_bytes") == length,
          "receipt_binary_size_mismatch")
    check("source.explicit_commit_pin", proof.get("source_commit") == expected_commit,
          "source_commit_mismatch")
    check("receipt_not_deployed", proof.get("production_deployed") is False,
          "release_receipt_is_not_a_predeployment_candidate")
    features = proof.get("build_default_features")
    check("features.ocr_and_stt",
          isinstance(features, list) and
          {"ocr", "local-stt"}.issubset(set(x for x in features if isinstance(x, str))),
          "full_ocr_stt_build_not_attested")
    claimed = proof.get("canary_verified")
    check("receipt.synthetic_canary_is_only_claimed",
          isinstance(claimed, list) and CANARY_FIELDS.issubset(
              set(x for x in claimed if isinstance(x, str))),
          "required_synthetic_canary_claim_missing")
    checks.extend(validate_native_config(source, "server-100"))
    checks.extend(validate_native_config(receiver, "server-44"))
    local_ok = all(c["passed"] for c in checks)
    return {
        "schema_version": 1,
        "decision": "NO_GO",
        "read_only": True,
        "candidate_sha256": digest,
        "candidate_bytes": length,
        "source_commit": expected_commit,
        "local_source_contract_passed": local_ok,
        "checks": checks,
        "unverified_external_gates": list(EXTERNAL_GATES),
        "reason": ("local_inputs_compatible_but_external_production_gates_unverified"
                   if local_ok else "local_release_or_architecture_incompatible"),
        "production_approved": False,
        "performed_deployment": False,
        "metadata_claims_are_not_signed_acl_proofs": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--config-100", type=Path, required=True)
    parser.add_argument("--config-44", type=Path, required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--expected-commit", required=True)
    args = parser.parse_args(argv)
    try:
        result = assess(args.candidate, args.receipt, args.config_100,
                        args.config_44, args.expected_sha256, args.expected_commit)
    except InvalidInput as exc:
        result = {"schema_version": 1, "decision": "NO_GO", "read_only": True,
                  "production_approved": False, "performed_deployment": False,
                  "reason": str(exc), "unverified_external_gates": list(EXTERNAL_GATES)}
    print(json.dumps(result, sort_keys=True, ensure_ascii=False))
    # Never return GO: this preflight does not receive a genuine signed proof.
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
