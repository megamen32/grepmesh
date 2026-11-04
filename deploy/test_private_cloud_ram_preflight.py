"""Offline/no-network tests of private Yandex + RAM-only release preflight."""
import hashlib
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

from private_cloud_ram_preflight import assess, InvalidInput, main, validate_native_config


SHA = hashlib.sha256(b"synthetic-binary-bytes").hexdigest()
COMMIT = "f" * 40


class PrivateCloudPreflightTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="grepmesh-synthetic-preflight-")
        self.addCleanup(self.tmp.cleanup)
        self.dir = pathlib.Path(self.tmp.name)
        self.binary = self.dir / "binary"
        self.binary.write_bytes(b"synthetic-binary-bytes")
        self.receipt = self.dir / "release.json"
        self.source = self.dir / "100.json"
        self.receiver = self.dir / "44.json"
        self.release = {
            "binary_sha256": SHA, "binary_bytes": 22, "source_commit": COMMIT,
            "production_deployed": False, "build_default_features": ["ocr", "local-stt"],
            "canary_verified": [
                "health_200", "mcp_initialize", "search.rank_boolean_default_true",
                "ranked_search_absent", "rank_search_parity",
                "read_text_synthetic_positive", "read_text_outside_root_denied",
            ],
        }
        self.a = {"host_id": "server-100", "index_enabled": False,
                  "peers": [], "remote_index_source": None, "remote_index_store": None}
        self.b = {"host_id": "server-44", "index_enabled": False,
                  "peers": [], "remote_index_source": None, "remote_index_store": None}
        self.write()

    def write(self):
        self.receipt.write_text(json.dumps(self.release))
        self.source.write_text(json.dumps(self.a))
        self.receiver.write_text(json.dumps(self.b))

    def assess(self):
        return assess(self.binary, self.receipt, self.source, self.receiver, SHA, COMMIT)

    def assert_blocked(self, check):
        report = self.assess()
        self.assertEqual(report["decision"], "NO_GO")
        self.assertFalse(report["production_approved"])
        self.assertFalse(report["performed_deployment"])
        self.assertIn(check, [v["name"] for v in report["checks"] if not v["passed"]])

    def test_all_structural_inputs_still_never_approve_production(self):
        report = self.assess()
        self.assertTrue(report["local_source_contract_passed"])
        self.assertEqual(len(report["unverified_external_gates"]), 6)
        self.assertFalse(report["production_approved"])

    def test_missing_explicit_index_enabled_rejected_default_is_true(self):
        del self.a["index_enabled"]
        self.write()
        self.assert_blocked("server-100.no_disk_fts")

    def test_persistent_receiver_index_rejected(self):
        self.b["remote_index_store"] = {"path": "/var/lib/grepmesh-mcp/index.sqlite"}
        self.write()
        self.assert_blocked("server-44.no_legacy_remote_store")

    def test_legacy_source_to_receiver_rejected(self):
        self.a["remote_index_source"] = {"index_host": "server-44"}
        self.write()
        self.assert_blocked("server-100.no_legacy_remote_source")

    def test_explicit_disk_index_path_rejected(self):
        self.a["index_path"] = "/var/lib/grepmesh-mcp/index.sqlite"
        self.write()
        self.assert_blocked("server-100.no_disk_index_path")

    def test_forged_candidate_sha_rejected(self):
        self.binary.write_bytes(b"changed-bytes")
        self.assert_blocked("binary.explicit_sha_pin")

    def test_receipt_source_commit_mismatch_rejected(self):
        self.release["source_commit"] = "0" * 40
        self.write()
        self.assert_blocked("source.explicit_commit_pin")

    def test_missing_ocr_feature_rejected(self):
        self.release["build_default_features"] = ["local-stt"]
        self.write()
        self.assert_blocked("features.ocr_and_stt")

    def test_symlink_candidate_rejected(self):
        self.binary.unlink()
        self.binary.symlink_to(self.receipt)
        with self.assertRaisesRegex(InvalidInput, "non_regular_or_shared_file"):
            self.assess()

    def test_invalid_json_fails_closed(self):
        self.receiver.write_text("{invalid")
        with self.assertRaisesRegex(InvalidInput, "invalid_json"):
            self.assess()

    def test_canary_metadata_not_signed_e2e(self):
        report = self.assess()
        self.assertTrue(report["metadata_claims_are_not_signed_acl_proofs"])
        self.assertIn("real_delegated_signed_acl_allow_deny", report["unverified_external_gates"])

    def test_cli_cannot_apply_and_returns_no_go(self):
        args = ["--candidate", str(self.binary), "--receipt", str(self.receipt),
                "--config-100", str(self.source), "--config-44", str(self.receiver),
                "--expected-sha256", SHA, "--expected-commit", COMMIT]
        from io import StringIO
        from contextlib import redirect_stdout
        buffer = StringIO()
        with redirect_stdout(buffer):
            self.assertEqual(main(args), 2)
        self.assertEqual(json.loads(buffer.getvalue())["decision"], "NO_GO")
        with self.assertRaises(SystemExit) as raised:
            main(args + ["--apply"])
        self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
