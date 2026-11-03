#!/usr/bin/python3
"""Source-only negative/unit checks. Synthetic policies are never release acceptance."""
import importlib.util
import unittest
from unittest.mock import patch

MODULE = "/home/roomhacker/agents-projects/grepmesh-worktrees/remote44-signed-bridge-20261103/scripts/cloud_index/hub_signed_mcp_bridge.py"
spec = importlib.util.spec_from_file_location("bridge", MODULE)
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)


def synthetic_policy(roots=None):
    return {
        "version": 1, "profile_id": bridge.PROFILE_ID,
        "profile_version": 1, "target": bridge.EXPECTED_TARGET,
        "tool": "mcp_call", "job_id": "unit-test-only",
        "roots_digest": "a"*64, "args_sha256": "b"*64,
        "read_roots": list(roots or ["/home", "/opt"]),
    }


class BridgeUnitTest(unittest.TestCase):
    def test_no_metadata_denied(self):
        with self.assertRaises(PermissionError):
            bridge._require_acl(None)
        with self.assertRaises(PermissionError):
            bridge._require_acl({})

    def test_exact_profile_required(self):
        p=synthetic_policy()
        p["profile_id"]="unrelated-client"
        with self.assertRaises(PermissionError):
            bridge._require_acl({"io.gptadmin/filesystem-acl": p})

    def test_unsafe_roots_denied(self):
        for roots in [["/"], ["/dev"], ["/home", "/dev/input"], ["/home/../dev"]]:
            with self.subTest(roots=roots):
                with self.assertRaises(PermissionError):
                    bridge._require_acl({"io.gptadmin/filesystem-acl": synthetic_policy(roots)})

    def test_acl_digest_required(self):
        p=synthetic_policy()
        p["args_sha256"]="not-a-digest"
        with self.assertRaises(PermissionError):
            bridge._require_acl({"io.gptadmin/filesystem-acl":p})

    def test_caller_supplied_policy_denied(self):
        with self.assertRaises(PermissionError):
            bridge.call("search",{"query":"needle","filesystem_acl":synthetic_policy()},None)
        with self.assertRaises(PermissionError):
            bridge.call("search",{"query":"needle","_meta":{"io.gptadmin/filesystem-acl":synthetic_policy()}},None)

    def test_unsigned_local_falls_back_to_original_only(self):
        with patch.object(bridge,"_native_tool_call",return_value={"isError":False}) as local:
            with patch.object(bridge,"_remote_signed_call",side_effect=AssertionError("remote invoked")):
                result=bridge.call("search",{"query":"needle"},None)
        self.assertFalse(result["isError"])
        local.assert_called_once()

    def test_mixed_host_not_deployed(self):
        d=bridge.call("search",{"query":"needle","hosts":["local","server-44"]},None)
        self.assertTrue(d["isError"])
        self.assertIn("parity",d["content"][0]["text"])

    def test_owner_metadata_only_for_explicit_host(self):
        meta={"io.gptadmin/filesystem-acl":synthetic_policy()}
        with patch.object(bridge,"_remote_signed_call",
            return_value={"isError":False,"content":[{"type":"text","text":'{"results":[],"partial":false}'}]}) as remote:
            result=bridge.call("search",{"query":"needle","hosts":"server-44"},meta)
        self.assertFalse(result["isError"])
        self.assertTrue(__import__("json").loads(result["content"][0]["text"])["partial"])
        remote.assert_called_once()

    def test_raw_historical_request_never_allowed(self):
        with self.assertRaises(PermissionError):
            bridge.call("read_text",{"host":"server-44","path":"/home/example"},None)

    def test_deleted_tools_never_resurrected(self):
        for name in ("ranked_search","search_text"):
            with self.assertRaises(ValueError):
                bridge.call(name,{"query":"marker"},None)

    def test_tool_catalog_has_one_search(self):
        upstream={"tools":[
          {"name":"search","inputSchema":{"type":"object","properties":{}}},
          {"name":"search_text","inputSchema":{"type":"object"}},
          {"name":"ranked_search","inputSchema":{"type":"object"}},
          {"name":"read_text","inputSchema":{"type":"object"}}]}
        with patch.object(bridge,"_rpc",return_value=upstream):
            listed=bridge.tool_list()
        names=[x["name"] for x in listed["tools"]]
        self.assertEqual(names,["search","read_text"])
        self.assertTrue(listed["tools"][0]["inputSchema"]["properties"]["rank"]["default"])

    def test_requires_write_free_acl(self):
        p=synthetic_policy()
        p["write_roots"]=["/home"]
        with self.assertRaises(PermissionError):
            bridge._require_acl({"io.gptadmin/filesystem-acl":p})

if __name__=="__main__":
    unittest.main()
