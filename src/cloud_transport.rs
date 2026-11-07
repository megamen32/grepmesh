//! Explicit server-owned adapters for the existing private cloud ACL wire contract.
use anyhow::{anyhow, Context, Result};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use base64::Engine;
use std::{collections::BTreeMap, time::Duration};

#[derive(Deserialize)]
struct Route { url: String, secret_env: String }

pub(crate) async fn call(host: &str, tool: &str, arguments: Value,
    limit: usize, timeout_ms: u64) -> Result<Option<Value>> {
    let Ok(raw) = std::env::var("GREPMESH_CLOUD_SIGNED_ROUTES") else { return Ok(None) };
    let routes: BTreeMap<String, Route> = serde_json::from_str(&raw)
        .context("invalid signed cloud route configuration")?;
    let Some(route) = routes.get(host) else { return Ok(None) };
    call_route(route, tool, arguments, limit, timeout_ms).await.map(Some)
}

async fn call_route(route: &Route, tool: &str, arguments: Value, limit: usize, timeout_ms: u64) -> Result<Value> {
    let started = std::time::Instant::now();
    match call_route_once(route, tool, arguments.clone(), limit, timeout_ms).await {
        Err(error) if tool == "read_text"
            && arguments.get("start_line").and_then(Value::as_u64).unwrap_or(1) == 1
            && error.to_string() == "signed cloud response exceeds limit"
            && limit >= 8192 => {
            // A line window is not a byte bound (JSONL may contain one huge line).
            // Retry only this idempotent read with the reader's existing prefix API.
            let byte_limit = (limit / 16).min(64 * 1024);
            let mut prefix_args = arguments.clone();
            let object = prefix_args.as_object_mut().context("cloud arguments must be an object")?;
            object.remove("start_line");
            object.remove("end_line");
            object.insert("byte_offset".into(), json!(0));
            object.insert("byte_limit".into(), json!(byte_limit));
            let remaining = timeout_ms.saturating_sub(started.elapsed().as_millis() as u64);
            anyhow::ensure!(remaining > 0, "signed cloud read deadline exceeded");
            let result = call_route_once(route, tool, prefix_args, limit, remaining).await?;
            bounded_prefix_result(result, &arguments, byte_limit, limit)
        }
        result => result,
    }
}

fn bounded_prefix_result(result: Value, arguments: &Value, byte_limit: usize, limit: usize) -> Result<Value> {
    let mut data = crate::mcp::decode_tool_result::<Value>(result)?;
    anyhow::ensure!(data.get("path") == arguments.get("path")
        && arguments.get("source_host").filter(|v| !v.is_null()).is_none_or(|v| data.get("source_host") == Some(v))
        && data.get("byte_offset").and_then(Value::as_u64) == Some(0), "cloud prefix identity mismatch");
    let encoded = data.get("data_base64").and_then(Value::as_str).context("missing cloud prefix bytes")?;
    anyhow::ensure!(encoded.len() <= byte_limit.div_ceil(3) * 4, "cloud prefix exceeds byte budget");
    let bytes = base64::engine::general_purpose::STANDARD.decode(encoded)?;
    let hash = format!("{:x}", Sha256::digest(&bytes));
    anyhow::ensure!(bytes.len() <= byte_limit
        && data.get("part_sha256").and_then(Value::as_str) == Some(hash.as_str())
        && data.get("next_byte_offset").and_then(Value::as_u64) == Some(bytes.len() as u64),
        "cloud prefix integrity mismatch");
    // The byte endpoint can end within the final UTF-8 codepoint; never invent text.
    let text = match std::str::from_utf8(&bytes) {
        Ok(text) => text,
        Err(error) if error.error_len().is_none() => std::str::from_utf8(&bytes[..error.valid_up_to()])?,
        Err(error) => return Err(error.into()),
    };
    let max_lines = arguments.get("end_line").and_then(Value::as_u64).unwrap_or(200).min(200) as usize;
    let lines: Vec<Value> = text.lines().take(max_lines).enumerate()
        .map(|(i, text)| json!({"line_number":i+1,"text":text})).collect();
    let end_line = lines.len();
    data["start_line"] = json!(1);
    data["end_line"] = json!(end_line);
    data["truncated"] = json!(true);
    data["chunks"] = json!([{"start_line":1,"end_line":end_line,"lines":lines}]);
    data.as_object_mut().context("invalid cloud prefix")?.remove("data_base64");
    let response = json!({"content":[{"type":"text","text":serde_json::to_string(&data)?}],"isError":false});
    anyhow::ensure!(serde_json::to_vec(&response)?.len() <= limit, "signed cloud prefix response exceeds limit");
    Ok(response)
}

async fn call_route_once(route: &Route, tool: &str, mut arguments: Value, limit: usize, timeout_ms: u64) -> Result<Value> {
    let url = reqwest::Url::parse(&route.url).context("invalid signed cloud endpoint")?;
    anyhow::ensure!(url.scheme() == "http" && url.host_str() == Some("127.0.0.1")
        && url.username().is_empty() && url.password().is_none()
        && url.path() == "/mcp" && url.query().is_none() && url.fragment().is_none(),
        "signed cloud endpoint must be an explicit loopback MCP route");
    let args = arguments.as_object_mut().context("cloud arguments must be an object")?;
    let policy = args.remove("filesystem_acl");
    // Only the mesh's already validated read policy may authorize content access.
    // Metadata authenticates the existing relay, without inventing a read policy.
    match tool {
        "search" | "search_text" | "read_text" | "find_paths" => {
            crate::mcp::validate_filesystem_acl_value(policy.as_ref()
                .ok_or_else(|| anyhow!("cloud read requires verified filesystem ACL"))?)?;
        }
        "list_locations" | "search_status" => {}
        _ => return Err(anyhow!("unsupported signed cloud operation")),
    }
    args.remove("_meta");
    args.retain(|_, value| !value.is_null());
    let body = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
        "params":{"name":tool,"arguments":arguments}}))?;
    let client = reqwest::Client::builder().no_proxy().redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_millis(timeout_ms)).build()?;
    let mut request = client.post(url).header("Content-Type", "application/json").body(body.clone());
    let secret = std::env::var(&route.secret_env).context("cloud signer is not configured")?;
    anyhow::ensure!(secret.len() >= 32, "cloud signer is too short");
    if let Some(policy) = policy {
        let policy = serde_json::to_string(&policy)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
        mac.update(&body); mac.update(b"\n"); mac.update(policy.as_bytes());
        let signature = mac.finalize().into_bytes().iter().map(|b| format!("{b:02x}")).collect::<String>();
        request = request.header("X-GrepMesh-Filesystem-Acl", policy)
            .header("X-GrepMesh-Acl-Signature", signature);
    } else {
        // Domain separation prevents this signature from authorizing content.
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
        mac.update(b"grepmesh-metadata-v1\n"); mac.update(&body);
        let signature = mac.finalize().into_bytes().iter().map(|b| format!("{b:02x}")).collect::<String>();
        request = request.header("X-GrepMesh-Metadata-Signature", signature);
    }
    let mut response = request.send().await.map_err(|_| anyhow!("signed cloud transport failed"))?;
    anyhow::ensure!(response.status().is_success(), "signed cloud HTTP status {}", response.status());
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| anyhow!("signed cloud response failed"))? {
        anyhow::ensure!(bytes.len().saturating_add(chunk.len()) <= limit, "signed cloud response exceeds limit");
        bytes.extend_from_slice(&chunk);
    }
    let envelope: Value = serde_json::from_slice(&bytes).context("invalid cloud JSON response")?;
    anyhow::ensure!(envelope.get("error").is_none(), "signed cloud RPC failed");
    envelope.get("result").cloned().context("missing cloud result")
}

#[cfg(test)]
mod tests {
    use super::*;
    fn byte_result(bytes: &[u8]) -> Value {
        let data = json!({"path":"/docs/huge.jsonl", "source_host":"server-100", "byte_offset":0,
            "next_byte_offset":bytes.len(), "part_sha256":format!("{:x}", Sha256::digest(bytes)),
            "data_base64":base64::engine::general_purpose::STANDARD.encode(bytes), "eof":false});
        json!({"content":[{"type":"text","text":data.to_string()}],"isError":false})
    }

    #[test]
    fn oversized_prefix_preserves_verified_source_and_utf8_bounds() {
        let args = json!({"path":"/docs/huge.jsonl","source_host":"server-100","end_line":2});
        let bytes = b"first\nsecond\nthird\xd1";
        let value = bounded_prefix_result(byte_result(bytes), &args, 64, 8192).unwrap();
        let data = crate::mcp::decode_tool_result::<Value>(value).unwrap();
        assert_eq!(data["source_host"], "server-100");
        assert_eq!(data["chunks"][0]["lines"].as_array().unwrap().len(), 2);
        assert_eq!(data["chunks"][0]["lines"][1]["text"], "second");
        assert_eq!(data["truncated"], true);
        assert!(data.get("data_base64").is_none());
    }

    #[test]
    fn oversized_prefix_rejects_wrong_identity_hash_and_budget() {
        let args = json!({"path":"/docs/huge.jsonl","source_host":"server-100"});
        for (key, bad) in [("source_host", json!("server-44")), ("path",json!("/other")),
            ("part_sha256",json!("wrong")), ("next_byte_offset",json!(99))] {
            let mut data = crate::mcp::decode_tool_result::<Value>(byte_result(b"ok")).unwrap();
            data[key] = bad;
            let value = json!({"content":[{"type":"text","text":data.to_string()}],"isError":false});
            assert!(bounded_prefix_result(value, &args, 64, 8192).is_err(), "accepted {key}");
        }
        assert!(bounded_prefix_result(byte_result(b"too long"), &args, 2, 8192).is_err());
    }

    #[tokio::test]
    async fn unsigned_content_and_nonloopback_routes_fail_before_network() {
        let local = Route { url:"http://127.0.0.1:1/mcp".into(), secret_env:"NO_SECRET_FOR_UNSIGNED_TEST".into() };
        for tool in ["search_text", "read_text", "find_paths"] {
            let error = call_route(&local, tool, json!({"path":"/home/allowed"}), 1000, 100).await.unwrap_err();
            assert!(error.to_string().contains("verified filesystem ACL"));
        }
        for url in ["http://example.com/mcp", "http://127.0.0.1/mcp?token=x", "http://user@127.0.0.1/mcp"] {
            let route = Route { url:url.into(), secret_env:"NO_SECRET_FOR_UNSIGNED_TEST".into() };
            assert!(call_route(&route,"search_status",json!({}),1000,100).await.unwrap_err().to_string().contains("loopback"));
        }
    }
}
