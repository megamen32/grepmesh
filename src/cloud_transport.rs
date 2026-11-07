//! Explicit server-owned adapters for the existing private cloud ACL wire contract.
use anyhow::{anyhow, Context, Result};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;
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

async fn call_route(route: &Route, tool: &str, mut arguments: Value, limit: usize, timeout_ms: u64) -> Result<Value> {
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
