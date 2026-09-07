use crate::topology_cache::{ProviderSnapshot, TopologyNode, TopologySnapshot};
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    env,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone)]
pub struct GptAdminTopologyClient {
    endpoint: String,
    token: Option<String>,
    local_host_id: String,
    ttl_ms: u64,
    client: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct TopologyResponse {
    nodes: Vec<TopologyNodeWire>,
}

#[derive(Debug, Deserialize)]
struct TopologyNodeWire {
    host_id: String,
    #[serde(default)]
    local_url: Option<String>,
    #[serde(alias = "advertise_url", alias = "routable_url", alias = "endpoint")]
    #[serde(default)]
    peer_url: Option<String>,
    #[serde(default)]
    capabilities: Vec<String>,
    #[serde(default)]
    roots: Vec<String>,
    #[serde(default)]
    generation: u64,
}

impl GptAdminTopologyClient {
    pub fn from_env(
        endpoint: impl Into<String>,
        local_host_id: impl Into<String>,
        token_env: Option<&str>,
        ttl_ms: u64,
    ) -> Self {
        let token = token_env
            .and_then(|name| env::var(name).ok())
            .filter(|value| !value.trim().is_empty());
        Self {
            endpoint: endpoint.into(),
            token,
            local_host_id: local_host_id.into(),
            ttl_ms,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(10))
                .build()
                .expect("build GPTAdmin client"),
        }
    }

    pub async fn fetch(&self) -> Result<ProviderSnapshot> {
        let mut request = self.client.get(&self.endpoint);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let body = request
            .send()
            .await
            .with_context(|| format!("fetch GPTAdmin topology {}", self.endpoint))?
            .error_for_status()
            .context("GPTAdmin topology returned an error")?
            .json::<Value>()
            .await
            .context("decode GPTAdmin topology")?;
        if body.get("agents").is_some() {
            return self.from_registry(body).await;
        }
        let projection: TopologyResponse = serde_json::from_value(body)?;
        let mut projected =
            parse_topology_response(projection, &self.local_host_id, now_ms(), self.ttl_ms)?;
        // Older Hub projections do not include dynamically exposed child MCPs.
        // Reuse the Hub's real registry instead of registering a second fleet.
        let mut registry = reqwest::Url::parse(&self.endpoint)?;
        registry.set_path("/mcp-relay/agents");
        registry.set_query(None);
        let mut request = self.client.get(registry);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let discovered: Result<ProviderSnapshot> = async {
            let body = request
                .send()
                .await?
                .error_for_status()?
                .json::<Value>()
                .await?;
            self.from_registry(body).await
        }
        .await;
        match discovered {
            Ok(registry) => {
                for peer in registry.peers {
                    if let Some(known) = projected
                        .peers
                        .iter_mut()
                        .find(|known| known.host_id == peer.host_id)
                    {
                        // The existing projection knows a direct address; the
                        // child registry adds a relay without replacing it.
                        known.gptadmin_relay_url = peer.gptadmin_relay_url;
                    } else {
                        projected.peers.push(peer);
                    }
                }
                projected.validate()?;
                Ok(projected)
            }
            Err(_) if !projected.peers.is_empty() => Ok(projected),
            Err(error) => Err(error),
        }
    }

    fn relay_url(&self, path: &str) -> Result<String> {
        if !path.starts_with("/server/")
            || !path.ends_with("/mcp")
            || path.contains(['?', '#', '\\'])
            || path.contains("..")
        {
            return Err(anyhow!("invalid GPTAdmin child MCP path"));
        }
        let mut url = reqwest::Url::parse(&self.endpoint)?;
        if !url.username().is_empty() || url.password().is_some() {
            return Err(anyhow!("GPTAdmin credentials must come from token_env"));
        }
        url.set_path(path);
        url.set_query(None);
        url.set_fragment(None);
        Ok(url.to_string())
    }

    pub async fn call_relay(&self, url: &str, tool: &str, arguments: Value) -> Result<Value> {
        let parsed = reqwest::Url::parse(url)?;
        if self.relay_url(parsed.path())? != url {
            return Err(anyhow!("GPTAdmin relay must use the configured Hub origin"));
        }
        let mut request = self.client.post(url).json(&json!({
            "jsonrpc":"2.0", "id":1, "method":"tools/call",
            "params":{"name":tool,"arguments":arguments}
        }));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let body: Value = request.send().await?.error_for_status()?.json().await?;
        if body.get("error").is_some()
            || body.pointer("/result/error").is_some()
            || body.pointer("/result/isError").and_then(Value::as_bool) == Some(true)
        {
            return Err(anyhow!("GPTAdmin child MCP call failed"));
        }
        body.get("result")
            .cloned()
            .ok_or_else(|| anyhow!("missing GPTAdmin MCP result"))
    }

    async fn from_registry(&self, body: Value) -> Result<ProviderSnapshot> {
        let agents = body
            .get("agents")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("missing GPTAdmin agents registry"))?;
        let fetched_at_ms = now_ms();
        let mut peers = BTreeMap::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        for agent in agents.iter().take(256) {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            if agent.get("kind").and_then(Value::as_str) != Some("child_mcp")
                || !agent
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .eq_ignore_ascii_case("grepmesh")
                || agent.get("status").and_then(Value::as_str) != Some("online")
            {
                continue;
            }
            let Some(path) = agent
                .pointer("/meta/public_mcp_path")
                .and_then(Value::as_str)
            else {
                continue;
            };
            let Ok(url) = self.relay_url(path) else {
                continue;
            };
            // A forwarded request must stay local even if that peer knows a mesh.
            let probe_deadline = deadline.min(tokio::time::Instant::now() + Duration::from_secs(3));
            let Ok(Ok(result)) = tokio::time::timeout_at(
                probe_deadline,
                self.call_relay(
                    &url,
                    "list_locations",
                    json!({"hosts":"local", "hop_count":1}),
                ),
            )
            .await
            else {
                continue;
            };
            let payload = result.get("structuredContent").cloned().or_else(|| {
                result.get("content")?.as_array()?.iter().find_map(|item| {
                    serde_json::from_str::<Value>(item.get("text")?.as_str()?).ok()
                })
            });
            let Some(payload) = payload else {
                continue;
            };
            let Some(host) = payload.get("host_id").and_then(Value::as_str) else {
                continue;
            };
            if host == self.local_host_id {
                continue;
            }
            peers.entry(host.to_string()).or_insert(TopologyNode {
                host_id: host.to_string(),
                local_url: url.clone(),
                routable_url: url.clone(),
                gptadmin_relay_url: Some(url),
                capabilities: vec![],
                roots: vec![],
                generation: 1,
                fetched_at_ms,
                expires_at_ms: fetched_at_ms.saturating_add(self.ttl_ms),
                last_refresh_error: None,
            });
        }
        if peers.is_empty() {
            return Err(anyhow!(
                "GPTAdmin registry returned no reachable remote GrepMesh children"
            ));
        }
        let result = ProviderSnapshot {
            local_host_id: self.local_host_id.clone(),
            generation: 1,
            fetched_at_ms,
            ttl_ms: self.ttl_ms,
            peers: peers.into_values().collect(),
        };
        result.validate()?;
        Ok(result)
    }

    pub async fn refresh_cache(
        &self,
        current: &TopologySnapshot,
        cache_path: Option<&Path>,
    ) -> Result<TopologySnapshot> {
        let mut provider = self.fetch().await?;
        // Registry probes are partial. Preserve missing cached hosts across
        // restarts without renewing their old freshness timestamps.
        for previous in current
            .peers
            .iter()
            .filter(|peer| peer.gptadmin_relay_url.is_some())
        {
            if let Some(peer) = provider
                .peers
                .iter_mut()
                .find(|peer| peer.host_id == previous.host_id)
            {
                if peer.gptadmin_relay_url.is_none() {
                    peer.gptadmin_relay_url = previous.gptadmin_relay_url.clone();
                }
            } else {
                provider.peers.push(previous.clone());
            }
        }
        let merged = current.merge_fresh_provider_result(provider)?;
        if let Some(path) = cache_path {
            merged.save_atomic(path)?;
        }
        Ok(merged)
    }
}

fn parse_topology_response(
    response: TopologyResponse,
    local_host_id: &str,
    fetched_at_ms: u64,
    ttl_ms: u64,
) -> Result<ProviderSnapshot> {
    let generation = response
        .nodes
        .iter()
        .map(|node| node.generation)
        .max()
        .unwrap_or(1)
        .max(1);
    let peers = response
        .nodes
        .into_iter()
        .filter(|node| node.host_id != local_host_id)
        .map(|node| -> Result<TopologyNode> {
            Ok(TopologyNode {
                host_id: node.host_id,
                gptadmin_relay_url: None,
                local_url: node
                    .local_url
                    .clone()
                    .or_else(|| node.peer_url.clone())
                    .ok_or_else(|| anyhow!("topology node has no local_url or peer_url"))?,
                routable_url: node
                    .peer_url
                    .or_else(|| node.local_url.clone())
                    .ok_or_else(|| anyhow!("topology node has no peer_url or local_url"))?,
                capabilities: node.capabilities,
                roots: node.roots,
                generation: node.generation.max(generation),
                fetched_at_ms,
                expires_at_ms: fetched_at_ms.saturating_add(ttl_ms),
                last_refresh_error: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let result = ProviderSnapshot {
        local_host_id: local_host_id.to_string(),
        generation,
        fetched_at_ms,
        ttl_ms,
        peers,
    };
    result.validate()?;
    Ok(result)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn registry_outage_preserves_cached_relay_routes_across_restart() {
        use axum::{http::StatusCode, routing::get, Json};
        let app = axum::Router::new()
            .route(
                "/mcp-relay/grepmesh",
                get(|| async {
                    Json(json!({"nodes":[
                        {"host_id":"B","peer_url":"http://direct-b/mcp","generation":9}
                    ]}))
                }),
            )
            .route(
                "/mcp-relay/agents",
                get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut cached = parse_topology_response(
            serde_json::from_value(json!({"nodes":[
                {"host_id":"B","peer_url":"http://old-b/mcp"},
                {"host_id":"C","peer_url":"http://old-c/mcp"}
            ]}))
            .unwrap(),
            "A",
            1,
            1,
        )
        .unwrap();
        for node in &mut cached.peers {
            node.gptadmin_relay_url = Some(format!("http://{address}/server/{}/mcp", node.host_id));
        }
        let folder = tempfile::tempdir_in(".tmp").unwrap();
        let path = folder.path().join("topology.json");
        TopologySnapshot::empty("A")
            .merge_fresh_provider_result(cached)
            .unwrap()
            .save_atomic(&path)
            .unwrap();
        let before = TopologySnapshot::load(&path).unwrap();
        let client = GptAdminTopologyClient::from_env(
            format!("http://{address}/mcp-relay/grepmesh"),
            "A",
            None,
            30_000,
        );
        client.refresh_cache(&before, Some(&path)).await.unwrap();
        let after = TopologySnapshot::load(&path).unwrap();
        assert_eq!(after.peers.len(), 2);
        assert_eq!(after.peer("B").unwrap().routable_url, "http://direct-b/mcp");
        assert_eq!(
            after.peer("B").unwrap().gptadmin_relay_url,
            before.peer("B").unwrap().gptadmin_relay_url
        );
        assert_eq!(after.peer("C").unwrap(), before.peer("C").unwrap());
        assert_eq!(after.peer("C").unwrap().expires_at_ms, 2);
        let known = crate::topology::Topology::from_snapshot(before, now_ms()).unwrap();
        let mut runtime = crate::topology::Topology::from_snapshot(after, now_ms()).unwrap();
        runtime
            .peers
            .iter_mut()
            .find(|p| p.host_id == "B")
            .unwrap()
            .gptadmin_relay_url = None;
        runtime.retain_known_peers(&known.peers);
        assert_eq!(
            runtime.peer("B").unwrap().gptadmin_relay_url,
            known.peer("B").unwrap().gptadmin_relay_url
        );
    }

    #[tokio::test]
    async fn mixed_projection_and_registry_preserve_direct_and_add_child_hosts() {
        use axum::{
            routing::{get, post},
            Json,
        };
        let app = axum::Router::new()
            .route("/mcp-relay/grepmesh", get(|| async { Json(json!({"nodes":[
                {"host_id":"B","peer_url":"http://direct-b/mcp","generation":9},
                {"host_id":"C","peer_url":"http://direct-c/mcp","generation":9}
            ]})) }))
            .route("/mcp-relay/agents", get(|| async { Json(json!({"agents":[
                {"name":"grepmesh","kind":"child_mcp","status":"online","meta":{"public_mcp_path":"/server/B/mcp"}},
                {"name":"grepmesh","kind":"child_mcp","status":"online","meta":{"public_mcp_path":"/server/D/mcp"}}
            ]})) }))
            .route("/server/:child/mcp", post(|axum::extract::Path(child): axum::extract::Path<String>| async move {
                Json(json!({"result":{"structuredContent":{"host_id":child}}}))
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = GptAdminTopologyClient::from_env(
            format!("http://{address}/mcp-relay/grepmesh"),
            "A",
            None,
            30_000,
        );
        let result = client.fetch().await.unwrap();
        assert_eq!(result.peers.len(), 3);
        let b = result.peers.iter().find(|p| p.host_id == "B").unwrap();
        assert_eq!(b.routable_url, "http://direct-b/mcp");
        assert_eq!(
            b.gptadmin_relay_url,
            Some(format!("http://{address}/server/B/mcp"))
        );
        assert!(result.peers.iter().any(|p| p.host_id == "D"));
    }

    #[tokio::test]
    async fn actual_registry_children_are_probed_locally_deduplicated_and_authenticated() {
        use axum::{
            http::HeaderMap,
            routing::{get, post},
            Json,
        };
        let app = axum::Router::new()
            .route("/mcp-relay/grepmesh", get(|| async { Json(json!({"nodes":[]})) }))
            .route("/mcp-relay/agents", get(|headers: HeaderMap| async move {
                assert_eq!(headers["authorization"], "Bearer hub-only");
                Json(json!({"agents":[
                    {"name":"grepmesh","kind":"child_mcp","status":"online","meta":{"public_mcp_path":"/server/b/mcp","public_mcp_endpoint":"https://untrusted.example/mcp"}},
                    {"name":"grepmesh","kind":"child_mcp","status":"online","meta":{"public_mcp_path":"/server/b-alias/mcp"}},
                    {"name":"grepmesh","kind":"child_mcp","status":"online","meta":{"public_mcp_path":"/server/self/mcp"}},
                    {"name":"grepmesh","kind":"child_mcp","status":"stale","meta":{"public_mcp_path":"/server/offline/mcp"}},
                    {"name":"grepmesh","kind":"child_mcp","status":"online","meta":{"public_mcp_path":"https://untrusted.example/mcp"}},
                    {"name":"grepmesh","kind":"child_mcp","status":"online","meta":{"public_mcp_path":"/server/failed/mcp"}}
                ]}))
            }))
            .route("/server/:child/mcp", post(|axum::extract::Path(child): axum::extract::Path<String>, headers: HeaderMap, Json(body): Json<Value>| async move {
                assert_eq!(headers["authorization"], "Bearer hub-only");
                assert_eq!(body.pointer("/params/arguments/hosts"), Some(&json!("local")));
                assert_eq!(body.pointer("/params/arguments/hop_count"), Some(&json!(1)));
                assert_ne!(child, "offline");
                if child == "failed" { return Json(json!({"result":{"error":"child unavailable"}})); }
                let host = if child == "self" { "A" } else { "B" };
                Json(json!({"result":{"isError":false,"content":[{"type":"text","text":json!({"host_id":host}).to_string()}]}}))
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut client = GptAdminTopologyClient::from_env(
            format!("http://{address}/mcp-relay/grepmesh"),
            "A",
            None,
            30_000,
        );
        client.token = Some("hub-only".into());
        let result = client.fetch().await.unwrap();
        assert_eq!(result.peers.len(), 1);
        assert_eq!(result.peers[0].host_id, "B");
        assert_eq!(
            result.peers[0].gptadmin_relay_url,
            Some(format!("http://{address}/server/b/mcp"))
        );
        let snapshot = TopologySnapshot::empty("A")
            .merge_fresh_provider_result(result)
            .unwrap();
        assert!(!serde_json::to_string(&snapshot)
            .unwrap()
            .contains("hub-only"));
        let mut partial_cache = snapshot.clone();
        let mut missing = partial_cache.peers[0].clone();
        missing.host_id = "previously-discovered".into();
        missing.fetched_at_ms = 1;
        missing.expires_at_ms = 2;
        partial_cache.peers.push(missing);
        let refreshed = client.refresh_cache(&partial_cache, None).await.unwrap();
        assert_eq!(
            refreshed
                .peer("previously-discovered")
                .unwrap()
                .expires_at_ms,
            2
        );
        assert_eq!(refreshed.peers.len(), 2);
        assert!(client
            .call_relay(
                "https://untrusted.example/server/b/mcp",
                "list_locations",
                json!({})
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn empty_registry_does_not_erase_cache_and_redirects_are_not_followed() {
        use axum::routing::get;
        let app = axum::Router::new()
            .route(
                "/mcp-relay/agents",
                get(|| async { axum::Json(json!({"agents":[]})) }),
            )
            .route(
                "/redirect",
                get(|| async { axum::response::Redirect::temporary("/mcp-relay/agents") }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let current = TopologySnapshot::empty("A");
        let client = GptAdminTopologyClient::from_env(
            format!("http://{address}/mcp-relay/agents"),
            "A",
            None,
            30_000,
        );
        assert!(client.refresh_cache(&current, None).await.is_err());
        let redirect = GptAdminTopologyClient::from_env(
            format!("http://{address}/redirect"),
            "A",
            None,
            30_000,
        );
        assert!(redirect.fetch().await.is_err());
    }

    #[test]
    fn topology_response_excludes_local_and_preserves_peer_url() {
        let response: TopologyResponse = serde_json::from_value(json!({
            "nodes": [
                {"host_id":"A","local_url":"http://127.0.0.1:9419/mcp","peer_url":"https://192.0.2.10:9419/mcp","generation":4},
                {"host_id":"B","local_url":"http://127.0.0.1:9419/mcp","advertise_url":"https://192.0.2.11:9419/mcp","generation":5,"roots":["home"]}
            ]
        })).unwrap();
        let result = parse_topology_response(response, "A", 1_700_000_000_000, 30_000).unwrap();
        assert_eq!(result.peers.len(), 1);
        assert_eq!(result.peers[0].host_id, "B");
        assert_eq!(result.peers[0].routable_url, "https://192.0.2.11:9419/mcp");
    }

    #[test]
    fn endpoint_only_discovery_record_is_usable_for_both_urls() {
        let response: TopologyResponse = serde_json::from_value(json!({
            "nodes": [{
                "host_id": "B",
                "endpoint": "https://192.0.2.11:9419/mcp",
                "generation": 5,
                "capabilities": ["search_text"],
                "roots": ["home"]
            }]
        }))
        .unwrap();
        let result = parse_topology_response(response, "A", 1_700_000_000_000, 30_000).unwrap();
        assert_eq!(result.peers[0].local_url, result.peers[0].routable_url);
        assert_eq!(result.peers[0].routable_url, "https://192.0.2.11:9419/mcp");
    }

    #[tokio::test]
    async fn unavailable_registry_preserves_valid_control_plane_projection() {
        let app = axum::Router::new().route(
            "/mcp-relay/grepmesh",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({
                    "nodes": [{
                        "host_id": "B",
                        "local_url": "http://127.0.0.1:9419/mcp",
                        "peer_url": "http://127.0.0.1:29419/mcp",
                        "generation": 9
                    }]
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = GptAdminTopologyClient::from_env(
            format!("http://{address}/mcp-relay/grepmesh"),
            "A",
            None,
            30_000,
        );
        let result = client.fetch().await.unwrap();
        assert_eq!(result.peers[0].host_id, "B");
        assert_eq!(result.peers[0].routable_url, "http://127.0.0.1:29419/mcp");
    }
}
