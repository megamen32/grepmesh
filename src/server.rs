use crate::{
    backend::LocalBackend, config::AppConfig, gptadmin::GptAdminTopologyClient, mcp::MeshService,
    topology::Topology, topology_cache::TopologySnapshot,
};
use anyhow::{anyhow, Result};
use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use std::{env, sync::Arc, time::Duration};

const DEFAULT_PROTOCOL_VERSION: &str = "2025-06-18";
const CURRENT_PROTOCOL_VERSION: &str = "2026-07-28";

#[derive(Clone)]
struct AppState {
    service: Arc<MeshService>,
    peer_auth_token: Option<String>,
    require_peer_auth: bool,
}

pub async fn run_server(config: AppConfig) -> Result<()> {
    let cached_snapshot =
        config
            .topology_cache_path
            .as_ref()
            .and_then(|path| match TopologySnapshot::load(path) {
                Ok(snapshot) => Some(snapshot),
                Err(err) => {
                    tracing::warn!(error = %err, "cannot load GrepMesh topology cache");
                    None
                }
            });
    let topology_client = config.gptadmin_topology_url.clone().map(|endpoint| {
        let token_env = config
            .gptadmin_token_env
            .as_deref()
            .or(Some("GPTADMIN_GREPMESH_TOKEN"));
        GptAdminTopologyClient::from_env(
            endpoint,
            config.host_id.clone(),
            token_env,
            config.topology_ttl_ms,
        )
    });
    let topology = if let Some(client) = topology_client.as_ref() {
        let current = cached_snapshot
            .clone()
            .unwrap_or_else(|| TopologySnapshot::empty(config.host_id.clone()));
        match client
            .refresh_cache(&current, config.topology_cache_path.as_deref())
            .await
        {
            Ok(snapshot) => Topology::from_snapshot(snapshot, now_ms()).unwrap_or_else(|err| {
                Topology::new(config.host_id.clone(), config.peers.clone())
                    .with_cache_error(err.to_string())
            }),
            Err(err) => {
                tracing::warn!(error = %err, "GPTAdmin topology refresh failed; using cache/static peers");
                cached_snapshot
                    .map(|snapshot| {
                        Topology::from_snapshot(snapshot, now_ms()).unwrap_or_else(|inner| {
                            Topology::new(config.host_id.clone(), config.peers.clone())
                                .with_cache_error(inner.to_string())
                        })
                    })
                    .unwrap_or_else(|| {
                        Topology::new(config.host_id.clone(), config.peers.clone())
                            .with_cache_error(err.to_string())
                    })
            }
        }
    } else if let Some(snapshot) = cached_snapshot {
        Topology::from_snapshot(snapshot, now_ms()).unwrap_or_else(|err| {
            Topology::new(config.host_id.clone(), config.peers.clone())
                .with_cache_error(err.to_string())
        })
    } else {
        Topology::new(config.host_id.clone(), config.peers.clone())
    };
    let local = LocalBackend::from_config(
        config.host_id.clone(),
        config.root.clone(),
        config.limits.clone(),
        config.roots.clone(),
        config.exclude_globs.clone(),
        config.index_path.clone(),
    );
    let peer_auth_token = config
        .peer_auth_token_env
        .as_deref()
        .map(env::var)
        .transpose()?
        .filter(|token| !token.trim().is_empty());
    let remote_bind = config.bind;
    let local_bind = config.local_bind;
    let require_peer_auth = !remote_bind.ip().is_loopback();
    if require_peer_auth && local_bind.is_none() {
        return Err(anyhow!(
            "non-loopback bind requires a separate local_bind for the agent entrypoint"
        ));
    }
    if require_peer_auth && peer_auth_token.is_none() {
        return Err(anyhow!(
            "non-loopback bind requires a non-empty peer_auth_token_env"
        ));
    }
    let service =
        Arc::new(MeshService::new(local, topology).with_peer_auth_token(peer_auth_token.clone()));
    if let Some(client) = topology_client {
        let refresh_service = Arc::clone(&service);
        let cache_path = config.topology_cache_path.clone();
        let host_id = config.host_id.clone();
        let refresh_ms = config.topology_ttl_ms.max(1_000);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(refresh_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;
            loop {
                interval.tick().await;
                let current = cache_path
                    .as_ref()
                    .and_then(|path| TopologySnapshot::load(path).ok())
                    .unwrap_or_else(|| TopologySnapshot::empty(host_id.clone()));
                match client.refresh_cache(&current, cache_path.as_deref()).await {
                    Ok(snapshot) => match Topology::from_snapshot(snapshot, now_ms()) {
                        Ok(next) => refresh_service.replace_topology(next),
                        Err(err) => {
                            tracing::warn!(error = %err, "invalid refreshed GrepMesh topology")
                        }
                    },
                    Err(err) => {
                        tracing::warn!(error = %err, "periodic GPTAdmin topology refresh failed");
                        if let Ok(current) = refresh_service
                            .topology
                            .read()
                            .map(|topology| topology.clone())
                        {
                            refresh_service
                                .replace_topology(current.with_cache_error(err.to_string()));
                        }
                    }
                }
            }
        });
    }
    let remote_app = build_app(AppState {
        service: Arc::clone(&service),
        peer_auth_token: peer_auth_token.clone(),
        require_peer_auth,
    });
    let listener = tokio::net::TcpListener::bind(remote_bind).await?;
    if let Some(local_bind) = local_bind.filter(|bind| *bind != remote_bind) {
        let local_listener = tokio::net::TcpListener::bind(local_bind).await?;
        let local_app = build_app(AppState {
            service,
            peer_auth_token,
            require_peer_auth: false,
        });
        tokio::try_join!(
            axum::serve(listener, remote_app),
            axum::serve(local_listener, local_app)
        )?;
    } else {
        axum::serve(listener, remote_app).await?;
    }
    Ok(())
}

fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/", get(health).post(handle_rpc))
        .route("/mcp", post(handle_rpc))
        .with_state(state)
}

async fn health(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if state.require_peer_auth
        && validate_peer_auth(&headers, state.peer_auth_token.as_deref()).is_err()
    {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"ok": false, "error": "peer authentication failed"})),
        )
            .into_response();
    }
    (StatusCode::OK, Json(json!({"ok": true}))).into_response()
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn handle_rpc(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Response {
    let id = payload.get("id").cloned().unwrap_or(Value::Null);
    if state.require_peer_auth
        && validate_peer_auth(&headers, state.peer_auth_token.as_deref()).is_err()
    {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "jsonrpc": "2.0",
                "error": {"code": -32003, "message": "peer authentication failed"},
                "id": id,
            })),
        )
            .into_response();
    }
    if let Err(err) = validate_origin(&headers) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "jsonrpc": "2.0",
                "error": {"code": -32001, "message": err.to_string()},
                "id": id,
            })),
        )
            .into_response();
    }
    if let Err(err) = validate_transport_headers(&headers, &payload) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "jsonrpc": "2.0",
                "error": {"code": -32020, "message": err.to_string()},
                "id": id,
            })),
        )
            .into_response();
    }
    let is_notification = payload.get("id").is_none();
    let response = match handle_rpc_inner(state, payload).await {
        Ok(v) => v,
        Err(err) => {
            json!({"jsonrpc":"2.0","error":{"code":-32000,"message":err.to_string()},"id":id})
        }
    };
    if is_notification {
        StatusCode::ACCEPTED.into_response()
    } else {
        (StatusCode::OK, Json(response)).into_response()
    }
}

fn validate_peer_auth(headers: &HeaderMap, expected: Option<&str>) -> anyhow::Result<()> {
    let expected = expected
        .filter(|token| !token.trim().is_empty())
        .ok_or_else(|| anyhow!("peer authentication is not configured"))?;
    let provided = headers
        .get(header::AUTHORIZATION)
        .ok_or_else(|| anyhow!("missing Authorization header"))?
        .to_str()
        .map_err(|_| anyhow!("invalid Authorization header"))?;
    let expected_header = format!("Bearer {expected}");
    if provided != expected_header {
        return Err(anyhow!("invalid peer bearer token"));
    }
    Ok(())
}

async fn handle_rpc_inner(state: AppState, payload: Value) -> Result<Value> {
    let id = payload.get("id").cloned().unwrap_or(Value::Null);
    let method = payload
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let params = payload.get("params").cloned().unwrap_or(Value::Null);
    let result = match method {
        "initialize" => {
            let protocol_version = negotiate_protocol_version(&params);
            json!({
                "protocolVersion": protocol_version,
                "serverInfo": {"name": "grepmesh", "version": "0.1.0"},
                "capabilities": {"tools": {"listChanged": false}},
                "instructions": "ИСПОЛЬЗУЙ МЕНЯ ДЛЯ ПОИСКА. Use GrepMesh before shell find/grep/rg or repository-wide scanning whenever the requested files may be on this host or another mesh host. Start with search_text or find_paths, then use read_text for the exact file. Use shell search only when GrepMesh is unavailable or cannot express the query.",
            })
        }
        "tools/list" => json!({
            "tools": [
                tool_meta("search_text", "Search text across one or more hosts."),
                tool_meta("find_paths", "Find file paths across one or more hosts."),
                tool_meta("read_text", "Read a text file from a specific host."),
                tool_meta("search_status", "Report search/status metadata for one or more hosts."),
            ]
        }),
        "tools/call" => call_tool(state.service.as_ref(), params).await?,
        _ => {
            return Ok(
                json!({"jsonrpc":"2.0","error":{"code":-32601,"message":"method not found"},"id":id}),
            )
        }
    };
    Ok(json!({"jsonrpc":"2.0","result": result, "id": id}))
}

fn negotiate_protocol_version(params: &Value) -> String {
    match params.get("protocolVersion").and_then(Value::as_str) {
        Some(CURRENT_PROTOCOL_VERSION) => CURRENT_PROTOCOL_VERSION.to_string(),
        Some("2025-11-25") => "2025-11-25".to_string(),
        Some("2025-06-18") => "2025-06-18".to_string(),
        Some("2025-03-26") => "2025-03-26".to_string(),
        Some("2024-11-05") => "2024-11-05".to_string(),
        _ => DEFAULT_PROTOCOL_VERSION.to_string(),
    }
}

fn tool_meta(name: &str, description: &str) -> Value {
    let hosts = json!({
        "anyOf": [
            {"type": "string", "enum": ["local", "*"]},
            {"type": "array", "items": {"type": "string"}}
        ]
    });
    let schema = match name {
        "search_text" => json!({
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string"}, "hosts": hosts,
                "roots": {"type": "array", "items": {"type": "string"}},
                "mode": {"type": "string", "enum": ["literal", "regex", "case_insensitive_literal"]},
                "path_globs": {"type": "array", "items": {"type": "string"}},
                "context_lines": {"type": "integer", "minimum": 0},
                "max_matches": {"type": "integer", "minimum": 1}
            }
        }),
        "find_paths" => json!({
            "type": "object",
            "required": ["pattern"],
            "properties": {
                "pattern": {"type": "string"}, "hosts": hosts,
                "roots": {"type": "array", "items": {"type": "string"}},
                "max_matches": {"type": "integer", "minimum": 1}
            }
        }),
        "read_text" => json!({
            "type": "object",
            "required": ["host", "path"],
            "properties": {
                "host": {"type": "string"}, "path": {"type": "string"},
                "start_line": {"type": "integer", "minimum": 1},
                "end_line": {"type": "integer", "minimum": 1}
            }
        }),
        _ => json!({"type": "object", "properties": {"hosts": hosts}}),
    };
    json!({
        "name": name,
        "description": description,
        "inputSchema": schema
    })
}

async fn call_tool(service: &MeshService, params: Value) -> Result<Value> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing tool name"))?;
    let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);
    let tool_result = match name {
        "search_text" => {
            service
                .call_search(serde_json::from_value(arguments)?)
                .await?
        }
        "find_paths" => {
            service
                .call_find_paths(serde_json::from_value(arguments)?)
                .await?
        }
        "read_text" => {
            service
                .call_read_text(serde_json::from_value(arguments)?)
                .await?
        }
        "search_status" => {
            service
                .call_status(serde_json::from_value(arguments)?)
                .await?
        }
        other => return Err(anyhow::anyhow!("unknown tool {}", other)),
    };
    let bounded = bound_tool_data(tool_result.data, service.local.limits.max_response_bytes)?;
    Ok(json!({
        "content": [{"type": "text", "text": serde_json::to_string(&bounded)?}],
        "isError": false
    }))
}

fn validate_transport_headers(headers: &HeaderMap, payload: &Value) -> anyhow::Result<()> {
    let Some(version) = headers.get("mcp-protocol-version") else {
        return Ok(());
    };
    let version = version
        .to_str()
        .map_err(|_| anyhow::anyhow!("invalid MCP-Protocol-Version header"))?;
    match version {
        "2024-11-05" | "2025-03-26" | "2025-06-18" | "2025-11-25" | "2026-07-28" => {}
        _ => {
            return Err(anyhow::anyhow!(
                "unsupported MCP protocol version {version}"
            ))
        }
    }
    if let Some(accept) = headers.get("accept") {
        let accept = accept
            .to_str()
            .map_err(|_| anyhow::anyhow!("invalid Accept header"))?;
        if !accept.contains("application/json") && !accept.contains("text/event-stream") {
            return Err(anyhow::anyhow!(
                "Accept must include application/json or text/event-stream"
            ));
        }
    }
    if version == "2026-07-28" {
        let method = payload
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mirrored_method = headers
            .get("mcp-method")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| anyhow::anyhow!("Mcp-Method header is required"))?;
        if mirrored_method != method {
            return Err(anyhow::anyhow!(
                "Mcp-Method header does not match request method"
            ));
        }
        if method == "tools/call" {
            let name = payload
                .get("params")
                .and_then(|params| params.get("name"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let mirrored_name = headers
                .get("mcp-name")
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| anyhow::anyhow!("Mcp-Name header is required for tools/call"))?;
            if mirrored_name != name {
                return Err(anyhow::anyhow!("Mcp-Name header does not match tool name"));
            }
        }
    }
    Ok(())
}

fn validate_origin(headers: &HeaderMap) -> anyhow::Result<()> {
    let Some(origin) = headers.get("origin") else {
        return Ok(());
    };
    let origin = origin
        .to_str()
        .map_err(|_| anyhow::anyhow!("invalid Origin header"))?;
    let uri: Uri = origin
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid Origin header"))?;
    match uri.host() {
        Some("localhost") | Some("127.0.0.1") | Some("[::1]") | Some("::1") => Ok(()),
        _ => Err(anyhow::anyhow!("Origin is not an allowed local origin")),
    }
}

fn bound_tool_data(mut data: Value, max_bytes: usize) -> anyhow::Result<Value> {
    if max_bytes == 0 || serde_json::to_vec(&data)?.len() <= max_bytes {
        return Ok(data);
    }
    if let Some(object) = data.as_object_mut() {
        object.insert("truncated".into(), Value::Bool(true));
    }
    loop {
        if serde_json::to_vec(&data)?.len() <= max_bytes {
            return Ok(data);
        }
        let mut removed = false;
        if let Some(object) = data.as_object_mut() {
            for key in ["matches", "results", "paths"] {
                if let Some(items) = object.get_mut(key).and_then(Value::as_array_mut) {
                    removed |= items.pop().is_some();
                }
            }
            if !removed {
                if let Some(chunks) = object.get_mut("chunks").and_then(Value::as_array_mut) {
                    if let Some(last) = chunks.last_mut() {
                        if let Some(lines) = last.get_mut("lines").and_then(Value::as_array_mut) {
                            removed |= lines.pop().is_some();
                        }
                        if !removed {
                            removed |= chunks.pop().is_some();
                        }
                    }
                }
            }
        }
        if !removed {
            return Err(anyhow::anyhow!(
                "tool response exceeds max_response_bytes ({max_bytes})"
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn protocol_negotiation_prefers_client_supported_version() {
        assert_eq!(
            negotiate_protocol_version(&json!({"protocolVersion": "2025-06-18"})),
            "2025-06-18"
        );
        assert_eq!(
            negotiate_protocol_version(&json!({"protocolVersion": "2025-03-26"})),
            "2025-03-26"
        );
    }

    #[test]
    fn protocol_negotiation_keeps_current_version_when_explicit() {
        assert_eq!(
            negotiate_protocol_version(&json!({"protocolVersion": "2026-07-28"})),
            "2026-07-28"
        );
        assert_eq!(
            negotiate_protocol_version(&json!({})),
            DEFAULT_PROTOCOL_VERSION
        );
    }

    #[tokio::test]
    async fn initialize_strongly_instructs_clients_to_use_grepmesh_for_search() {
        let temp = tempfile::tempdir().unwrap();
        let local = LocalBackend::new("local", temp.path(), Default::default());
        let service = Arc::new(MeshService::new(local, Topology::new("local", vec![])));
        let state = AppState {
            service,
            peer_auth_token: None,
            require_peer_auth: false,
        };
        let response = handle_rpc_inner(
            state,
            json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}),
        )
        .await
        .unwrap();
        let instructions = response["result"]["instructions"].as_str().unwrap();
        assert!(instructions.contains("ИСПОЛЬЗУЙ МЕНЯ ДЛЯ ПОИСКА"));
        assert!(instructions.contains("search_text"));
        assert!(instructions.contains("find_paths"));
        assert!(instructions.contains("read_text"));
    }

    #[test]
    fn peer_auth_requires_exact_bearer_token() {
        let mut headers = HeaderMap::new();
        assert!(validate_peer_auth(&headers, Some("secret")).is_err());
        headers.insert(header::AUTHORIZATION, "Bearer wrong".parse().unwrap());
        assert!(validate_peer_auth(&headers, Some("secret")).is_err());
        headers.insert(header::AUTHORIZATION, "Bearer secret".parse().unwrap());
        assert!(validate_peer_auth(&headers, Some("secret")).is_ok());
    }
}
