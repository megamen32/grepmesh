use crate::{
    backend::LocalBackend,
    compute::{ComputeWorker, OcrExtractRequest},
    config::AppConfig,
    gptadmin::GptAdminTopologyClient,
    jobs::SearchJobs,
    mcp::{compact_search_response_data, MeshService},
    topology::{PeerConfig, Topology},
    topology_cache::TopologySnapshot,
};
use anyhow::{anyhow, Result};
use axum::{
    extract::{DefaultBodyLimit, Request, State},
    http::{header, HeaderMap, StatusCode, Uri},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use std::{env, fs, sync::Arc, time::Duration};

const DEFAULT_PROTOCOL_VERSION: &str = "2025-06-18";
const CURRENT_PROTOCOL_VERSION: &str = "2026-07-28";
const DEFAULT_FOREGROUND_SEARCH_WAIT_MS: u64 = 500;

#[derive(Clone)]
struct AppState {
    service: Arc<MeshService>,
    compute: Arc<ComputeWorker>,
    jobs: SearchJobs,
    peer_auth_token: Option<String>,
    remote_index_auth_token: Option<String>,
    require_peer_auth: bool,
}

pub async fn run_server(mut config: AppConfig) -> Result<()> {
    let peer_auth_token = config
        .peer_auth_token_env
        .as_deref()
        .map(env::var)
        .transpose()?
        .filter(|token| !token.trim().is_empty());
    let remote_index_auth_token = config
        .remote_index_auth_token_env
        .as_deref()
        .map(env::var)
        .transpose()?
        .filter(|token| !token.trim().is_empty());
    let remote_index_configured =
        config.remote_index_source.is_some() || config.remote_index_store.is_some();
    if remote_index_configured && remote_index_auth_token.is_none() {
        return Err(anyhow!(
            "remote index is configured but its bearer token is unavailable"
        ));
    }
    if remote_index_configured && !config.bind.ip().is_loopback() && peer_auth_token.is_none() {
        return Err(anyhow!(
            "network-bound remote index MCP requires a configured peer bearer token"
        ));
    }
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
    let mut topology = if let Some(client) = topology_client.as_ref() {
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
    topology.retain_known_peers(&config.peers);
    // OCR discovery follows the same protected live/cache routes as search;
    // never force indexing clients to construct a local CPU session for mesh.
    config.ocr.mesh_peers = crate::compute_client::merge_compute_peers(topology.clone(), &config.ocr.mesh_peers);
    if config.compute.enabled {
        let local_compute_address = config.local_bind.unwrap_or_else(|| {
            if config.bind.ip().is_unspecified() {
                std::net::SocketAddr::from(([127, 0, 0, 1], config.bind.port()))
            } else {
                config.bind
            }
        });
        let local_compute_url = format!("http://{local_compute_address}/mcp");
        config.ocr.mesh_peers.push(PeerConfig {
            host_id: config.host_id.clone(),
            local_url: local_compute_url.clone(),
            routable_url: local_compute_url,
            gptadmin_proxy_url: None,
            gptadmin_relay_url: None,
        });
    }
    config.ocr.mesh_topology_cache_path = config.topology_cache_path.clone();
    config.ocr.mesh_peer_token_env = config.peer_auth_token_env.clone();
    config.ocr.mesh_relay_token_env = config.gptadmin_token_env.clone();
    config.ocr.mesh_relay_origin = config.gptadmin_topology_url.clone();
    let compute = Arc::new(ComputeWorker::new(
        config.host_id.clone(),
        config.compute.clone(),
        config.ocr.clone(),
        config.limits.max_response_bytes,
    ));
    if config.userio.enabled {
        // The cache root must exist before the index watcher registers its
        // roots: notify refuses to watch a missing directory and the adapter
        // would then populate an unindexed cache until the next full rebuild.
        for dir in ["conversations", "attachments"] {
            if let Err(error) = fs::create_dir_all(config.userio.cache_dir.join(dir)) {
                tracing::warn!(
                    dir = %config.userio.cache_dir.join(dir).display(),
                    error = %error,
                    "cannot prepare userio cache directory"
                );
            }
        }
    }
    let mut index_limits = config.limits.clone();
    if let Some(source) = config.remote_index_source.as_ref() {
        index_limits
            .index_activity
            .activity_bucket_roots
            .extend(source.roots.values().flatten().cloned());
    }
    let mut local = LocalBackend::from_config_with_ingestion(
        config.host_id.clone(),
        config.root.clone(),
        index_limits,
        config.roots.clone(),
        config.exclude_globs.clone(),
        if config.index_enabled {
            config.index_path.clone()
        } else {
            None
        },
        config.stt.clone(),
        config.ocr.clone(),
    );
    let remote_index_store = config
        .remote_index_store
        .clone()
        .map(crate::remote_index::RemoteIndexStore::open)
        .transpose()?;
    if let Some(store) = remote_index_store.clone() {
        local = local.with_remote_index_store(store);
    }
    let remote_bind = config.bind;
    let local_bind = config.local_bind;
    if local_bind.is_some_and(|bind| !bind.ip().is_loopback()) {
        return Err(anyhow!("local_bind must be a loopback address"));
    }
    let backup_catalog = config.backup_catalog.clone();
    // A network-bound remote-index source/store requires peer auth above so
    // MCP search cannot bypass the separately authenticated source-sync API.
    // Other deployments retain the existing opt-in peer-auth behavior.
    let require_peer_auth = peer_auth_token.is_some();
    if let (Some(sync), Some(index_path)) = (
        config.remote_index_source.clone(),
        config.index_path.clone(),
    ) {
        crate::remote_index::spawn_source_sync(sync, index_path, remote_index_auth_token.clone());
    }
    let service = Arc::new(
        MeshService::new(local, topology)
            .with_peer_auth_token(peer_auth_token.clone())
            .with_gptadmin_client(topology_client.clone()),
    );
    let jobs = SearchJobs::persistent(config.job_store_root(), &service.local.limits)?;
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
    let console_service = Arc::clone(&service);
    let console_jobs = jobs.clone();
    let console_config = config.clone();
    if config.userio.enabled {
        if !config
            .roots
            .values()
            .flatten()
            .any(|root| root == &config.userio.cache_dir)
        {
            tracing::warn!(
                cache = %config.userio.cache_dir.display(),
                "userio cache_dir is not listed in roots; rendered messages will not be indexed"
            );
        }
        crate::userio::spawn_poller(config.userio.clone());
    }
    let mut remote_app = build_app(AppState {
        service: Arc::clone(&service),
        compute: Arc::clone(&compute),
        jobs: jobs.clone(),
        peer_auth_token: peer_auth_token.clone(),
        remote_index_auth_token: remote_index_auth_token.clone(),
        require_peer_auth,
    });
    let listener = tokio::net::TcpListener::bind(remote_bind).await?;
    if let Some(local_bind) = local_bind.filter(|bind| *bind != remote_bind) {
        let local_listener = tokio::net::TcpListener::bind(local_bind).await?;
        let local_app = build_app(AppState {
            service,
            compute,
            jobs,
            peer_auth_token,
            remote_index_auth_token,
            require_peer_auth: false,
        })
        .merge(crate::console::router(
            console_service,
            console_jobs,
            backup_catalog,
            console_config,
        ));
        tokio::try_join!(
            axum::serve(listener, remote_app),
            axum::serve(local_listener, local_app)
        )?;
    } else {
        if remote_bind.ip().is_loopback() {
            remote_app = remote_app.merge(crate::console::router(
                console_service,
                console_jobs,
                backup_catalog,
                console_config,
            ));
        }
        axum::serve(listener, remote_app).await?;
    }
    Ok(())
}

fn build_app(state: AppState) -> Router {
    let max_request_bytes = state.compute.max_request_bytes().max(2 * 1024 * 1024);
    Router::new()
        .route("/", post(handle_rpc))
        .route("/health", get(health))
        .route("/mcp", post(handle_rpc))
        .route("/compute/status", get(compute_status))
        .route("/compute/ocr", post(compute_extract))
        .route("/index/v1/status", get(remote_index_status))
        .route("/index/v1/upsert", post(remote_index_upsert))
        .route("/index/v1/delete", post(remote_index_delete))
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            guard_request_intake,
        ))
        .with_state(state)
}

fn authorize_remote_index(state: &AppState, headers: &HeaderMap) -> Result<()> {
    // Remote index writes/status use their own bearer token. Do not couple this
    // ACL to the optional general mesh peer token, which would break peers that
    // do not participate in the remote-index source relationship.
    validate_remote_index_auth(headers, state.remote_index_auth_token.as_deref())
}

fn validate_remote_index_auth(headers: &HeaderMap, token: Option<&str>) -> Result<()> {
    let token = token
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("remote index bearer auth is not configured"))?;
    validate_peer_auth(headers, Some(token))
}

async fn remote_index_status(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(error) = authorize_remote_index(&state, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": error.to_string()})),
        )
            .into_response();
    }
    match state
        .service
        .local
        .remote_index
        .as_ref()
        .map(crate::remote_index::RemoteIndexStore::status)
        .transpose()
    {
        Ok(Some(status)) => (StatusCode::OK, Json(status)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error":"remote index store is disabled"})),
        )
            .into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

async fn remote_index_upsert(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(document): Json<crate::remote_index::RemoteDocument>,
) -> Response {
    if let Err(error) = authorize_remote_index(&state, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": error.to_string()})),
        )
            .into_response();
    }
    match state
        .service
        .local
        .remote_index
        .as_ref()
        .ok_or_else(|| anyhow!("remote index store is disabled"))
        .and_then(|store| store.upsert(document))
    {
        Ok(()) => (StatusCode::OK, Json(json!({"accepted":true}))).into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

async fn remote_index_delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(deletion): Json<crate::remote_index::RemoteDelete>,
) -> Response {
    if let Err(error) = authorize_remote_index(&state, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": error.to_string()})),
        )
            .into_response();
    }
    match state
        .service
        .local
        .remote_index
        .as_ref()
        .ok_or_else(|| anyhow!("remote index store is disabled"))
        .and_then(|store| store.delete(deletion))
    {
        Ok(()) => (StatusCode::OK, Json(json!({"accepted":true}))).into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

fn requires_bounded_intake(path: &str, headers: &HeaderMap) -> bool {
    path == "/compute/ocr"
        || matches!(path, "/" | "/mcp")
            && headers
                .get(header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map_or(true, |length| length > 2 * 1024 * 1024)
}

async fn guard_request_intake(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    // Authenticate remote MCP search before buffering its body. Remote-index
    // write routes use their separate token; compute routes validate in-handler;
    // /health intentionally remains public for deployment liveness probes.
    if state.require_peer_auth
        && matches!(request.uri().path(), "/" | "/mcp")
        && validate_peer_auth(request.headers(), state.peer_auth_token.as_deref()).is_err()
    {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "peer authentication failed"})),
        )
            .into_response();
    }
    let _intake = if requires_bounded_intake(request.uri().path(), request.headers()) {
        match state.compute.try_acquire_intake() {
            Ok(permit) => Some(permit),
            Err(_) => {
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(json!({"error": "GPU worker request intake is busy"})),
                )
                    .into_response()
            }
        }
    } else {
        None
    };
    // Holding the permit through the handler bounds both parsing and waiting
    // memory; the separate native slot still survives OCR response deadlines.
    next.run(request).await
}

fn validate_private_compute(state: &AppState, headers: &HeaderMap) -> Result<()> {
    if state.require_peer_auth {
        validate_peer_auth(headers, state.peer_auth_token.as_deref())?;
    }
    validate_origin(headers)
}

async fn compute_status(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(error) = validate_private_compute(&state, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": error.to_string()})),
        )
            .into_response();
    }
    (StatusCode::OK, Json(state.compute.status().await)).into_response()
}

async fn compute_extract(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<OcrExtractRequest>,
) -> Response {
    if let Err(error) = validate_private_compute(&state, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": error.to_string()})),
        )
            .into_response();
    }
    match state.compute.extract(request).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": error.to_string()})),
        )
            .into_response(),
    }
}

async fn health() -> Response {
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
    // Only byte-carrying OCR calls need the raised transport limit. Preserve
    // the former Axum 2 MiB request ceiling for every existing mesh operation.
    let is_ocr = payload.get("method").and_then(Value::as_str) == Some("tools/call")
        && payload.pointer("/params/name").and_then(Value::as_str) == Some("ocr_extract");
    if !is_ocr && serde_json::to_vec(&payload).map_or(true, |bytes| bytes.len() > 2 * 1024 * 1024) {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({"error": "request exceeds mesh request limit"})),
        )
            .into_response();
    }
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
        Err(err) if err.is::<crate::compute_client::InvalidOcrInput>() => {
            json!({"jsonrpc":"2.0","error":{"code":-32042,"message":"OCR input cannot be decoded"},"id":id})
        }
        Err(err) if err.is::<crate::compute_client::OcrRecognitionFailure>() => {
            json!({"jsonrpc":"2.0","error":{"code":-32043,"message":"OCR processing failed"},"id":id})
        }
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
                "serverInfo": {"name": "grepmesh", "version": env!("CARGO_PKG_VERSION")},
                "capabilities": {"tools": {"listChanged": false}},
                "instructions": "Use GrepMesh when the location is unknown, the search may span multiple mesh hosts, or its configured cross-host scopes are needed. For a known local checkout or explicitly named local path, prefer bounded shell rg/find; it is faster and avoids unnecessary mesh fan-out. For GrepMesh, start with search, set a small wait_ms for multi-host requests, and use read_text only for an exact result path and host. If a search returns state=running, call search_status with its job_id. Surface partial host_status failures rather than treating partial results as complete.",
            })
        }
        "tools/list" => json!({
            "tools": [
                tool_meta("search", "Search file names and indexed content across one or more hosts."),
                tool_meta("find_paths", "Find file paths across one or more hosts."),
                tool_meta("find_duplicates", "Hash a bounded sample from configured roots or validated subpaths and report duplicate paths and reclaimable bytes without deleting files."),
                tool_meta("read_text", "Read a text file from a specific host."),
                tool_meta("list_locations", "List configured browse locations across hosts."),
                tool_meta("list_directory", "List immediate safe directory entries for one host."),
                tool_meta("search_status", "Report search/status metadata for one or more hosts."),
                tool_meta("compute_status", "Report OCR performance, fresh host/GPU load and bounded worker admission without inference."),
                tool_meta("ocr_extract", "Extract bounded image/PDF bytes with this host's explicit CPU, CUDA or Apple CoreML worker."),
            ]
        }),
        "tools/call" => match params.get("name").and_then(Value::as_str) {
            Some("compute_status") => {
                tool_content(state.compute.status().await, state.service.as_ref())?
            }
            Some("ocr_extract") => {
                let request = serde_json::from_value(
                    params.get("arguments").cloned().unwrap_or(Value::Null),
                )?;
                tool_content(
                    state.compute.extract(request).await?,
                    state.service.as_ref(),
                )?
            }
            _ => call_tool(state.service.as_ref(), &state.jobs, params).await?,
        },
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
        "compute_status" => json!({"type": "object", "properties": {}}),
        "ocr_extract" => json!({
            "type": "object", "required": ["content_base64", "input_type"], "additionalProperties": false,
            "properties": {
                "content_base64": {"type": "string", "description": "Base64 file bytes; decoded size at most 32 MiB."},
                "input_type": {"type": "string", "enum": ["png", "jpg", "jpeg", "webp", "tif", "tiff", "bmp", "gif", "avif", "pdf"]},
                "filename": {"type": "string", "description": "Optional basename with matching allowed extension; never a path to read."},
                "execution_provider": {"type": "string", "enum": ["cpu", "cuda", "coreml"], "description": "Only this host's available CPU or configured accelerator; never a remote override."},
                "task_class": {"type": "string", "maxLength": 80, "description": "Optional shared TaskDescriptor class; verified against actual input before inference."}
            }
        }),
        "search" | "search_text" => json!({
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string"}, "hosts": hosts,
                "verbose": {"type": "boolean", "description": "Return legacy per-line search hits instead of compact ranges."},
                "roots": {"type": "array", "items": {"type": "string"}},
                "mode": {"type": "string", "enum": ["literal", "regex", "case_insensitive_literal"]},
                "path_globs": {"type": "array", "items": {"type": "string"}},
                "context_lines": {"type": "integer", "minimum": 0},
                "max_matches": {"type": "integer", "minimum": 1},
                "wait_ms": {"type": "integer", "minimum": 0, "default": DEFAULT_FOREGROUND_SEARCH_WAIT_MS,
                    "description": "Foreground wait budget; defaults to 500 ms to return quickly. A running search returns an opaque job_id and can be polled."}
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
        "find_duplicates" => json!({
            "type": "object",
            "required": ["roots"],
            "properties": {
                "hosts": hosts,
                "roots": {"type": "array", "minItems": 1, "items": {"type": "string"}, "description": "Explicit configured root names only."},
                "paths": {"type": "array", "maxItems": 16, "items": {"type": "string"}, "description": "Optional narrower directory paths; each is canonicalized and must remain inside a selected configured root."},
                "max_files": {"type": "integer", "minimum": 2, "maximum": 250, "default": 100, "description": "Maximum files per host. Files over 64 MiB or beyond a 512 MiB total hash budget are skipped; physical reclaimable bytes are null where allocated-block metadata is unavailable."}
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
        "list_locations" => json!({
            "type": "object",
            "properties": {"hosts": hosts}
        }),
        "list_directory" => json!({
            "type": "object",
            "required": ["host", "path"],
            "properties": {
                "host": {"type": "string"}, "path": {"type": "string"}
            }
        }),
        "search_status" => json!({
            "type": "object",
            "properties": {
                "hosts": hosts, "job_id": {"type": "string"},
                "cursor": {"type": "string"}, "page_size": {"type": "integer", "minimum": 1}
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

async fn call_tool(service: &MeshService, jobs: &SearchJobs, params: Value) -> Result<Value> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing tool name"))?;
    let mut arguments = params.get("arguments").cloned().unwrap_or(Value::Null);
    let delegated_acl = params
        .pointer("/_meta/io.gptadmin~1filesystem-acl")
        .cloned();
    apply_filesystem_acl_metadata(name, &mut arguments, delegated_acl)?;
    let tool_result = match name {
        "search" | "search_text" => {
            let mut arguments = arguments;
            let explicit_wait_ms = arguments.get("wait_ms").and_then(Value::as_u64);
            let wait_ms = explicit_wait_ms.unwrap_or(DEFAULT_FOREGROUND_SEARCH_WAIT_MS);
            if let Some(object) = arguments.as_object_mut() {
                object.remove("wait_ms");
            }
            let args = serde_json::from_value(arguments)?;
            if wait_ms == 0 {
                service.call_search(args).await?
            } else {
                let verbose = args.verbose;
                let job_id = jobs.start(service.clone(), args)?;
                jobs.wait(&job_id, Duration::from_millis(wait_ms)).await;
                let mut data = jobs.status(&job_id, None, None)?;
                let running = jobs.is_running(&job_id);
                if !running {
                    // Explicit wait callers historically receive a normal
                    // final search response, not a job envelope.
                    for field in [
                        "state",
                        "job_id",
                        "cursor",
                        "pending_hosts",
                        "next_poll_after_ms",
                        "message",
                    ] {
                        data.as_object_mut()
                            .expect("job status object")
                            .remove(field);
                    }
                }
                if !verbose {
                    data = compact_job_search_data(data)?;
                }
                return Ok(tool_content(data, service)?);
            }
        }
        "find_paths" => {
            service
                .call_find_paths(serde_json::from_value(arguments)?)
                .await?
        }
        "find_duplicates" => {
            service
                .call_find_duplicates(serde_json::from_value(arguments)?)
                .await?
        }
        "read_text" => {
            service
                .call_read_text(serde_json::from_value(arguments)?)
                .await?
        }
        "list_locations" => {
            service
                .call_list_locations(serde_json::from_value(arguments)?)
                .await?
        }
        "list_directory" => {
            service
                .call_list_directory(serde_json::from_value(arguments)?)
                .await?
        }
        "search_status" => {
            if let Some(job_id) = arguments.get("job_id").and_then(Value::as_str) {
                let cursor = arguments.get("cursor").and_then(Value::as_str);
                let page_size = arguments
                    .get("page_size")
                    .and_then(Value::as_u64)
                    .map(|value| value as usize);
                let data = jobs.status(job_id, cursor, page_size)?;
                let data = if data.get("state").and_then(Value::as_str) == Some("expired")
                    || jobs.is_verbose(job_id)?
                {
                    data
                } else {
                    compact_job_search_data(data)?
                };
                return Ok(tool_content(data, service)?);
            }
            service
                .call_status(serde_json::from_value(arguments)?)
                .await?
        }
        other => return Err(anyhow::anyhow!("unknown tool {}", other)),
    };
    Ok(tool_content(tool_result.data, service)?)
}

fn apply_filesystem_acl_metadata(
    name: &str,
    arguments: &mut Value,
    delegated_acl: Option<Value>,
) -> Result<()> {
    let supported = matches!(name, "search" | "search_text" | "read_text");
    if delegated_acl.is_some() && !supported {
        anyhow::bail!("GPTAdmin filesystem ACL is valid only for GrepMesh search/read_text")
    }
    let Some(object) = arguments.as_object_mut() else {
        if delegated_acl.is_some() {
            anyhow::bail!("GPTAdmin filesystem ACL requires object arguments")
        }
        return Ok(());
    };
    let peer_hop = object
        .get("hop_count")
        .and_then(Value::as_u64)
        .is_some_and(|hop| hop > 0)
        && object
            .get("origin_host")
            .and_then(Value::as_str)
            .is_some_and(|host| !host.trim().is_empty());
    let peer_acl = if delegated_acl.is_none() && supported && peer_hop {
        match object.get("filesystem_acl") {
            Some(acl) if !acl.is_null() => Some(acl.clone()),
            _ => anyhow::bail!("peer read requires a verified GPTAdmin filesystem ACL"),
        }
    } else {
        None
    };
    let acl = delegated_acl.or(peer_acl);
    object.remove("filesystem_acl");
    if let Some(acl) = acl {
        crate::mcp::validate_filesystem_acl_value(&acl)?;
        object.insert("filesystem_acl".into(), acl);
    }
    Ok(())
}

fn compact_job_search_data(data: Value) -> Result<Value> {
    let envelope = data.clone();
    let mut compact = compact_search_response_data(data)?;
    for field in [
        "state",
        "job_id",
        "artifact_id",
        "cursor",
        "pending_hosts",
        "next_poll_after_ms",
        "message",
        "lost",
        "error",
    ] {
        if let Some(value) = envelope.get(field) {
            compact[field] = value.clone();
        }
    }
    Ok(compact)
}

fn tool_content(data: Value, service: &MeshService) -> Result<Value> {
    let bounded = bound_tool_data(data, service.local.limits.max_response_bytes)?;
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
    use std::collections::BTreeMap;

    fn filesystem_acl_fixture(root: &str) -> Value {
        json!({
            "version": 1,
            "profile_id": "qa-profile",
            "profile_version": 7,
            "roots_digest": "a".repeat(64),
            "job_id": "qa-job",
            "target": "shell:server-100",
            "tool": "mcp_call",
            "args_sha256": "b".repeat(64),
            "read_roots": [root],
            "write_roots": null
        })
    }

    #[test]
    fn filesystem_acl_comes_from_verified_shell_metadata_and_untrusted_tool_args_are_stripped() {
        let root = tempfile::tempdir().unwrap();
        let acl = filesystem_acl_fixture(&root.path().display().to_string());
        let mut arguments = json!({"query":"allowed marker", "filesystem_acl":acl});
        apply_filesystem_acl_metadata("search", &mut arguments, None).unwrap();
        assert!(arguments.get("filesystem_acl").is_none());

        let mut arguments = json!({"query":"allowed marker"});
        apply_filesystem_acl_metadata("search", &mut arguments, Some(acl.clone())).unwrap();
        assert_eq!(arguments["filesystem_acl"]["profile_id"], "qa-profile");

        let mut peer_args = json!({
            "query":"allowed marker", "origin_host":"server-100", "hop_count":1,
            "filesystem_acl":acl
        });
        apply_filesystem_acl_metadata("search", &mut peer_args, None).unwrap();
        assert_eq!(peer_args["filesystem_acl"]["profile_id"], "qa-profile");

        for missing_acl in [
            json!({"query":"allowed marker", "origin_host":"server-100", "hop_count":1}),
            json!({"query":"allowed marker", "origin_host":"server-100", "hop_count":1, "filesystem_acl":null}),
        ] {
            let mut missing_acl = missing_acl;
            assert!(
                apply_filesystem_acl_metadata("search", &mut missing_acl, None)
                    .unwrap_err()
                    .to_string()
                    .contains("peer read requires a verified GPTAdmin filesystem ACL")
            );
        }

        let mut invalid = json!({"query":"allowed marker"});
        let empty_roots = filesystem_acl_fixture("");
        assert!(apply_filesystem_acl_metadata("search", &mut invalid, Some(empty_roots)).is_err());
        let mut non_search = json!({});
        assert!(apply_filesystem_acl_metadata("find_duplicates", &mut non_search, Some(acl)).is_err());
    }

    #[test]
    fn body_intake_covers_unknown_and_large_rpc_without_blocking_small_search() {
        let mut headers = HeaderMap::new();
        assert!(requires_bounded_intake("/mcp", &headers));
        headers.insert(header::CONTENT_LENGTH, "1024".parse().unwrap());
        assert!(!requires_bounded_intake("/mcp", &headers));
        assert!(requires_bounded_intake("/compute/ocr", &headers));
        assert!(!requires_bounded_intake("/compute/status", &headers));
        headers.insert(header::CONTENT_LENGTH, "2097153".parse().unwrap());
        assert!(requires_bounded_intake("/", &headers));
        assert!(requires_bounded_intake("/mcp", &headers));
    }

    #[tokio::test]
    async fn mcp_search_authorization_denies_missing_bearer_and_allows_authorized_search() {
        let root = tempfile::tempdir().unwrap();
        let marker = "acl-authorized-fixture-marker";
        fs::write(root.path().join("authorized.md"), marker).unwrap();
        let local = LocalBackend::from_config_with_ingestion(
            "local",
            root.path(),
            Default::default(),
            BTreeMap::from([("local".to_string(), vec![root.path().to_path_buf()])]),
            Vec::new(),
            None,
            Default::default(),
            Default::default(),
        );
        let state = AppState {
            service: Arc::new(MeshService::new(local, Topology::new("local", vec![]))),
            compute: Arc::new(ComputeWorker::new(
                "local".into(),
                Default::default(),
                Default::default(),
                128 * 1024,
            )),
            jobs: SearchJobs::default(),
            peer_auth_token: Some("fixture-peer-token".into()),
            remote_index_auth_token: None,
            require_peer_auth: true,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = build_app(state);
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = stop_rx.await;
                })
                .await
                .unwrap();
        });
        let payload = json!({
            "jsonrpc":"2.0", "id":1, "method":"tools/call",
            "params":{"name":"search","arguments":{
                "query":marker,"hosts":["local"],"roots":["local"],
                "max_matches":2,"wait_ms":5000
            }}
        });
        let client = reqwest::Client::new();
        let denied = client
            .post(format!("http://{address}/mcp"))
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let denied_body = denied.text().await.unwrap();
        assert!(!denied_body.contains(marker));

        let allowed = client
            .post(format!("http://{address}/mcp"))
            .bearer_auth("fixture-peer-token")
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);
        let allowed_body = allowed.text().await.unwrap();
        assert!(
            allowed_body.contains(marker),
            "authorized MCP search did not return its fixture hit"
        );
        let _ = stop_tx.send(());
        server.await.unwrap();
    }

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

    #[test]
    fn compact_job_response_keeps_only_the_opaque_artifact_reference() {
        let compact = compact_job_search_data(json!({
            "state": "running", "job_id": "job-deadbeef-1", "artifact_id": "job-deadbeef-1",
            "request_id": "job-deadbeef-1", "origin_host": "A", "hop_count": 0, "host_id": "A",
            "partial": true, "truncated": false, "results": [{
                "host_id": "A", "path": "/private/result.txt", "line_number": 1,
                "column": 1, "text": "needle", "context": []
            }], "host_status": [], "pending_hosts": ["B"], "next_poll_after_ms": 30_000
        }))
        .unwrap();
        assert_eq!(compact["artifact_id"], "job-deadbeef-1");
        assert!(compact.get("artifact_path").is_none());
        assert_eq!(compact["results"][0]["data"], "needle");
    }

    #[tokio::test]
    async fn initialize_routes_known_local_searches_to_rg() {
        let temp = tempfile::tempdir().unwrap();
        let local = LocalBackend::new("local", temp.path(), Default::default());
        let service = Arc::new(MeshService::new(local, Topology::new("local", vec![])));
        let state = AppState {
            service,
            compute: Arc::new(ComputeWorker::new(
                "local".into(),
                Default::default(),
                Default::default(),
                128 * 1024,
            )),
            jobs: SearchJobs::default(),
            peer_auth_token: None,
            remote_index_auth_token: None,
            require_peer_auth: false,
        };
        let response = handle_rpc_inner(
            state,
            json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}),
        )
        .await
        .unwrap();
        let instructions = response["result"]["instructions"].as_str().unwrap();
        assert!(instructions.contains("location is unknown"));
        assert!(instructions.contains("known local checkout"));
        assert!(instructions.contains("rg/find"));
        assert!(instructions.contains("search"));
        assert!(instructions.contains("read_text"));
        assert!(instructions.contains("host_status"));
    }

    #[tokio::test]
    async fn compute_tools_use_disabled_cpu_worker_state() {
        let temp = tempfile::tempdir().unwrap();
        let local = LocalBackend::new("cpu-host", temp.path(), Default::default());
        let state = AppState {
            service: Arc::new(MeshService::new(local, Topology::new("cpu-host", vec![]))),
            compute: Arc::new(ComputeWorker::new(
                "cpu-host".into(),
                Default::default(),
                Default::default(),
                128 * 1024,
            )),
            jobs: SearchJobs::default(),
            peer_auth_token: None,
            remote_index_auth_token: None,
            require_peer_auth: false,
        };
        let response = handle_rpc_inner(state.clone(), json!({"id": 1, "method": "tools/call", "params": {"name": "compute_status", "arguments": {}}})).await.unwrap();
        let status: Value =
            serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap())
                .unwrap();
        assert_eq!(status["host_id"], "cpu-host");
        assert_eq!(status["admission"], "ineligible");
        let extraction = handle_rpc_inner(state, json!({"id": 2, "method": "tools/call", "params": {"name": "ocr_extract", "arguments": {"content_base64": "YWJj", "input_type": "png"}}})).await;
        assert!(extraction.is_err());
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

    #[test]
    fn remote_index_requires_its_dedicated_bearer_token() {
        let mut headers = HeaderMap::new();
        assert!(validate_remote_index_auth(&headers, None).is_err());
        assert!(validate_remote_index_auth(&headers, Some("remote-secret")).is_err());
        headers.insert(header::AUTHORIZATION, "Bearer wrong".parse().unwrap());
        assert!(validate_remote_index_auth(&headers, Some("remote-secret")).is_err());
        headers.insert(
            header::AUTHORIZATION,
            "Bearer remote-secret".parse().unwrap(),
        );
        assert!(validate_remote_index_auth(&headers, Some("remote-secret")).is_ok());
    }
}
