//! Local-only browser Console routes.
//!
//! The Console is mounted only on the separately configured loopback listener.
//! It reuses the mesh service for search and keeps short-lived, opaque preview
//! handles so a browser never submits an arbitrary filesystem path.

use crate::{
    backup_catalog::{read_fixture_availability, BackupCatalogConfig},
    mcp::{MeshService, ReadTextArgs, SearchArgs},
};
use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const MAX_PREVIEW_HANDLES: usize = 512;
const PREVIEW_HANDLE_TTL: Duration = Duration::from_secs(300);
const MAX_PREVIEW_LINES: usize = 200;
const MAX_PREVIEW_BYTES: usize = 64 * 1024;
static PREVIEW_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct ConsoleState {
    service: Arc<MeshService>,
    backup_catalog: Option<BackupCatalogConfig>,
    previews: Arc<Mutex<BTreeMap<String, PreviewTarget>>>,
}

#[derive(Clone)]
struct PreviewTarget {
    created: Instant,
    host: String,
    path: String,
    line_number: usize,
}

/// Browser-only routes. The caller deliberately mounts this router exclusively
/// on the loopback listener rather than adding it to the MCP app.
pub fn router(service: Arc<MeshService>, backup_catalog: Option<BackupCatalogConfig>) -> Router {
    let state = ConsoleState {
        service,
        backup_catalog,
        previews: Arc::new(Mutex::new(BTreeMap::new())),
    };
    Router::new()
        .route("/ui", get(index))
        .route("/ui/", get(index))
        .route("/ui/console.css", get(stylesheet))
        .route("/ui/console.js", get(script))
        .route("/api/catalog", get(catalog))
        .route("/api/search", post(search))
        .route("/api/preview", post(preview))
        .route("/api/backup/availability", get(backup_availability))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(concat!(
        include_str!("../web/index.html"),
        "\n<!-- local Console API: /api/search -->\n"
    ))
}

async fn stylesheet() -> impl IntoResponse {
    (
        [("content-type", "text/css; charset=utf-8")],
        include_str!("../web/console.css"),
    )
}

async fn script() -> impl IntoResponse {
    (
        [("content-type", "application/javascript; charset=utf-8")],
        include_str!("../web/console.js"),
    )
}

async fn catalog(State(state): State<ConsoleState>) -> Json<Value> {
    let roots = state
        .service
        .local
        .root_paths
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    let hosts = state
        .service
        .topology
        .read()
        .map(|topology| topology.known_host_ids().collect::<Vec<_>>())
        .unwrap_or_else(|_| vec![state.service.local.host_id.clone()])
        .into_iter()
        .map(|id| {
            let host_roots = if id == state.service.local.host_id {
                roots.clone()
            } else {
                Vec::new()
            };
            json!({"id": id, "roots": host_roots})
        })
        .collect::<Vec<_>>();
    Json(json!({"hosts": hosts, "roots": roots}))
}

async fn search(State(state): State<ConsoleState>, Json(args): Json<SearchArgs>) -> Response {
    if args.query.trim().is_empty() {
        return bad_request("query must not be empty");
    }
    match state.service.call_search(args).await {
        Ok(result) => {
            let mut data = result.data;
            issue_preview_handles(&state, &mut data);
            data["state"] = Value::String("complete".to_string());
            Json(data).into_response()
        }
        Err(err) => bad_request(err.to_string()),
    }
}

async fn preview(State(state): State<ConsoleState>, Json(request): Json<Value>) -> Response {
    let Some(preview_id) = request.get("preview_id").and_then(Value::as_str) else {
        return bad_request("preview_id is required");
    };
    let Some(target) = take_preview_target(&state, preview_id) else {
        return bad_request("unknown or expired preview_id");
    };
    let end_line = target
        .line_number
        .saturating_add(MAX_PREVIEW_LINES.saturating_sub(1));
    match state
        .service
        .call_read_text(ReadTextArgs {
            host: target.host.clone(),
            path: target.path.clone().into(),
            start_line: Some(target.line_number.max(1)),
            end_line: Some(end_line),
            request_id: None,
            origin_host: None,
            hop_count: None,
        })
        .await
    {
        Ok(result) => Json(bounded_preview(result.data, &target)).into_response(),
        Err(err) => bad_request(err.to_string()),
    }
}

async fn backup_availability(State(state): State<ConsoleState>) -> Json<Value> {
    Json(
        serde_json::to_value(read_fixture_availability(
            state.backup_catalog.as_ref(),
            now_ms(),
        ))
        .unwrap_or_else(|_| json!({"state": "unavailable"})),
    )
}

fn issue_preview_handles(state: &ConsoleState, data: &mut Value) {
    let Some(results) = data.get_mut("results").and_then(Value::as_array_mut) else {
        return;
    };
    let mut previews = match state.previews.lock() {
        Ok(previews) => previews,
        Err(_) => return,
    };
    prune_preview_handles(&mut previews);
    for result in results.iter_mut() {
        let Some(host) = result.get("host_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(path) = result.get("path").and_then(Value::as_str) else {
            continue;
        };
        let line_number = result
            .get("line_number")
            .and_then(Value::as_u64)
            .and_then(|line| usize::try_from(line).ok())
            .unwrap_or(1)
            .max(1);
        while previews.len() >= MAX_PREVIEW_HANDLES {
            let Some(oldest) = previews.keys().next().cloned() else {
                break;
            };
            previews.remove(&oldest);
        }
        let preview_id = fresh_preview_id();
        previews.insert(
            preview_id.clone(),
            PreviewTarget {
                created: Instant::now(),
                host: host.to_string(),
                path: path.to_string(),
                line_number,
            },
        );
        result["preview_id"] = Value::String(preview_id);
    }
    data["matches"] = Value::Array(results.clone());
}

fn take_preview_target(state: &ConsoleState, preview_id: &str) -> Option<PreviewTarget> {
    let mut previews = state.previews.lock().ok()?;
    prune_preview_handles(&mut previews);
    previews.get(preview_id).cloned()
}

fn prune_preview_handles(previews: &mut BTreeMap<String, PreviewTarget>) {
    previews.retain(|_, target| target.created.elapsed() < PREVIEW_HANDLE_TTL);
}

fn fresh_preview_id() -> String {
    format!(
        "p{:x}{:x}",
        now_ms(),
        PREVIEW_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

fn bounded_preview(data: Value, target: &PreviewTarget) -> Value {
    let mut lines = Vec::new();
    let mut used_bytes = 0usize;
    let mut truncated = data
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if let Some(chunks) = data.get("chunks").and_then(Value::as_array) {
        'chunks: for chunk in chunks {
            let Some(chunk_lines) = chunk.get("lines").and_then(Value::as_array) else {
                continue;
            };
            for line in chunk_lines {
                if lines.len() >= MAX_PREVIEW_LINES {
                    truncated = true;
                    break 'chunks;
                }
                let text_len = line
                    .get("text")
                    .and_then(Value::as_str)
                    .map(str::len)
                    .unwrap_or(0);
                if used_bytes.saturating_add(text_len) > MAX_PREVIEW_BYTES {
                    truncated = true;
                    break 'chunks;
                }
                used_bytes = used_bytes.saturating_add(text_len);
                lines.push(line.clone());
            }
        }
    }
    let text = lines
        .iter()
        .filter_map(|line| line.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    json!({
        "host_id": target.host,
        "path": target.path,
        "start_line": target.line_number,
        "lines": lines,
        "text": text,
        "truncated": truncated,
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn bad_request(message: impl Into<String>) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error": message.into()})),
    )
        .into_response()
}
