//! Local-only browser Console routes.
//!
//! The Console is mounted only on the separately configured loopback listener.
//! It reuses the mesh service for search and keeps short-lived, opaque preview
//! handles so a browser never submits an arbitrary filesystem path.

use crate::{
    backend::MatchLine,
    backup_catalog::{read_fixture_availability, BackupCatalogConfig},
    config::{AppConfig, SttConfig},
    jobs::SearchJobs,
    mcp::{
        ListDirectoryArgs, ListLocationsArgs, MeshService, ReadTextArgs, SearchArgs, StatusArgs,
    },
};
use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::PathBuf,
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
    jobs: SearchJobs,
    backup_catalog: Option<BackupCatalogConfig>,
    config: Arc<Mutex<AppConfig>>,
    previews: Arc<Mutex<BTreeMap<String, PreviewTarget>>>,
}

#[derive(Clone)]
struct PreviewTarget {
    created: Instant,
    host: String,
    path: String,
    line_number: usize,
    match_lines: Vec<MatchLine>,
    match_truncated: bool,
}

/// Browser-only routes. The caller deliberately mounts this router exclusively
/// on the loopback listener rather than adding it to the MCP app.
pub fn router(
    service: Arc<MeshService>,
    jobs: SearchJobs,
    backup_catalog: Option<BackupCatalogConfig>,
    config: AppConfig,
) -> Router {
    let state = ConsoleState {
        service,
        jobs,
        backup_catalog,
        config: Arc::new(Mutex::new(config)),
        previews: Arc::new(Mutex::new(BTreeMap::new())),
    };
    Router::new()
        .route("/", get(|| async { Redirect::temporary("/ui/") }))
        .route("/ui", get(|| async { Redirect::temporary("/ui/") }))
        .route("/ui/", get(index))
        .route("/ui/console.css", get(stylesheet))
        .route("/ui/console.js", get(script))
        .route("/ui/markdown-it.js", get(markdown_script))
        .route("/ui/settings", get(settings_page))
        .route("/ui/settings.js", get(settings_script))
        .route("/api/settings", get(settings).post(save_settings))
        .route("/api/catalog", get(catalog))
        .route("/api/host-status", get(host_status))
        .route("/api/telemetry", get(telemetry))
        .route("/api/browse", post(browse))
        .route("/api/search", post(search))
        .route("/api/search/status", post(search_status))
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

async fn settings_page() -> Html<&'static str> {
    Html(include_str!("../web/settings.html"))
}

async fn settings_script() -> impl IntoResponse {
    (
        [("content-type", "application/javascript; charset=utf-8")],
        include_str!("../web/settings.js"),
    )
}

async fn settings(State(state): State<ConsoleState>) -> Response {
    let config = match state.config.lock() {
        Ok(config) => config.clone(),
        Err(_) => return bad_request("settings lock is unavailable"),
    };
    Json(json!({
        "host_id": config.host_id,
        "bind": config.bind.to_string(),
        "local_bind": config.local_bind.map(|bind| bind.to_string()),
        "root": config.root,
        "roots": config.roots,
        "exclude_globs": config.exclude_globs,
        "index_path": config.index_path,
        "stt": config.stt,
        "ocr": config.ocr,
        "config_path": config.config_path,
        "restart_required_for_changes": true
    }))
    .into_response()
}

async fn save_settings(State(state): State<ConsoleState>, Json(request): Json<Value>) -> Response {
    let stt: SttConfig = match request.get("stt") {
        Some(value) => match serde_json::from_value(value.clone()) {
            Ok(stt) => stt,
            Err(error) => return bad_request(format!("invalid stt settings: {error}")),
        },
        None => match state.config.lock() {
            Ok(config) => config.stt.clone(),
            Err(_) => return bad_request("settings lock is unavailable"),
        },
    };
    let ocr = match request.get("ocr") {
        Some(value) => match serde_json::from_value(value.clone()) {
            Ok(ocr) => ocr,
            Err(error) => return bad_request(format!("invalid OCR settings: {error}")),
        },
        None => match state.config.lock() {
            Ok(config) => config.ocr.clone(),
            Err(_) => return bad_request("settings lock is unavailable"),
        },
    };
    let mut config = match state.config.lock() {
        Ok(config) => config,
        Err(_) => return bad_request("settings lock is unavailable"),
    };
    let Some(path) = config.config_path.clone() else {
        return bad_request("this GrepMesh instance was not started from a writable config file");
    };
    config.stt = stt;
    config.ocr = ocr;
    let bytes = match serde_json::to_vec_pretty(&*config) {
        Ok(bytes) => bytes,
        Err(error) => return bad_request(format!("serialize settings: {error}")),
    };
    let tmp = path.with_extension("json.tmp");
    if let Err(error) = std::fs::write(&tmp, bytes).and_then(|_| std::fs::rename(&tmp, &path)) {
        let _ = std::fs::remove_file(&tmp);
        return bad_request(format!("save settings: {error}"));
    }
    Json(json!({"ok": true, "restart_required": true, "config_path": path})).into_response()
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

async fn markdown_script() -> impl IntoResponse {
    (
        [("content-type", "application/javascript; charset=utf-8")],
        include_str!("../web/vendor/markdown-it-15.0.3.min.js"),
    )
}

async fn catalog(State(state): State<ConsoleState>) -> Response {
    match state
        .service
        .call_list_locations(ListLocationsArgs {
            hosts: None,
            request_id: None,
            origin_host: None,
            hop_count: None,
        })
        .await
    {
        Ok(result) => {
            let locations = result
                .data
                .get("locations")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let mut roots_by_host = BTreeMap::<String, Vec<String>>::new();
            for location in &locations {
                let (Some(host), Some(path)) = (
                    location.get("host").and_then(Value::as_str),
                    location.get("path").and_then(Value::as_str),
                ) else {
                    continue;
                };
                roots_by_host
                    .entry(host.to_string())
                    .or_default()
                    .push(path.to_string());
            }
            for status in result
                .data
                .get("host_status")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(host) = status.get("host_id").and_then(Value::as_str) {
                    roots_by_host.entry(host.to_string()).or_default();
                }
            }
            for roots in roots_by_host.values_mut() {
                roots.sort();
                roots.dedup();
            }
            let roots = roots_by_host
                .values()
                .flatten()
                .cloned()
                .collect::<Vec<_>>();
            let statuses = result
                .host_status
                .iter()
                .map(|status| {
                    (
                        status.host_id.clone(),
                        (status.ok, safe_host_error(status.error.as_deref())),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let hosts = roots_by_host
                .into_iter()
                .map(|(id, roots)| {
                    let (healthy, last_error) = statuses.get(&id).cloned().unwrap_or((
                        false,
                        Some("Device did not report its catalog status.".to_string()),
                    ));
                    let health = if healthy {
                        "healthy"
                    } else if last_error
                        .as_deref()
                        .is_some_and(|error| error.starts_with("Some configured paths"))
                    {
                        "degraded"
                    } else {
                        "offline"
                    };
                    json!({
                        "id": id,
                        "roots": roots,
                        "health": health,
                        "last_error": last_error,
                    })
                })
                .collect::<Vec<_>>();
            Json(json!({
                "hosts": hosts,
                "roots": roots,
                "partial": result.partial,
                "host_status": result.host_status,
            }))
            .into_response()
        }
        Err(err) => bad_request(err.to_string()),
    }
}

async fn browse(State(state): State<ConsoleState>, Json(request): Json<Value>) -> Response {
    let Some(host) = request.get("host").and_then(Value::as_str) else {
        return bad_request("host is required");
    };
    let Some(path) = request.get("path").and_then(Value::as_str) else {
        return bad_request("path is required");
    };
    match state
        .service
        .call_list_directory(ListDirectoryArgs {
            host: host.to_string(),
            path: PathBuf::from(path),
            request_id: None,
            origin_host: None,
            hop_count: None,
        })
        .await
    {
        Ok(result) => {
            let mut data = result.data;
            issue_browse_preview_handles(&state, &mut data, host);
            Json(data).into_response()
        }
        Err(err) => bad_request(err.to_string()),
    }
}

async fn search(State(state): State<ConsoleState>, Json(mut args): Json<SearchArgs>) -> Response {
    if args.query.trim().is_empty() {
        return bad_request("query must not be empty");
    }
    args.verbose = true;
    match state.jobs.start((*state.service).clone(), args) {
        Ok(job_id) => match state.jobs.status(&job_id, None, None) {
            Ok(mut data) => {
                issue_preview_handles(&state, &mut data);
                Json(data).into_response()
            }
            Err(err) => bad_request(err.to_string()),
        },
        Err(err) => bad_request(err.to_string()),
    }
}

async fn host_status(State(state): State<ConsoleState>) -> Response {
    match state
        .service
        .call_status(StatusArgs {
            hosts: None,
            request_id: None,
            origin_host: None,
            hop_count: None,
        })
        .await
    {
        Ok(result) => Json(result.data).into_response(),
        Err(error) => bad_request(error.to_string()),
    }
}

async fn telemetry(State(state): State<ConsoleState>) -> Response {
    match state.jobs.telemetry() {
        Ok(data) => Json(data).into_response(),
        Err(error) => bad_request(error.to_string()),
    }
}

async fn search_status(State(state): State<ConsoleState>, Json(request): Json<Value>) -> Response {
    let Some(job_id) = request.get("job_id").and_then(Value::as_str) else {
        return bad_request("job_id is required");
    };
    match state.jobs.status(
        job_id,
        request.get("cursor").and_then(Value::as_str),
        request
            .get("page_size")
            .and_then(Value::as_u64)
            .map(|value| value as usize),
    ) {
        Ok(mut data) => {
            issue_preview_handles(&state, &mut data);
            Json(data).into_response()
        }
        Err(err) => bad_request(err.to_string()),
    }
}

fn safe_host_error(error: Option<&str>) -> Option<String> {
    error.map(|error| {
        let lower = error.to_ascii_lowercase();
        if lower.contains("timeout") {
            "Device did not respond before the request deadline.".to_string()
        } else if lower.contains("unauthorized") || lower.contains("401") {
            "Device rejected the peer request.".to_string()
        } else if lower.contains("not readable") || lower.contains("permission") {
            "Some configured paths are not readable.".to_string()
        } else {
            "Device is unavailable or returned an incomplete result.".to_string()
        }
    })
}

async fn preview(State(state): State<ConsoleState>, Json(request): Json<Value>) -> Response {
    let Some(preview_id) = request.get("preview_id").and_then(Value::as_str) else {
        return bad_request("preview_id is required");
    };
    let Some(target) = take_preview_target(&state, preview_id) else {
        return bad_request("unknown or expired preview_id");
    };
    let view = request
        .get("view")
        .and_then(Value::as_str)
        .unwrap_or("match");
    if !matches!(view, "match" | "window" | "document") {
        return bad_request("view must be match, window, or document");
    }
    let mut preview_target = target.clone();
    if view == "document" {
        preview_target.line_number = 1;
    }
    if view == "match" && !target.match_lines.is_empty() {
        preview_target.line_number = target.match_lines[0].line_number;
        let data = json!({"chunks": [{"lines": target.match_lines}]});
        return Json(bounded_preview(data, &preview_target)).into_response();
    }
    if view == "window" {
        preview_target.line_number = target
            .line_number
            .saturating_sub(MAX_PREVIEW_LINES / 2)
            .max(1);
    }
    let end_line = if view == "document" {
        MAX_PREVIEW_LINES.saturating_add(1)
    } else {
        preview_target
            .line_number
            .saturating_add(MAX_PREVIEW_LINES.saturating_sub(1))
    };
    match state
        .service
        .call_read_text(ReadTextArgs {
            host: target.host.clone(),
            path: target.path.clone().into(),
            start_line: Some(preview_target.line_number.max(1)),
            end_line: Some(end_line),
            request_id: None,
            origin_host: None,
            hop_count: None,
        })
        .await
    {
        Ok(result) => Json(bounded_preview(result.data, &preview_target)).into_response(),
        Err(err)
            if matches!(view, "window" | "document")
                && !target.match_lines.is_empty()
                && is_preview_size_limit(&err) =>
        {
            tracing::warn!(
                host_id = %target.host,
                reason = "bounded_read_limit",
                "file preview fell back to indexed match context"
            );
            let mut fallback_target = target.clone();
            fallback_target.line_number = target.match_lines[0].line_number;
            let data = json!({"chunks": [{"lines": target.match_lines.clone()}]});
            let mut preview = bounded_preview(data, &fallback_target);
            if view == "document" {
                preview["document_fallback"] = Value::Bool(true);
            } else {
                preview["window_fallback"] = Value::Bool(true);
            }
            Json(preview).into_response()
        }
        Err(err) => bad_request(err.to_string()),
    }
}

fn is_preview_size_limit(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string().to_ascii_lowercase();
        message.contains("read limit") || message.contains("max_file_bytes")
    })
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
    issue_entry_preview_handles(state, results, None, false);
    data["matches"] = Value::Array(results.clone());
}

fn issue_browse_preview_handles(state: &ConsoleState, data: &mut Value, host: &str) {
    let Some(entries) = data.get_mut("entries").and_then(Value::as_array_mut) else {
        return;
    };
    issue_entry_preview_handles(state, entries, Some(host), true);
}

fn issue_entry_preview_handles(
    state: &ConsoleState,
    entries: &mut [Value],
    fallback_host: Option<&str>,
    files_only: bool,
) {
    let mut previews = match state.previews.lock() {
        Ok(previews) => previews,
        Err(_) => return,
    };
    issue_entry_preview_handles_in_map(&mut previews, entries, fallback_host, files_only);
}

fn issue_entry_preview_handles_in_map(
    previews: &mut BTreeMap<String, PreviewTarget>,
    entries: &mut [Value],
    fallback_host: Option<&str>,
    files_only: bool,
) {
    prune_preview_handles(previews);
    let eligible_count = entries
        .iter()
        .filter(|entry| {
            let is_file = entry
                .get("kind")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind.eq_ignore_ascii_case("file"));
            (!files_only || is_file)
                && entry.get("path").and_then(Value::as_str).is_some()
                && entry
                    .get("host_id")
                    .or_else(|| entry.get("host"))
                    .and_then(Value::as_str)
                    .or(fallback_host)
                    .is_some()
        })
        .count();
    let batch_capacity = eligible_count.min(MAX_PREVIEW_HANDLES);
    while previews.len() > MAX_PREVIEW_HANDLES.saturating_sub(batch_capacity) {
        let Some(oldest) = previews.keys().next().cloned() else {
            break;
        };
        previews.remove(&oldest);
    }
    let mut issued_in_batch = 0;
    for entry in entries {
        let is_file = entry
            .get("kind")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind.eq_ignore_ascii_case("file"));
        if files_only && !is_file {
            continue;
        }
        let host = entry
            .get("host_id")
            .or_else(|| entry.get("host"))
            .and_then(Value::as_str)
            .or(fallback_host);
        let Some(host) = host else { continue };
        let Some(path) = entry.get("path").and_then(Value::as_str) else {
            continue;
        };
        if issued_in_batch >= batch_capacity {
            entry["preview_unavailable"] = Value::String("handle_limit".to_string());
            if let Some(object) = entry.as_object_mut() {
                let _ = object.remove("preview_id");
            }
            continue;
        }
        let line_number = entry
            .get("line_number")
            .and_then(Value::as_u64)
            .and_then(|line| usize::try_from(line).ok())
            .unwrap_or(1)
            .max(1);
        let mut match_lines: Vec<MatchLine> = entry
            .get("context")
            .cloned()
            .and_then(|context| serde_json::from_value(context).ok())
            .unwrap_or_default();
        if !match_lines
            .iter()
            .any(|line| line.line_number == line_number)
        {
            if let Some(text) = entry
                .get("text")
                .or_else(|| entry.get("snippet"))
                .and_then(Value::as_str)
            {
                match_lines.push(MatchLine {
                    line_number,
                    text: text.to_string(),
                });
            }
        }
        match_lines.sort_by_key(|line| line.line_number);
        match_lines.dedup_by_key(|line| line.line_number);
        match_lines.sort_by_key(|line| {
            (
                line.line_number != line_number,
                line.line_number.abs_diff(line_number),
                line.line_number,
            )
        });
        let mut match_truncated = match_lines.len() > MAX_PREVIEW_LINES;
        match_lines.truncate(MAX_PREVIEW_LINES);
        let mut remaining_bytes = MAX_PREVIEW_BYTES;
        let mut bounded_lines = Vec::with_capacity(match_lines.len());
        for mut line in match_lines {
            if remaining_bytes == 0 {
                match_truncated = true;
                break;
            }
            if line.text.len() > remaining_bytes {
                truncate_utf8(&mut line.text, remaining_bytes);
                match_truncated = true;
            }
            remaining_bytes = remaining_bytes.saturating_sub(line.text.len());
            bounded_lines.push(line);
        }
        match_lines = bounded_lines;
        match_lines.sort_by_key(|line| line.line_number);
        let preview_id = fresh_preview_id();
        previews.insert(
            preview_id.clone(),
            PreviewTarget {
                created: Instant::now(),
                host: host.to_string(),
                path: path.to_string(),
                line_number,
                match_lines,
                match_truncated,
            },
        );
        entry["preview_id"] = Value::String(preview_id);
        if let Some(object) = entry.as_object_mut() {
            let _ = object.remove("preview_unavailable");
        }
        issued_in_batch += 1;
    }
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

fn truncate_utf8(value: &mut String, max_bytes: usize) {
    if value.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
}

fn bounded_preview(data: Value, target: &PreviewTarget) -> Value {
    let mut lines = Vec::new();
    let mut used_bytes = 0usize;
    let mut truncated = target.match_truncated
        || data
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browse_preview_capacity_never_expires_handles_from_the_same_response() {
        let mut entries: Vec<Value> = (0..520)
            .map(|index| {
                json!({
                    "kind": "file",
                    "path": format!("/docs/file-{index:04}.txt"),
                })
            })
            .collect();
        let mut previews = BTreeMap::new();

        issue_entry_preview_handles_in_map(&mut previews, &mut entries, Some("server-100"), true);

        let handles: Vec<&str> = entries
            .iter()
            .filter_map(|entry| entry["preview_id"].as_str())
            .collect();
        assert_eq!(handles.len(), MAX_PREVIEW_HANDLES);
        assert_eq!(previews.len(), MAX_PREVIEW_HANDLES);
        assert!(
            entries
                .iter()
                .filter(|entry| entry["preview_unavailable"] == "handle_limit")
                .count()
                == 8
        );
        assert!(handles.iter().all(|handle| previews.contains_key(*handle)));
    }

    #[test]
    fn bounded_match_context_prioritizes_the_match_within_the_byte_budget() {
        let mut entries = vec![json!({
            "kind": "file",
            "path": "/docs/large-context.txt",
            "line_number": 2,
            "context": [
                {"line_number": 1, "text": "before"},
                {"line_number": 2, "text": "n".repeat(MAX_PREVIEW_BYTES)}
            ]
        })];
        let mut previews = BTreeMap::new();

        issue_entry_preview_handles_in_map(&mut previews, &mut entries, Some("server-100"), false);

        let preview = previews
            .get(entries[0]["preview_id"].as_str().unwrap())
            .unwrap();
        assert!(preview.match_truncated);
        assert_eq!(preview.match_lines.len(), 1);
        assert_eq!(preview.match_lines[0].line_number, 2);
        assert_eq!(preview.match_lines[0].text.len(), MAX_PREVIEW_BYTES);
    }
}
