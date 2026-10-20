use grepmesh::config::AppConfig;
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::{
    fs,
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tempfile::TempDir;

struct ConsoleHarness {
    _temp: TempDir,
    child: Child,
    remote_base: String,
    local_base: String,
}

impl Drop for ConsoleHarness {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn wait_until_ready(client: &Client, base: &str) {
    for _ in 0..160 {
        if client.get(base).send().await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("console server did not become ready at {base}");
}

async fn start_console(root: &Path) -> ConsoleHarness {
    let temp = tempfile::tempdir().unwrap();
    let remote_port = free_port();
    let local_port = free_port();
    let remote_base = format!("http://127.0.0.1:{remote_port}");
    let local_base = format!("http://127.0.0.1:{local_port}");
    let config: AppConfig = serde_json::from_value(json!({
        "host_id": "local",
        "bind": format!("127.0.0.1:{remote_port}"),
        "local_bind": format!("127.0.0.1:{local_port}"),
        "root": root,
    }))
    .unwrap();
    let config_path = temp.path().join("grepmesh.json");
    fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_grepmesh-mcp"))
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let client = Client::new();
    wait_until_ready(&client, &local_base).await;

    ConsoleHarness {
        _temp: temp,
        child,
        remote_base,
        local_base,
    }
}

fn assert_browser_json(response: &reqwest::Response) {
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    assert!(
        content_type.starts_with("application/json"),
        "expected a JSON browser API response, got {content_type:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_console_ui_and_catalog_have_a_browser_safe_success_shape() {
    let fixture = tempfile::tempdir().unwrap();
    let harness = start_console(fixture.path()).await;
    let client = Client::new();

    let ui = client
        .get(format!("{}/ui", harness.local_base))
        .send()
        .await
        .unwrap();
    assert_eq!(ui.status(), StatusCode::OK);
    assert!(ui
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/html")));
    let ui = ui.text().await.unwrap();
    assert!(ui.contains("/api/search"));
    assert!(ui.contains("id=\"host-sidebar\""));
    assert!(ui.contains("class=\"results finder-list\""));
    assert!(ui.contains("Select a file or folder"));
    assert!(ui.contains("Details appear here"));
    assert!(ui.contains("id=\"breadcrumbs\""));
    assert!(ui.contains("aria-label=\"Current path\""));
    assert!(ui.contains("id=\"bookmarks-sidebar\""));
    assert!(ui.contains("id=\"telemetry-panel\""));
    for column in ["name", "size", "modified", "created"] {
        assert!(ui.contains(&format!("data-sort=\"{column}\"")));
    }
    assert!(ui.contains("<script src=\"/ui/markdown-it.js?v=15.0.3\" defer></script>"));
    assert!(ui.contains("<script src=\"/ui/console.js?v=file-preview-20261020i\" defer></script>"));
    assert!(!ui.contains("Directory browsing is not available yet."));
    assert!(ui.contains("id=\"host-failures\""));

    let script = client
        .get(format!(
            "{}/ui/console.js?v=finder-20260907",
            harness.local_base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(script.status(), StatusCode::OK);
    let script = script.text().await.unwrap();
    assert!(script.contains("/api/browse"));
    assert!(script.contains("browseDirectory(root)"));
    assert!(script.contains("renderHostFailures(details.failures)"));
    assert!(script.contains("Host ${failure.host} reported an error"));
    assert!(script.contains("/api/search/status"));
    assert!(script.contains("pollSearch"));
    assert!(script.contains("/api/host-status"));
    assert!(script.contains("/api/telemetry"));
    let markdown = client
        .get(format!("{}/ui/markdown-it.js?v=15.0.3", harness.local_base))
        .send()
        .await
        .unwrap();
    assert_eq!(markdown.status(), StatusCode::OK);
    assert!(markdown
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/javascript")));
    assert!(markdown.text().await.unwrap().contains("markdownit"));

    let catalog = client
        .get(format!("{}/api/catalog", harness.local_base))
        .send()
        .await
        .unwrap();
    assert_eq!(catalog.status(), StatusCode::OK);
    assert_browser_json(&catalog);
    let catalog: Value = catalog.json().await.unwrap();
    assert!(catalog["hosts"].is_array());
    assert!(catalog["roots"].is_array());
    assert!(catalog["hosts"].as_array().unwrap().iter().all(|host| {
        host["id"].is_string()
            && host.get("token").is_none()
            && host.get("bearer").is_none()
            && host.get("url").is_none()
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn base_url_opens_webui_and_settings_are_linked_and_persisted() {
    let fixture = tempfile::tempdir().unwrap();
    let harness = start_console(fixture.path()).await;
    let client = Client::new();

    let base = client.get(&harness.local_base).send().await.unwrap();
    assert_eq!(base.status(), StatusCode::OK);
    assert_eq!(base.url().path(), "/ui/");
    let html = base.text().await.unwrap();
    assert!(html.contains("href=\"/ui/settings\""));

    let settings_page = client
        .get(format!("{}/ui/settings", harness.local_base))
        .send()
        .await
        .unwrap();
    assert_eq!(settings_page.status(), StatusCode::OK);
    assert!(settings_page
        .text()
        .await
        .unwrap()
        .contains("Media transcription"));

    let settings: Value = client
        .get(format!("{}/api/settings", harness.local_base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(settings["stt"]["enabled"], false);
    assert_eq!(settings["stt"]["backend"], "auto");

    let saved: Value = client
        .post(format!("{}/api/settings", harness.local_base))
        .json(&json!({"stt": {
            "enabled": true,
            "backend": "auto",
            "model": "auto",
            "auto_download": true,
            "max_media_bytes": 4294967296u64
        }}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(saved["ok"], true);
    assert_eq!(saved["restart_required"], true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_console_browse_lists_immediate_safe_entries_for_a_catalog_root() {
    let fixture = tempfile::tempdir().unwrap();
    fs::create_dir(fixture.path().join("nested")).unwrap();
    fs::write(fixture.path().join("finder-canary.txt"), "finder canary\n").unwrap();
    fs::write(
        fixture.path().join("long.md"),
        (1..=205)
            .map(|line| format!("line {line}\n"))
            .collect::<Vec<_>>()
            .join(""),
    )
    .unwrap();
    let harness = start_console(fixture.path()).await;
    let client = Client::new();

    let catalog = client
        .get(format!("{}/api/catalog", harness.local_base))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let root = catalog["roots"]
        .as_array()
        .and_then(|roots| roots.first())
        .and_then(Value::as_str)
        .expect("the local catalog must expose its configured root");

    let browse = client
        .post(format!("{}/api/browse", harness.local_base))
        .json(&json!({"host": "local", "path": root}))
        .send()
        .await
        .unwrap();
    let browse_status = browse.status();
    assert_browser_json(&browse);
    let browse: Value = browse.json().await.unwrap();
    assert_eq!(browse_status, StatusCode::OK, "{browse}");
    let entries = browse["entries"]
        .as_array()
        .expect("browse must return a browser-safe entries array");
    assert!(entries.iter().any(|entry| {
        entry["name"] == "nested" && entry["kind"] == "directory" && entry["path"].is_string()
    }));
    assert!(entries.iter().any(|entry| {
        entry["name"] == "finder-canary.txt" && entry["kind"] == "file" && entry["size"].is_number()
    }));
    let preview_id = entries
        .iter()
        .find(|entry| entry["name"] == "finder-canary.txt")
        .and_then(|entry| entry["preview_id"].as_str())
        .expect("a browsed file must get an opaque preview handle");
    let preview = client
        .post(format!("{}/api/preview", harness.local_base))
        .json(&json!({"preview_id": preview_id}))
        .send()
        .await
        .unwrap();
    assert_eq!(preview.status(), StatusCode::OK);
    let preview: Value = preview.json().await.unwrap();
    assert!(preview["text"]
        .as_str()
        .is_some_and(|text| text.contains("finder canary")));
    assert_eq!(preview["start_line"], 1);
    let long_preview_id = entries
        .iter()
        .find(|entry| entry["name"] == "long.md")
        .and_then(|entry| entry["preview_id"].as_str())
        .expect("a browsed document must receive a bounded preview handle");
    let document_preview = client
        .post(format!("{}/api/preview", harness.local_base))
        .json(&json!({"preview_id": long_preview_id, "view": "document"}))
        .send()
        .await
        .unwrap();
    assert_eq!(document_preview.status(), StatusCode::OK);
    let document_preview: Value = document_preview.json().await.unwrap();
    assert_eq!(document_preview["lines"].as_array().unwrap().len(), 200);
    assert_eq!(document_preview["truncated"], true);
    let document_preview = client
        .post(format!("{}/api/preview", harness.local_base))
        .json(&json!({"preview_id": preview_id, "view": "document"}))
        .send()
        .await
        .unwrap();
    assert_eq!(document_preview.status(), StatusCode::OK);
    let invalid_view = client
        .post(format!("{}/api/preview", harness.local_base))
        .json(&json!({"preview_id": preview_id, "view": "other"}))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_view.status(), StatusCode::BAD_REQUEST);
    assert!(entries
        .iter()
        .find(|entry| entry["name"] == "nested")
        .unwrap()
        .get("preview_id")
        .is_none());
    assert!(entries.iter().all(|entry| {
        entry.get("token").is_none() && entry.get("bearer").is_none() && entry.get("url").is_none()
    }));

    let remote_browse = client
        .post(format!("{}/api/browse", harness.remote_base))
        .json(&json!({"host": "local", "path": root}))
        .send()
        .await
        .unwrap();
    assert_eq!(remote_browse.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_match_context_keeps_the_match_when_prior_context_uses_the_byte_budget() {
    let fixture = tempfile::tempdir().unwrap();
    fs::write(
        fixture.path().join("large-context.txt"),
        format!("{}\nneedle\n", "x".repeat(64 * 1024)),
    )
    .unwrap();
    let harness = start_console(fixture.path()).await;
    let client = Client::new();
    let mut search: Value = client
        .post(format!("{}/api/search", harness.local_base))
        .json(&json!({"query": "needle"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    for _ in 0..40 {
        if search["results"]
            .as_array()
            .is_some_and(|results| !results.is_empty())
        {
            break;
        }
        let Some(job_id) = search["job_id"].as_str() else {
            break;
        };
        tokio::time::sleep(Duration::from_millis(25)).await;
        search = client
            .post(format!("{}/api/search/status", harness.local_base))
            .json(&json!({"job_id": job_id}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    }
    let preview_id = search["results"][0]["preview_id"].as_str().unwrap();
    let preview: Value = client
        .post(format!("{}/api/preview", harness.local_base))
        .json(&json!({"preview_id": preview_id}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(preview["truncated"], true);
    assert!(preview["text"]
        .as_str()
        .is_some_and(|text| text.contains("needle")));
    assert!(!preview["lines"]
        .as_array()
        .unwrap()
        .iter()
        .any(|line| line["text"] == ""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_search_keeps_running_partial_truncated_and_host_status_fields() {
    let fixture = tempfile::tempdir().unwrap();
    fs::write(
        fixture.path().join("console-canary.txt"),
        "GREP_CONSOLE_CONTRACT_CANARY\n",
    )
    .unwrap();
    let harness = start_console(fixture.path()).await;
    let client = Client::new();

    let search = client
        .post(format!("{}/api/search", harness.local_base))
        .json(&json!({
            "query": "GREP_CONSOLE_CONTRACT_CANARY",
            "hosts": "local",
            "roots": [],
            "max_matches": 10,
            "wait_ms": 0,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(search.status(), StatusCode::OK);
    assert_browser_json(&search);
    let search: Value = search.json().await.unwrap();
    assert!(matches!(
        search["state"].as_str(),
        Some("running") | Some("complete")
    ));
    assert!(search["partial"].is_boolean());
    assert!(search["truncated"].is_boolean());
    assert!(search["host_status"].is_array());
    assert!(search["host_status"]
        .as_array()
        .unwrap()
        .iter()
        .all(|status| status["host_id"].is_string() && status["ok"].is_boolean()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preview_requires_an_opaque_search_result_handle_not_an_arbitrary_path() {
    let fixture = tempfile::tempdir().unwrap();
    let mut content = vec![b'x'; 1024 * 1024 + 32];
    content.extend_from_slice(b"\nGREP_CONSOLE_PREVIEW_CANARY\n");
    fs::write(fixture.path().join("preview-canary.txt"), content).unwrap();
    let harness = start_console(fixture.path()).await;
    let client = Client::new();

    let search = client
        .post(format!("{}/api/search", harness.local_base))
        .json(&json!({
            "query": "GREP_CONSOLE_PREVIEW_CANARY",
            "hosts": "local",
            "max_matches": 10,
            "context_lines": 0,
            "wait_ms": 1_000,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(search.status(), StatusCode::OK);
    let mut search: Value = search.json().await.unwrap();
    for _ in 0..40 {
        if search["results"]
            .as_array()
            .is_some_and(|results| !results.is_empty())
        {
            break;
        }
        let job_id = search["job_id"]
            .as_str()
            .expect("running search has a job id");
        tokio::time::sleep(Duration::from_millis(25)).await;
        search = client
            .post(format!("{}/api/search/status", harness.local_base))
            .json(&json!({"job_id": job_id}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    }
    let preview_id = search["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|result| result["preview_id"].as_str())
        .expect("a completed search result must issue a preview_id");

    let approved_preview = client
        .post(format!("{}/api/preview", harness.local_base))
        .json(&json!({"preview_id": preview_id}))
        .send()
        .await
        .unwrap();
    assert_eq!(approved_preview.status(), StatusCode::OK);
    assert_browser_json(&approved_preview);
    let approved_preview: Value = approved_preview.json().await.unwrap();
    assert!(approved_preview["lines"].is_array());
    assert!(approved_preview["truncated"].is_boolean());
    assert!(approved_preview["lines"].as_array().unwrap().len() <= 200);
    assert_eq!(approved_preview["start_line"], 2);
    assert!(approved_preview["text"]
        .as_str()
        .is_some_and(|text| text.contains("GREP_CONSOLE_PREVIEW_CANARY")));

    let bounded_fallback = client
        .post(format!("{}/api/preview", harness.local_base))
        .json(&json!({"preview_id": preview_id, "view": "window"}))
        .send()
        .await
        .unwrap();
    assert_eq!(bounded_fallback.status(), StatusCode::OK);
    let bounded_fallback: Value = bounded_fallback.json().await.unwrap();
    assert_eq!(bounded_fallback["window_fallback"], true);
    assert!(bounded_fallback["text"]
        .as_str()
        .is_some_and(|text| text.contains("GREP_CONSOLE_PREVIEW_CANARY")));

    let document_fallback = client
        .post(format!("{}/api/preview", harness.local_base))
        .json(&json!({"preview_id": preview_id, "view": "document"}))
        .send()
        .await
        .unwrap();
    assert_eq!(document_fallback.status(), StatusCode::OK);
    let document_fallback: Value = document_fallback.json().await.unwrap();
    assert_eq!(document_fallback["document_fallback"], true);
    assert!(document_fallback["text"]
        .as_str()
        .is_some_and(|text| text.contains("GREP_CONSOLE_PREVIEW_CANARY")));

    let preview = client
        .post(format!("{}/api/preview", harness.local_base))
        .json(&json!({
            "host": "local",
            "path": "/etc/passwd",
            "start_line": 1,
            "end_line": 10,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(preview.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_preview_window_opens_near_the_match_and_stays_bounded() {
    let fixture = tempfile::tempdir().unwrap();
    let content = (1..=500)
        .map(|line| {
            if line == 250 {
                format!("line {line}: WINDOW_PREVIEW_CANARY")
            } else {
                format!("line {line}: surrounding text")
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(fixture.path().join("window.txt"), content).unwrap();
    let harness = start_console(fixture.path()).await;
    let client = Client::new();
    let mut search: Value = client
        .post(format!("{}/api/search", harness.local_base))
        .json(&json!({
            "query": "WINDOW_PREVIEW_CANARY",
            "hosts": "local",
            "max_matches": 10,
            "context_lines": 0,
            "wait_ms": 1_000,
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    for _ in 0..40 {
        if search["results"]
            .as_array()
            .is_some_and(|rows| !rows.is_empty())
        {
            break;
        }
        let job_id = search["job_id"]
            .as_str()
            .expect("running search has a job id");
        tokio::time::sleep(Duration::from_millis(25)).await;
        search = client
            .post(format!("{}/api/search/status", harness.local_base))
            .json(&json!({"job_id": job_id}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    }
    let preview_id = search["results"][0]["preview_id"]
        .as_str()
        .expect("a completed search result must issue a preview_id");
    let preview: Value = client
        .post(format!("{}/api/preview", harness.local_base))
        .json(&json!({"preview_id": preview_id, "view": "window"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(preview["start_line"], 150);
    assert_eq!(preview["lines"].as_array().unwrap().len(), 200);
    assert!(preview["text"]
        .as_str()
        .unwrap()
        .contains("WINDOW_PREVIEW_CANARY"));
    assert!(preview["lines"][0]["text"]
        .as_str()
        .unwrap()
        .contains("line 150:"));
    assert!(preview["lines"][199]["text"]
        .as_str()
        .unwrap()
        .contains("line 349:"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_availability_is_local_only_and_separate_from_search_results() {
    let fixture = tempfile::tempdir().unwrap();
    let harness = start_console(fixture.path()).await;
    let client = Client::new();

    let backup = client
        .get(format!("{}/api/backup/availability", harness.local_base))
        .send()
        .await
        .unwrap();
    assert_eq!(backup.status(), StatusCode::OK);
    assert_browser_json(&backup);
    let backup: Value = backup.json().await.unwrap();
    assert!(matches!(
        backup["state"].as_str(),
        Some("unconfigured") | Some("unavailable") | Some("stale") | Some("available")
    ));
    assert!(backup.get("results").is_none());
    assert!(backup.get("host_status").is_none());

    let remote_ui = client
        .get(format!("{}/ui", harness.remote_base))
        .send()
        .await
        .unwrap();
    assert_eq!(remote_ui.status(), StatusCode::NOT_FOUND);
}
