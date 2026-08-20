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
    for _ in 0..40 {
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
    assert!(ui.contains("No file selected."));
    assert!(ui.contains("Choose a root or directory to browse metadata"));
    assert!(ui.contains("<script src=\"/ui/console.js?v=20260820-host-failures\" defer></script>"));
    assert!(!ui.contains("Directory browsing is not available yet."));
    assert!(ui.contains("id=\"host-failures\""));

    let script = client
        .get(format!(
            "{}/ui/console.js?v=20260820-host-failures",
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
    assert!(script.contains("Host ${failure.host} unavailable"));

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
async fn local_console_browse_lists_immediate_safe_entries_for_a_catalog_root() {
    let fixture = tempfile::tempdir().unwrap();
    fs::create_dir(fixture.path().join("nested")).unwrap();
    fs::write(fixture.path().join("finder-canary.txt"), "finder canary\n").unwrap();
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
    fs::write(
        fixture.path().join("preview-canary.txt"),
        "GREP_CONSOLE_PREVIEW_CANARY\n",
    )
    .unwrap();
    let harness = start_console(fixture.path()).await;
    let client = Client::new();

    let search = client
        .post(format!("{}/api/search", harness.local_base))
        .json(&json!({
            "query": "GREP_CONSOLE_PREVIEW_CANARY",
            "hosts": "local",
            "max_matches": 10,
            "wait_ms": 1_000,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(search.status(), StatusCode::OK);
    let search: Value = search.json().await.unwrap();
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
