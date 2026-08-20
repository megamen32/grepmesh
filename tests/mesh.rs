use grepmesh::{
    backend::{LocalBackend, SearchMode},
    config::AppConfig,
    jobs::SearchJobs,
    mcp::{normalize_request, HostsInput, MeshService, SearchArgs},
    topology::{PeerConfig, Topology},
};
use std::{
    fs,
    io::{Read, Write},
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tempfile::TempDir;

#[test]
fn normalize_wildcard_hop_and_dedup_rules() {
    let req = normalize_request(
        "A",
        vec!["A".to_string(), "B".to_string(), "C".to_string()],
        Some(HostsInput::One("*".into())),
        Some("req-1".into()),
        None,
        Some(0),
    )
    .unwrap();
    assert_eq!(req.hosts, vec!["A", "B", "C"]);

    let req = normalize_request(
        "A",
        vec!["A".to_string(), "B".to_string()],
        Some(HostsInput::Many(vec!["A".into(), "B".into(), "A".into()])),
        None,
        None,
        Some(0),
    )
    .unwrap();
    assert_eq!(req.hosts, vec!["A", "B"]);

    assert!(normalize_request(
        "A",
        vec!["A".into(), "B".into()],
        Some(HostsInput::One("*".into())),
        None,
        None,
        Some(2)
    )
    .is_err());
    assert!(normalize_request(
        "A",
        vec!["A".into(), "B".into()],
        Some(HostsInput::Many(vec!["B".into()])),
        None,
        None,
        Some(1)
    )
    .is_err());
}

#[test]
fn missing_search_job_is_explicitly_expired_without_result_payload() {
    let status = SearchJobs::default()
        .status("opaque-missing-job", None, None)
        .unwrap();
    assert_eq!(status["state"], "expired");
    assert_eq!(status["lost"], true);
    assert!(status.get("results").is_none());
    assert!(status.get("matches").is_none());
    assert!(status["error"]
        .as_str()
        .unwrap()
        .contains("expired or was lost"));
}

#[test]
fn read_text_routes_by_host_and_path() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();
    let path = root.join("note.txt");
    fs::write(&path, "hello\nworld\n").unwrap();
    let backend = grepmesh::backend::LocalBackend::new("A", root, Default::default());
    let chunks = backend.read_text(&path, Some(1), Some(2)).unwrap();
    assert_eq!(chunks[0].lines.len(), 2);
    assert_eq!(chunks[0].lines[0].text, "hello");
}

#[test]
fn browse_lists_configured_locations_and_immediate_safe_entries() {
    let temp = TempDir::new().unwrap();
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(workspace.join("nested")).unwrap();
    fs::create_dir_all(workspace.join("node_modules")).unwrap();
    fs::write(workspace.join("note.txt"), "finder canary\n").unwrap();
    fs::write(
        workspace.join("node_modules").join("hidden.js"),
        "ignored\n",
    )
    .unwrap();

    let mut roots = std::collections::BTreeMap::new();
    roots.insert("workspace".to_string(), vec![workspace.clone()]);
    let backend = LocalBackend::new("A", temp.path(), Default::default()).with_named_roots(roots);

    let locations = backend.list_locations();
    assert!(locations
        .iter()
        .any(|location| location.name == "workspace"
            && location.path == workspace.display().to_string()));

    let entries = backend.list_directory(&workspace).unwrap();
    assert!(entries
        .iter()
        .any(|entry| entry.name == "nested" && entry.kind == "directory"));
    assert!(entries
        .iter()
        .any(|entry| entry.name == "note.txt" && entry.kind == "file" && entry.size == Some(14)));
    assert!(!entries.iter().any(|entry| entry.name == "node_modules"));
    assert!(backend
        .list_directory(&temp.path().join("outside"))
        .is_err());
    assert!(backend
        .list_directory(&workspace.join("node_modules"))
        .is_err());
}

#[tokio::test]
async fn malformed_search_inputs_are_failed_partial_mcp_results() {
    let root = tempfile::tempdir().unwrap();
    fs::write(root.path().join("config.rs"), "SEARCH_INPUT_TOKEN\n").unwrap();
    let service = MeshService::new(
        LocalBackend::from_config(
            "A",
            root.path(),
            Default::default(),
            std::collections::BTreeMap::new(),
            vec![],
            None,
        ),
        Topology::new("A", vec![]),
    );

    for (request_id, query, mode, path_globs) in [
        ("invalid-regex", "(", SearchMode::Regex, vec![]),
        (
            "invalid-glob",
            "SEARCH_INPUT_TOKEN",
            SearchMode::Literal,
            vec!["[".into()],
        ),
    ] {
        let result = service
            .call_search(SearchArgs {
                query: query.into(),
                verbose: false,
                hosts: Some(HostsInput::One("local".into())),
                request_id: Some(request_id.into()),
                origin_host: None,
                hop_count: None,
                limit: Some(10),
                context_lines: Some(0),
                mode,
                path_globs,
                roots: vec![],
            })
            .await
            .unwrap();

        assert!(result.partial, "{request_id}");
        assert!(result.data["results"].as_array().unwrap().is_empty());
        let status = result
            .host_status
            .iter()
            .find(|status| status.host_id == "A")
            .unwrap();
        assert!(!status.ok, "{request_id}");
        assert!(status
            .error
            .as_deref()
            .is_some_and(|error| !error.is_empty()));
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn permission_denied_search_is_partial_and_keeps_readable_match() {
    let service = MeshService::new(
        LocalBackend::from_config(
            "A",
            "/proc/1",
            Default::default(),
            std::collections::BTreeMap::new(),
            vec![],
            None,
        ),
        Topology::new("A", vec![]),
    );
    let result = service
        .call_search(SearchArgs {
            query: "Name".into(),
            verbose: false,
            hosts: Some(HostsInput::One("local".into())),
            request_id: Some("permission-partial".into()),
            origin_host: None,
            hop_count: None,
            limit: Some(10),
            context_lines: Some(0),
            mode: SearchMode::Literal,
            path_globs: vec!["**/status".into()],
            roots: vec![],
        })
        .await
        .unwrap();

    assert!(result.partial);
    assert!(result.data["results"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hit| hit["data"]
            .as_str()
            .is_some_and(|text| text.contains("Name"))));
    let status = result
        .host_status
        .iter()
        .find(|status| status.host_id == "A")
        .unwrap();
    assert!(!status.ok);
    assert_eq!(
        status.error.as_deref(),
        Some("some configured paths were not readable")
    );
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn write_config(path: &PathBuf, cfg: &AppConfig) {
    fs::write(path, serde_json::to_vec_pretty(cfg).unwrap()).unwrap();
}

fn spawn_server(config: &PathBuf) -> Child {
    Command::new(env!("CARGO_BIN_EXE_grepmesh-mcp"))
        .arg("--config")
        .arg(config)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

async fn rpc(url: &str, method: &str, params: serde_json::Value) -> serde_json::Value {
    let client = reqwest::Client::new();
    let mut request = client
        .post(url)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2026-07-28")
        .header("Mcp-Method", method);
    if let Some(name) = params.get("name").and_then(|value| value.as_str()) {
        request = request.header("Mcp-Name", name);
    }
    let resp = request
        .json(&serde_json::json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
        .send()
        .await
        .unwrap();
    resp.json::<serde_json::Value>().await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn black_box_two_process_peer_fanout_and_partial_results() {
    let temp_a = TempDir::new().unwrap();
    let temp_b = TempDir::new().unwrap();
    let root_a = temp_a.path().to_path_buf();
    let root_b = temp_b.path().to_path_buf();
    fs::write(root_a.join("a-canary.txt"), "alpha canary from A\n").unwrap();
    fs::write(root_b.join("b-canary.txt"), "bravo canary from B\n").unwrap();
    fs::create_dir(root_b.join("child")).unwrap();
    fs::write(root_b.join("child").join("inside.txt"), "browse child\n").unwrap();
    let port_a = free_port();
    let port_b = free_port();
    let url_a = format!("http://127.0.0.1:{port_a}/mcp");
    let url_b = format!("http://127.0.0.1:{port_b}/mcp");
    let cfg_b = AppConfig {
        host_id: "B".into(),
        bind: format!("127.0.0.1:{port_b}").parse().unwrap(),
        local_bind: None,
        root: root_b.clone(),
        roots: std::collections::BTreeMap::new(),
        peers: vec![],
        limits: Default::default(),
        exclude_globs: vec![],
        topology_cache_path: None,
        index_path: Some(root_b.join("index.sqlite")),
        gptadmin_topology_url: None,
        gptadmin_token_env: None,
        peer_auth_token_env: None,
        backup_catalog: None,
        topology_ttl_ms: 30_000,
    };
    let cfg_a = AppConfig {
        host_id: "A".into(),
        bind: format!("127.0.0.1:{port_a}").parse().unwrap(),
        local_bind: None,
        root: root_a.clone(),
        roots: std::collections::BTreeMap::new(),
        peers: vec![PeerConfig {
            host_id: "B".into(),
            local_url: "http://127.0.0.1:1/mcp".into(),
            routable_url: url_b.clone(),
        }],
        limits: Default::default(),
        exclude_globs: vec![],
        topology_cache_path: None,
        index_path: Some(root_a.join("index.sqlite")),
        gptadmin_topology_url: None,
        gptadmin_token_env: None,
        peer_auth_token_env: None,
        backup_catalog: None,
        topology_ttl_ms: 30_000,
    };
    let path_a = temp_a.path().join("a.json");
    let path_b = temp_b.path().join("b.json");
    write_config(&path_a, &cfg_a);
    write_config(&path_b, &cfg_b);
    for (path, legacy_index_path) in [
        (&path_a, temp_a.path().join("legacy-index-a.json")),
        (&path_b, temp_b.path().join("legacy-index-b.json")),
    ] {
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        value["index_path"] = serde_json::json!(legacy_index_path);
        fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    }
    let mut child_b = spawn_server(&path_b);
    let mut child_a = spawn_server(&path_a);
    tokio::time::sleep(Duration::from_millis(700)).await;

    let init = rpc(&url_a, "initialize", serde_json::json!({})).await;
    assert_eq!(init["jsonrpc"], "2.0");
    let tools = rpc(&url_a, "tools/list", serde_json::json!({})).await;
    let tool_names = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"list_locations"));
    assert!(tool_names.contains(&"list_directory"));

    let status = rpc(
        &url_a,
        "tools/call",
        serde_json::json!({
            "name": "search_status",
            "arguments": {"hosts": "local"}
        }),
    )
    .await;
    let status_text = status["result"]["content"][0]["text"].as_str().unwrap();
    let status_value: serde_json::Value = serde_json::from_str(status_text).unwrap();
    assert_eq!(status_value["local"]["backend"], "indexed+rg-fallback");

    let search = rpc(
        &url_a,
        "tools/call",
        serde_json::json!({
            "name": "search_text",
            "arguments": {"query": "canary", "limit": 10}
        }),
    )
    .await;
    let result_text = search["result"]["content"][0]["text"].as_str().unwrap();
    let value: serde_json::Value = serde_json::from_str(result_text).unwrap();
    assert_eq!(value["host_id"], "A");
    let results = value["results"].as_array().unwrap();
    assert!(results.iter().any(|r| r["path"]
        .as_str()
        .is_some_and(|path| path.ends_with("a-canary.txt"))));
    assert!(results.iter().any(|r| r["path"]
        .as_str()
        .is_some_and(|path| path.ends_with("b-canary.txt"))));

    let paths = rpc(
        &url_a,
        "tools/call",
        serde_json::json!({
            "name": "find_paths",
            "arguments": {"pattern": "*canary*", "hosts": "*", "max_matches": 10}
        }),
    )
    .await;
    let paths_text = paths["result"]["content"][0]["text"].as_str().unwrap();
    let paths_value: serde_json::Value = serde_json::from_str(paths_text).unwrap();
    assert!(paths_value["paths"].as_array().unwrap().iter().any(|path| {
        path["host"] == "A" && path["path"].as_str().unwrap().ends_with("a-canary.txt")
    }));
    assert!(paths_value["paths"].as_array().unwrap().iter().any(|path| {
        path["host"] == "B" && path["path"].as_str().unwrap().ends_with("b-canary.txt")
    }));

    let read_remote = rpc(&url_a, "tools/call", serde_json::json!({
        "name": "read_text",
        "arguments": {"host": "B", "path": root_b.join("b-canary.txt"), "start_line": 1, "end_line": 1}
    })).await;
    let read_value: serde_json::Value = serde_json::from_str(
        read_remote["result"]["content"][0]["text"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(read_value["host_id"], "A");
    assert_eq!(read_value["target_host_id"], "B");
    assert_eq!(
        read_value["chunks"][0]["lines"][0]["text"],
        "bravo canary from B"
    );

    let locations = rpc(
        &url_a,
        "tools/call",
        serde_json::json!({
            "name": "list_locations",
            "arguments": {"hosts": "*"}
        }),
    )
    .await;
    let locations_value: serde_json::Value =
        serde_json::from_str(locations["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert!(locations_value["locations"]
        .as_array()
        .unwrap()
        .iter()
        .any(
            |location| location["host"] == "B" && location["path"] == root_b.display().to_string()
        ));
    assert_eq!(locations_value["host_status"].as_array().unwrap().len(), 2);

    let directory = rpc(
        &url_a,
        "tools/call",
        serde_json::json!({
            "name": "list_directory",
            "arguments": {"host": "B", "path": root_b}
        }),
    )
    .await;
    let directory_value: serde_json::Value =
        serde_json::from_str(directory["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(directory_value["target_host_id"], "B");
    assert!(directory_value["entries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| entry["name"] == "child" && entry["kind"] == "directory"));

    fs::remove_file(root_b.join("b-canary.txt")).unwrap();
    let _ = child_b.kill();
    let _ = child_b.wait();

    let search_partial = rpc(
        &url_a,
        "tools/call",
        serde_json::json!({
            "name": "search_text",
            "arguments": {"query": "canary", "hosts": "*", "limit": 10}
        }),
    )
    .await;
    let partial_value: serde_json::Value = serde_json::from_str(
        search_partial["result"]["content"][0]["text"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(partial_value["partial"], true);
    let partial_results = partial_value["results"].as_array().unwrap();
    assert!(partial_results.iter().any(|r| r["path"]
        .as_str()
        .is_some_and(|path| path.ends_with("a-canary.txt"))));
    assert!(!partial_results.iter().any(|r| r["path"]
        .as_str()
        .is_some_and(|path| path.ends_with("b-canary.txt"))));

    let _ = child_a.kill();
    let _ = child_a.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_partial_status_and_local_results_survive_fanout() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().to_path_buf();
    fs::write(root.join("local-canary.txt"), "local partial canary\n").unwrap();
    let fake_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let fake_url = format!(
        "http://127.0.0.1:{}/mcp",
        fake_listener.local_addr().unwrap().port()
    );
    let fake_thread = std::thread::spawn(move || {
        let response_data = serde_json::json!({
            "request_id": "fake-peer-request",
            "origin_host": "A",
            "hop_count": 1,
            "host_id": "B",
            "partial": true,
            "truncated": false,
            "results": [{
                "host_id": "B",
                "path": "/fake/partial-canary.txt",
                "line_number": 1,
                "context": [],
                "text": "fake peer partial",
                "column": 1
            }],
            "host_status": [{
                "host_id": "B",
                "ok": false,
                "error": "peer-local-timeout"
            }]
        });
        let envelope = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "content": [{
                    "type": "text",
                    "text": serde_json::to_string(&response_data).unwrap()
                }],
                "isError": false
            }
        });
        let body = serde_json::to_vec(&envelope).unwrap();
        for _ in 0..2 {
            let (mut stream, _) = fake_listener.accept().unwrap();
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .unwrap();
            stream.write_all(&body).unwrap();
        }
    });

    let port = free_port();
    let url = format!("http://127.0.0.1:{port}/mcp");
    let cfg = AppConfig {
        host_id: "A".into(),
        bind: format!("127.0.0.1:{port}").parse().unwrap(),
        local_bind: None,
        root: root.clone(),
        roots: std::collections::BTreeMap::new(),
        peers: vec![PeerConfig {
            host_id: "B".into(),
            local_url: "http://127.0.0.1:1/mcp".into(),
            routable_url: fake_url,
        }],
        limits: grepmesh::config::LimitsConfig {
            peer_timeout_ms: 500,
            overall_timeout_ms: 1000,
            ..Default::default()
        },
        exclude_globs: vec![],
        topology_cache_path: None,
        index_path: Some(root.join("index.sqlite")),
        gptadmin_topology_url: None,
        gptadmin_token_env: None,
        peer_auth_token_env: None,
        backup_catalog: None,
        topology_ttl_ms: 30_000,
    };
    let path = temp.path().join("config.json");
    write_config(&path, &cfg);
    let mut child = spawn_server(&path);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let search = rpc(
        &url,
        "tools/call",
        serde_json::json!({
            "name": "search_text",
            "arguments": {"query": "partial", "hosts": "*", "limit": 10}
        }),
    )
    .await;
    let search_value: serde_json::Value =
        serde_json::from_str(search["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(search_value["partial"], true);
    assert!(search_value["results"]
        .as_array()
        .unwrap()
        .iter()
        .any(|result| result["path"] == "/fake/partial-canary.txt"));
    assert!(search_value["host_status"]
        .as_array()
        .unwrap()
        .iter()
        .any(|status| status["host_id"] == "B" && status["ok"] == false));

    let paths = rpc(
        &url,
        "tools/call",
        serde_json::json!({
            "name": "find_paths",
            "arguments": {"pattern": "partial", "hosts": "*", "max_matches": 10}
        }),
    )
    .await;
    let paths_value: serde_json::Value =
        serde_json::from_str(paths["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(paths_value["partial"], true);
    assert!(paths_value["paths"]
        .as_array()
        .unwrap()
        .iter()
        .any(|path| path["host"] == "B"));
    assert!(paths_value["host_status"]
        .as_array()
        .unwrap()
        .iter()
        .any(|status| status["host_id"] == "B" && status["ok"] == false));

    let _ = child.kill();
    let _ = child.wait();
    fake_thread.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_peer_body_keeps_completed_local_results() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().to_path_buf();
    fs::write(root.join("local-stalled.txt"), "local stalled canary\n").unwrap();
    let fake_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let fake_url = format!(
        "http://127.0.0.1:{}/mcp",
        fake_listener.local_addr().unwrap().port()
    );
    let fake_thread = std::thread::spawn(move || {
        let (mut stream, _) = fake_listener.accept().unwrap();
        let mut request = [0u8; 4096];
        let _ = stream.read(&mut request);
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100000\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(700));
    });

    let port = free_port();
    let url = format!("http://127.0.0.1:{port}/mcp");
    let cfg = AppConfig {
        host_id: "A".into(),
        bind: format!("127.0.0.1:{port}").parse().unwrap(),
        local_bind: None,
        root: root.clone(),
        roots: std::collections::BTreeMap::new(),
        peers: vec![PeerConfig {
            host_id: "B".into(),
            local_url: "http://127.0.0.1:1/mcp".into(),
            routable_url: fake_url,
        }],
        limits: grepmesh::config::LimitsConfig {
            peer_timeout_ms: 150,
            overall_timeout_ms: 500,
            ..Default::default()
        },
        exclude_globs: vec![],
        topology_cache_path: None,
        index_path: Some(root.join("index.sqlite")),
        gptadmin_topology_url: None,
        gptadmin_token_env: None,
        peer_auth_token_env: None,
        backup_catalog: None,
        topology_ttl_ms: 30_000,
    };
    let path = temp.path().join("config.json");
    write_config(&path, &cfg);
    let mut child = spawn_server(&path);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let search = rpc(
        &url,
        "tools/call",
        serde_json::json!({
            "name": "search_text",
            "arguments": {"query": "stalled", "hosts": "*", "limit": 10}
        }),
    )
    .await;
    let value: serde_json::Value =
        serde_json::from_str(search["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(value["partial"], true);
    assert!(value["results"]
        .as_array()
        .unwrap()
        .iter()
        .any(|result| result["path"]
            .as_str()
            .is_some_and(|path| path.ends_with("local-stalled.txt"))));
    assert!(value["host_status"]
        .as_array()
        .unwrap()
        .iter()
        .any(|status| status["host_id"] == "B" && status["ok"] == false));

    let _ = child.kill();
    let _ = child.wait();
    fake_thread.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_search_status_returns_a_bounded_final_page() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().to_path_buf();
    fs::write(
        root.join("many-async-canary.txt"),
        (0..6)
            .map(|number| format!("async canary {number}\n"))
            .collect::<String>(),
    )
    .unwrap();
    let fake_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let fake_url = format!(
        "http://127.0.0.1:{}/mcp",
        fake_listener.local_addr().unwrap().port()
    );
    let fake_thread = std::thread::spawn(move || {
        let (mut stream, _) = fake_listener.accept().unwrap();
        let mut request = [0u8; 4096];
        let _ = stream.read(&mut request);
        std::thread::sleep(Duration::from_millis(250));
        let result_text = serde_json::to_string(&serde_json::json!({
            "request_id": "peer-async", "origin_host": "B", "hop_count": 1,
            "host_id": "A", "partial": false, "truncated": false,
            "results": [{
                "host_id": "A", "path": "/remote-async.txt", "line_number": 1,
                "column": 1, "text": "async canary remote", "context": []
            }],
            "host_status": [{"host_id": "A", "ok": true, "error": null}]
        }))
        .unwrap();
        let body = serde_json::to_vec(&serde_json::json!({
            "jsonrpc": "2.0", "id": 1,
            "result": {"content": [{"type": "text", "text": result_text}], "isError": false}
        }))
        .unwrap();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(&body).unwrap();
    });

    let port = free_port();
    let url = format!("http://127.0.0.1:{port}/mcp");
    let cfg = AppConfig {
        host_id: "B".into(),
        bind: format!("127.0.0.1:{port}").parse().unwrap(),
        local_bind: None,
        root: root.clone(),
        roots: std::collections::BTreeMap::new(),
        peers: vec![PeerConfig {
            host_id: "A".into(),
            local_url: "http://127.0.0.1:1/mcp".into(),
            routable_url: fake_url,
        }],
        limits: grepmesh::config::LimitsConfig {
            peer_timeout_ms: 500,
            // This is the ordinary synchronous fan-out ceiling. The job must
            // outlive it and still collect A after the 250ms peer delay.
            overall_timeout_ms: 50,
            max_results: 10,
            ..Default::default()
        },
        exclude_globs: vec![],
        topology_cache_path: None,
        index_path: None,
        gptadmin_topology_url: None,
        gptadmin_token_env: None,
        peer_auth_token_env: None,
        backup_catalog: None,
        topology_ttl_ms: 30_000,
    };
    let path = temp.path().join("config.json");
    write_config(&path, &cfg);
    let mut child = spawn_server(&path);
    tokio::time::sleep(Duration::from_millis(400)).await;

    let search = rpc(
        &url,
        "tools/call",
        serde_json::json!({
            "name": "search_text",
            "arguments": {
                "query": "async canary", "hosts": "*", "limit": 10, "wait_ms": 100, "verbose": true
            }
        }),
    )
    .await;
    let running: serde_json::Value =
        serde_json::from_str(search["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(running["state"], "running");
    let job_id = running["job_id"].as_str().unwrap().to_string();
    assert!(running["results"]
        .as_array()
        .unwrap()
        .iter()
        .any(|result| result["host_id"] == "B"));
    assert!(running["host_status"]
        .as_array()
        .unwrap()
        .iter()
        .any(|status| status["host_id"] == "B"));
    assert!(running["pending_hosts"]
        .as_array()
        .unwrap()
        .iter()
        .any(|host| host == "A"));
    assert_eq!(running["next_poll_after_ms"], 30_000);
    assert!(running["message"]
        .as_str()
        .unwrap()
        .contains("Search continues"));
    let delivered_cursor = running["cursor"].as_str().unwrap().to_string();

    tokio::time::sleep(Duration::from_millis(350)).await;
    let complete = rpc(
        &url,
        "tools/call",
        serde_json::json!({
            "name": "search_status",
            "arguments": {"job_id": job_id, "cursor": delivered_cursor, "page_size": 2}
        }),
    )
    .await;
    let incremental: serde_json::Value =
        serde_json::from_str(complete["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(incremental["state"], "complete");
    assert_eq!(incremental["results"].as_array().unwrap().len(), 1);
    // A sorts before the already-delivered B, but its later arrival is still
    // returned exactly once because the cursor is an append-only watermark.
    assert_eq!(incremental["results"][0]["host_id"], "A");

    let complete = rpc(
        &url,
        "tools/call",
        serde_json::json!({
            "name": "search_status",
            "arguments": {"job_id": job_id, "page_size": 2}
        }),
    )
    .await;
    let first_page: serde_json::Value =
        serde_json::from_str(complete["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(first_page["state"], "complete");
    assert_eq!(first_page["results"].as_array().unwrap().len(), 2);
    let cursor = first_page["cursor"].as_str().unwrap().to_string();

    // The opaque reference is durable: a fresh server process can retrieve
    // the private service-owned artifact without receiving its filesystem path.
    let _ = child.kill();
    let _ = child.wait();
    child = spawn_server(&path);
    tokio::time::sleep(Duration::from_millis(400)).await;
    let restored = rpc(
        &url,
        "tools/call",
        serde_json::json!({
            "name": "search_status",
            "arguments": {"job_id": job_id, "page_size": 2}
        }),
    )
    .await;
    let restored: serde_json::Value =
        serde_json::from_str(restored["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(restored["state"], "complete");
    assert!(restored.get("artifact_id").is_none());
    assert!(restored.get("artifact_path").is_none());

    let next = rpc(
        &url,
        "tools/call",
        serde_json::json!({
            "name": "search_status",
            "arguments": {"job_id": job_id, "cursor": cursor, "page_size": 2}
        }),
    )
    .await;
    let second_page: serde_json::Value =
        serde_json::from_str(next["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(second_page["state"], "complete");
    assert_eq!(second_page["results"].as_array().unwrap().len(), 2);
    assert_ne!(
        first_page["results"][0]["line_number"],
        second_page["results"][0]["line_number"]
    );

    let _ = child.kill();
    let _ = child.wait();
    fake_thread.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_with_sufficient_wait_returns_the_complete_result_directly() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().to_path_buf();
    fs::write(root.join("direct-canary.txt"), "direct wait canary\n").unwrap();
    let port = free_port();
    let url = format!("http://127.0.0.1:{port}/mcp");
    let cfg = AppConfig {
        host_id: "A".into(),
        bind: format!("127.0.0.1:{port}").parse().unwrap(),
        local_bind: None,
        root: root.clone(),
        roots: std::collections::BTreeMap::new(),
        peers: vec![],
        limits: Default::default(),
        exclude_globs: vec![],
        topology_cache_path: None,
        index_path: None,
        gptadmin_topology_url: None,
        gptadmin_token_env: None,
        peer_auth_token_env: None,
        backup_catalog: None,
        topology_ttl_ms: 30_000,
    };
    let path = temp.path().join("config.json");
    write_config(&path, &cfg);
    let mut child = spawn_server(&path);
    tokio::time::sleep(Duration::from_millis(400)).await;

    let search = rpc(
        &url,
        "tools/call",
        serde_json::json!({
            "name": "search_text",
            "arguments": {"query": "direct wait canary", "wait_ms": 500}
        }),
    )
    .await;
    let value: serde_json::Value =
        serde_json::from_str(search["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert!(value.get("state").is_none());
    assert!(value.get("job_id").is_none());
    assert!(value["results"]
        .as_array()
        .unwrap()
        .iter()
        .any(|result| result["path"]
            .as_str()
            .is_some_and(|path| path.ends_with("direct-canary.txt"))));

    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test]
async fn search_text_tool_defaults_to_compact_ranges_with_verbose_raw_opt_in() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().to_path_buf();
    let file = root.join("result.rs");
    fs::write(&file, "before\nneedle one\nneedle two\nafter\n").unwrap();
    let port = free_port();
    let url = format!("http://127.0.0.1:{port}/mcp");
    let config = AppConfig {
        host_id: "A".into(),
        bind: format!("127.0.0.1:{port}").parse().unwrap(),
        local_bind: None,
        root,
        roots: std::collections::BTreeMap::new(),
        peers: vec![],
        limits: Default::default(),
        exclude_globs: vec![],
        topology_cache_path: None,
        index_path: Some(temp.path().join("index.sqlite")),
        gptadmin_topology_url: None,
        gptadmin_token_env: None,
        peer_auth_token_env: None,
        backup_catalog: None,
        topology_ttl_ms: 30_000,
    };
    let path = temp.path().join("config.json");
    write_config(&path, &config);
    let mut child = spawn_server(&path);
    tokio::time::sleep(Duration::from_millis(400)).await;

    let tools = rpc(&url, "tools/list", serde_json::json!({})).await;
    let search_schema = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "search_text")
        .unwrap();
    assert_eq!(
        search_schema["inputSchema"]["properties"]["verbose"]["type"],
        "boolean"
    );
    assert_eq!(
        search_schema["inputSchema"]["properties"]["wait_ms"]["default"],
        30_000
    );

    let compact = rpc(
        &url,
        "tools/call",
        serde_json::json!({
            "name": "search_text",
            "arguments": {"query": "needle", "context_lines": 0}
        }),
    )
    .await;
    let compact: serde_json::Value =
        serde_json::from_str(compact["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(
        compact["results"],
        serde_json::json!([{
            "path": file.display().to_string(),
            "data": "needle one\nneedle two",
            "loc": "2-3"
        }])
    );
    assert!(compact.get("matches").is_none());

    let verbose = rpc(
        &url,
        "tools/call",
        serde_json::json!({
            "name": "search_text",
            "arguments": {"query": "needle", "context_lines": 0, "verbose": true}
        }),
    )
    .await;
    let verbose: serde_json::Value =
        serde_json::from_str(verbose["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(verbose["results"][0]["line_number"], 2);
    assert_eq!(verbose["matches"], verbose["results"]);

    let _ = child.kill();
    let _ = child.wait();
}
