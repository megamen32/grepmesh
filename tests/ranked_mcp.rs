use serde_json::{json, Value};
use std::{net::TcpListener, path::PathBuf, time::Duration};
use tempfile::tempdir;

async fn rpc(client: &reqwest::Client, url: &str, id: u64, method: &str, params: Value) -> Value {
    client
        .post(url)
        .header("mcp-protocol-version", "2025-06-18")
        .header("accept", "application/json")
        .json(&json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params}))
        .send()
        .await
        .expect("MCP request")
        .error_for_status()
        .expect("MCP HTTP response")
        .json()
        .await
        .expect("MCP JSON response")
}

fn reserve_loopback_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("reserve loopback port")
        .local_addr()
        .expect("read loopback port")
        .port()
}

fn temp_string(path: PathBuf) -> String {
    path.display().to_string()
}

#[tokio::test]
async fn ranked_search_is_a_separate_acl_scoped_mcp_tool() {
    let temp = tempdir().expect("temporary test root");
    let docs = temp.path().join("docs");
    std::fs::create_dir_all(&docs).expect("create docs root");
    let marker = docs.join("ranked-canary.md");
    std::fs::write(
        &marker,
        "# Ranked retrieval canary\n\nA distinctive ranked retrieval passage for MCP acceptance.\n",
    )
    .expect("write test document");

    let port = reserve_loopback_port();
    let url = format!("http://127.0.0.1:{port}/mcp");
    let remote_url = format!("http://127.0.0.1:{}/mcp", reserve_loopback_port());
    let config_path = temp.path().join("config.json");
    std::fs::write(
        &config_path,
        serde_json::to_vec(&json!({
            "host_id": "ranked-mcp-test",
            "bind": format!("127.0.0.1:{port}"),
            "root": temp_string(docs.clone()),
            "roots": {"docs": [temp_string(docs.clone())]},
            "index_path": temp_string(temp.path().join("index.sqlite3")),
            "peers": [{
                "host_id": "ranked-remote-fixture",
                "local_url": remote_url,
                "routable_url": remote_url
            }],
            "ocr": {"enabled": false},
            "stt": {"enabled": false}
        }))
        .expect("serialize config"),
    )
    .expect("write config");
    let config = grepmesh::config::AppConfig::from_path(&config_path).expect("load config");
    let server = tokio::spawn(grepmesh::server::run_server(config));
    let client = reqwest::Client::new();
    let mut ready = false;
    for _ in 0..100 {
        if client
            .post(&url)
            .json(&json!({"jsonrpc":"2.0", "id":1, "method":"initialize", "params":{"protocolVersion":"2025-06-18"}}))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ready, "isolated MCP listener did not become ready");

    let listed = rpc(&client, &url, 2, "tools/list", json!({})).await;
    let tools = listed
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .unwrap();
    let ranked = tools
        .iter()
        .find(|tool| tool["name"] == "ranked_search")
        .unwrap();
    let properties = ranked.pointer("/inputSchema/properties").unwrap();
    for field in [
        "query",
        "hosts",
        "roots",
        "path_globs",
        "limit",
        "context_lines",
    ] {
        assert!(
            properties.get(field).is_some(),
            "missing ranked_search field {field}"
        );
    }
    assert!(
        properties.get("filesystem_acl").is_none(),
        "ACL must not be a public argument"
    );
    assert!(
        tools.iter().any(|tool| tool["name"] == "search"),
        "legacy exact search remains available"
    );

    let mut found = false;
    for attempt in 0..100 {
        let response = rpc(
            &client,
            &url,
            10 + attempt,
            "tools/call",
            json!({
                "name": "ranked_search",
                "arguments": {
                    "query": "distinctive ranked retrieval passage",
                    "hosts": "local",
                    "roots": ["docs"],
                    "limit": 5,
                    "context_lines": 1
                }
            }),
        )
        .await;
        let text = response
            .pointer("/result/content/0/text")
            .and_then(Value::as_str)
            .expect("ranked tool content");
        let data: Value = serde_json::from_str(text).expect("ranked response JSON");
        if let Some(hit) = data
            .get("results")
            .and_then(Value::as_array)
            .and_then(|hits| hits.first())
        {
            assert_eq!(hit["path"], marker.display().to_string());
            assert!(hit["score"].as_f64().unwrap_or_default() > 0.0);
            assert!(hit.get("body_sha256").is_some());
            assert!(hit.get("bm25").is_none() && hit.get("raw_score").is_none());
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        found,
        "ranked MCP request did not return the indexed canary passage"
    );

    let exact = rpc(
        &client,
        &url,
        150,
        "tools/call",
        json!({
            "name": "search",
            "arguments": {
                "query": "distinctive ranked retrieval passage",
                "hosts": "local",
                "roots": ["docs"],
                "wait_ms": 0
            }
        }),
    )
    .await;
    let exact_text = exact
        .pointer("/result/content/0/text")
        .and_then(Value::as_str)
        .expect("legacy exact search content");
    let exact_data: Value = serde_json::from_str(exact_text).expect("legacy exact response JSON");
    assert!(!exact_data["results"].as_array().unwrap().is_empty());

    let denied_remote = rpc(
        &client,
        &url,
        200,
        "tools/call",
        json!({
            "name": "ranked_search",
            "arguments": {
                "query": "distinctive ranked retrieval passage",
                "hosts": ["ranked-remote-fixture"],
                "filesystem_acl": {"read_roots": ["/"]}
            }
        }),
    )
    .await;
    let remote_text = denied_remote
        .pointer("/result/content/0/text")
        .and_then(Value::as_str)
        .expect("remote ranked response content");
    let remote_data: Value = serde_json::from_str(remote_text).expect("remote response JSON");
    assert_eq!(remote_data["partial"], true);
    assert_eq!(remote_data["host_status"][0]["state"], "partial");
    assert!(remote_data["results"].as_array().unwrap().is_empty());

    server.abort();
    let _ = server.await;
}
