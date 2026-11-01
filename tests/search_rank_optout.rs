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
async fn search_ranks_by_default_and_has_no_ranked_alias() {
    let temp = tempdir().expect("temporary test root");
    let docs = temp.path().join("docs");
    std::fs::create_dir_all(&docs).expect("create docs root");
    let marker = docs.join("ranked-canary.md");
    std::fs::write(
        &marker,
        "# Ranked retrieval canary\n\nA distinctive ranked retrieval passage for MCP acceptance.\n",
    )
    .expect("write test document");

    let reference = docs.join("aaa-mention.txt");
    std::fs::write(&reference, "A distinctive ranked retrieval passage reference.\n").unwrap();

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
    let tools = listed.pointer("/result/tools").and_then(Value::as_array).unwrap();
    assert!(!tools.iter().any(|tool| tool["name"] == "ranked_search"));
    let search = tools.iter().find(|tool| tool["name"] == "search").unwrap();
    assert_eq!(search.pointer("/inputSchema/properties/rank/default"), Some(&json!(true)));
    assert!(search.pointer("/inputSchema/properties/filesystem_acl").is_none());
    let query = "distinctive ranked retrieval passage";
    let mut baseline = Value::Null;
    for attempt in 0..120 {
        let response = rpc(&client, &url, 10 + attempt, "tools/call", json!({
            "name": "search", "arguments": {"query": query, "hosts": "local",
                "roots": ["docs"], "limit": 20, "context_lines": 2,
                "verbose": true, "wait_ms": 0}
        })).await;
        let text = response.pointer("/result/content/0/text").and_then(Value::as_str).unwrap();
        let data: Value = serde_json::from_str(text).unwrap();
        if data["results"].as_array().is_some_and(|hits| hits.len() == 2) {
            baseline = data; break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let ranked = baseline["results"].as_array().expect("both indexed fixtures must be found");
    assert_eq!(ranked.len(), 2);
    assert_eq!(ranked[0]["path"], marker.display().to_string());
    assert!(ranked[0]["score"].as_f64().unwrap() > ranked[1]["score"].as_f64().unwrap());
    for (id, extra) in [
        (200, json!({"rank": false, "verbose": true, "limit": 20})),
        (201, json!({"verbose": false, "limit": 20})),
        (202, json!({"verbose": true, "limit": 1})),
        (203, json!({"mode": "regex", "query": "distinctive.*passage", "verbose": true, "limit": 20}))
    ] {
        let mut args = json!({"query":query,"hosts":"local","roots":["docs"],"wait_ms":0});
        args.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        let response = rpc(&client, &url, id, "tools/call", json!({"name":"search","arguments":args})).await;
        let data:Value=serde_json::from_str(response.pointer("/result/content/0/text").and_then(Value::as_str).unwrap()).unwrap();
        let hits=data["results"].as_array().unwrap();
        match id {
            200 => {
                assert_eq!(hits.len(),2);
                assert_eq!(hits[0]["path"],reference.display().to_string());
                assert!(hits.iter().all(|hit| hit.get("score").is_none()));
                let paths=|items:&Vec<Value>| items.iter().map(|hit| hit["path"].as_str().unwrap().to_string()).collect::<std::collections::BTreeSet<_>>();
                assert_eq!(paths(hits),paths(ranked),"rank=false must not change the match set");
            }
            201 => {
                assert_eq!(hits[0]["path"],marker.display().to_string());
                assert!(hits[0].get("loc").is_some());
                assert!(hits[0].get("score").is_none(), "compact output stays lean; verbose carries scores");
            }
            202 => {
                assert_eq!(hits.len(),1);
                assert_eq!(hits[0]["path"],marker.display().to_string(),"rank before top-k, not after");
                assert_eq!(data["truncated"],true);
            }
            203 => assert_eq!(hits.len(),2,"regex matching must not become term retrieval"),
            _ => unreachable!(),
        }
    }
    let removed=rpc(&client,&url,210,"tools/call",json!({"name":"ranked_search","arguments":{"query":query,"hosts":"local"}})).await;
    assert!(removed.get("error").is_some() || removed.pointer("/result/isError")==Some(&json!(true)), "removed tool must not be an alias: {removed}");
    let denied=rpc(&client,&url,211,"tools/call",json!({"name":"search","arguments":{"query":query,"hosts":["ranked-remote-fixture"],"wait_ms":0,"filesystem_acl":{"verified":true,"read_roots":["/"]}}})).await;
    // Caller-supplied ACL is stripped, not trusted. Local configured roots are
    // valid without it; a remote hop must remain denied rather than attempted.
    let denied_data:Value=serde_json::from_str(denied.pointer("/result/content/0/text").and_then(Value::as_str).unwrap()).unwrap();
    assert_eq!(denied_data["partial"],true);
    assert!(denied_data["results"].as_array().unwrap().is_empty());
    assert!(denied_data["host_status"].to_string().contains("verified GPTAdmin filesystem ACL"));
    server.abort();
}
