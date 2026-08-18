use grepmesh::{
    backend::{LocalBackend, SearchMode},
    mcp::{HostsInput, MeshService, SearchArgs},
    topology::{PeerConfig, Topology},
};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    io::{Read, Write},
    net::TcpListener,
    thread,
    time::Duration,
};

struct ProxyEnvironmentGuard {
    previous: Vec<(&'static str, Option<OsString>)>,
}

impl ProxyEnvironmentGuard {
    fn poison(proxy_url: String) -> Self {
        let values = [
            ("HTTP_PROXY", Some(proxy_url)),
            ("http_proxy", None),
            ("HTTPS_PROXY", None),
            ("https_proxy", None),
            ("ALL_PROXY", None),
            ("all_proxy", None),
            ("NO_PROXY", None),
            ("no_proxy", None),
        ];
        let previous = values
            .iter()
            .map(|(key, value)| {
                let old = std::env::var_os(key);
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
                (*key, old)
            })
            .collect();
        Self { previous }
    }
}

impl Drop for ProxyEnvironmentGuard {
    fn drop(&mut self) {
        for (key, value) in self.previous.drain(..) {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

#[tokio::test]
async fn peer_dispatch_bypasses_a_poisoned_proxy_and_keeps_bearer_auth() {
    let peer_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer_url = format!(
        "http://127.0.0.1:{}/mcp",
        peer_listener.local_addr().unwrap().port()
    );
    let poison_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let poison_url = format!(
        "http://127.0.0.1:{}",
        poison_listener.local_addr().unwrap().port()
    );
    drop(poison_listener);
    let _proxy_environment = ProxyEnvironmentGuard::poison(poison_url);

    let peer = thread::spawn(move || {
        let (mut stream, _) = peer_listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut request = [0u8; 4096];
        let read = stream.read(&mut request).unwrap();
        let request = String::from_utf8_lossy(&request[..read]).to_ascii_lowercase();
        assert!(request.starts_with("post /mcp "));
        assert!(request.contains("authorization: bearer peer-test-token"));
        assert!(request.contains("mcp-protocol-version: 2026-07-28"));

        let result_text = serde_json::json!({
            "request_id": "peer-proxy", "origin_host": "A", "hop_count": 1,
            "host_id": "B", "partial": false, "truncated": false,
            "results": [{
                "host_id": "B", "path": "/peer-canary.txt", "line_number": 1,
                "column": 1, "text": "peer proxy canary", "context": []
            }],
            "host_status": [{"host_id": "B", "ok": true, "error": null}]
        })
        .to_string();
        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1,
            "result": {"content": [{"type": "text", "text": result_text}], "isError": false}
        })
        .to_string();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
    });

    let root = tempfile::tempdir().unwrap();
    let backend = LocalBackend::from_config(
        "A",
        root.path(),
        Default::default(),
        BTreeMap::new(),
        vec![],
        None,
    );
    let service = MeshService::new(
        backend,
        Topology::new(
            "A",
            vec![PeerConfig {
                host_id: "B".into(),
                local_url: "http://127.0.0.1:1/mcp".into(),
                routable_url: peer_url,
            }],
        ),
    )
    .with_peer_auth_token(Some("peer-test-token".into()));

    let result = service
        .call_search(SearchArgs {
            query: "peer proxy canary".into(),
            verbose: true,
            hosts: Some(HostsInput::One("B".into())),
            request_id: Some("peer-proxy-request".into()),
            origin_host: None,
            hop_count: None,
            limit: Some(10),
            context_lines: Some(0),
            mode: SearchMode::Literal,
            path_globs: vec![],
            roots: vec![],
        })
        .await
        .unwrap();

    assert!(!result.partial);
    assert_eq!(result.data["results"][0]["host_id"], "B");
    peer.join().unwrap();
}
