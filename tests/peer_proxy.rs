use grepmesh::{
    backend::{LocalBackend, SearchMode},
    config::LimitsConfig,
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
                gptadmin_proxy_url: None,
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

    assert!(!result.partial, "{:?}", result.host_status);
    assert_eq!(result.data["results"][0]["host_id"], "B");
    peer.join().unwrap();
}

#[tokio::test]
async fn peer_dispatch_uses_gptadmin_connect_fallback_only_after_direct_connect_failure() {
    let proxy_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy_url = format!(
        "http://127.0.0.1:{}",
        proxy_listener.local_addr().unwrap().port()
    );
    let proxy = thread::spawn(move || {
        let (mut stream, _) = proxy_listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut request = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        assert!(String::from_utf8_lossy(&request).starts_with("CONNECT 127.0.0.1:1 HTTP/1.1"));
        stream
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .unwrap();
        request.clear();
        loop {
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let request = String::from_utf8_lossy(&request).to_ascii_lowercase();
        assert!(request.starts_with("post /mcp "));
        assert!(request.contains("authorization: bearer peer-test-token"));
        let content_length = request
            .lines()
            .find_map(|line| line.strip_prefix("content-length: "))
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let mut payload = vec![0; content_length];
        stream.read_exact(&mut payload).unwrap();
        let result_text = serde_json::json!({
            "request_id": "fallback", "origin_host": "A", "hop_count": 1,
            "host_id": "B", "partial": false, "truncated": false,
            "results": [], "host_status": [{"host_id": "B", "ok": true, "error": null}]
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
    let service = MeshService::new(
        LocalBackend::from_config(
            "A",
            root.path(),
            Default::default(),
            BTreeMap::new(),
            vec![],
            None,
        ),
        Topology::new(
            "A",
            vec![PeerConfig {
                host_id: "B".into(),
                local_url: "http://127.0.0.1:1/mcp".into(),
                routable_url: "http://127.0.0.1:1/mcp".into(),
                gptadmin_proxy_url: Some(proxy_url),
            }],
        ),
    )
    .with_peer_auth_token(Some("peer-test-token".into()));

    let result = service
        .call_search(SearchArgs {
            query: "fallback".into(),
            verbose: false,
            hosts: Some(HostsInput::One("B".into())),
            request_id: Some("gptadmin-fallback".into()),
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

    assert!(!result.partial, "{:?}", result.host_status);
    proxy.join().unwrap();
}

#[tokio::test]
async fn gptadmin_fallback_rejects_an_oversized_response_while_streaming() {
    let proxy_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy_url = format!(
        "http://127.0.0.1:{}",
        proxy_listener.local_addr().unwrap().port()
    );
    let proxy = thread::spawn(move || {
        let (mut stream, _) = proxy_listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        for _ in 0..2 {
            let mut request = Vec::new();
            loop {
                let mut byte = [0u8; 1];
                stream.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            if request.starts_with(b"CONNECT") {
                stream
                    .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    .unwrap();
            }
        }

        let body = vec![b'x'; 128];
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(&body).unwrap();
        // Keep the connection open beyond the peer timeout. A bounded reader
        // must reject the received chunk rather than wait for EOF.
        thread::sleep(Duration::from_millis(250));
    });

    let limits = LimitsConfig {
        max_response_bytes: 64,
        peer_timeout_ms: 100,
        ..Default::default()
    };
    let root = tempfile::tempdir().unwrap();
    let service = MeshService::new(
        LocalBackend::from_config("A", root.path(), limits, BTreeMap::new(), vec![], None),
        Topology::new(
            "A",
            vec![PeerConfig {
                host_id: "B".into(),
                local_url: "http://127.0.0.1:1/mcp".into(),
                routable_url: "http://127.0.0.1:1/mcp".into(),
                gptadmin_proxy_url: Some(proxy_url),
            }],
        ),
    );

    let result = service
        .call_search(SearchArgs {
            query: "oversized fallback".into(),
            verbose: false,
            hosts: Some(HostsInput::One("B".into())),
            request_id: Some("gptadmin-fallback-oversized".into()),
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

    assert!(result.partial);
    assert!(
        result.host_status[0]
            .error
            .as_deref()
            .unwrap()
            .contains("remote response exceeds max_response_bytes (64)"),
        "{:?}",
        result.host_status
    );
    proxy.join().unwrap();
}

#[tokio::test]
async fn unreachable_peer_reports_missing_gptadmin_fallback_per_host() {
    let root = tempfile::tempdir().unwrap();
    let service = MeshService::new(
        LocalBackend::from_config(
            "A",
            root.path(),
            Default::default(),
            BTreeMap::new(),
            vec![],
            None,
        ),
        Topology::new(
            "A",
            vec![PeerConfig {
                host_id: "B".into(),
                local_url: "http://127.0.0.1:1/mcp".into(),
                routable_url: "http://127.0.0.1:1/mcp".into(),
                gptadmin_proxy_url: None,
            }],
        ),
    );

    let result = service
        .call_search(SearchArgs {
            query: "unreachable".into(),
            verbose: false,
            hosts: Some(HostsInput::One("B".into())),
            request_id: Some("missing-gptadmin-fallback".into()),
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

    assert!(result.partial);
    assert_eq!(
        result.host_status[0].error.as_deref(),
        Some("direct transport unavailable; no GPTAdmin fallback is configured")
    );
}

#[test]
fn topology_refresh_retains_only_configured_loopback_fallback() {
    let root = tempfile::tempdir().unwrap();
    let service = MeshService::new(
        LocalBackend::from_config(
            "A",
            root.path(),
            Default::default(),
            BTreeMap::new(),
            vec![],
            None,
        ),
        Topology::new(
            "A",
            vec![PeerConfig {
                host_id: "B".into(),
                local_url: "http://127.0.0.1:9419/mcp".into(),
                routable_url: "http://192.0.2.10:9419/mcp".into(),
                gptadmin_proxy_url: Some("http://127.0.0.1:3126".into()),
            }],
        ),
    );

    service.replace_topology(Topology::new(
        "A",
        vec![PeerConfig {
            host_id: "B".into(),
            local_url: "http://127.0.0.1:9419/mcp".into(),
            routable_url: "http://192.0.2.11:9419/mcp".into(),
            gptadmin_proxy_url: None,
        }],
    ));

    let peer = service.topology.read().unwrap().peer("B").unwrap().clone();
    assert_eq!(peer.routable_url, "http://192.0.2.11:9419/mcp");
    assert_eq!(
        peer.gptadmin_proxy_url.as_deref(),
        Some("http://127.0.0.1:3126")
    );
}
