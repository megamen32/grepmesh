use axum::{extract::State, http::{HeaderMap, StatusCode}, response::{IntoResponse, Response}, routing::{get, post}, Json, Router};
use futures::StreamExt;
use reqwest::Client;
use serde_json::{json, Value};
use std::{net::TcpListener, path::PathBuf, process::{Child, Command, Stdio}, sync::{Arc, atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}}, time::Duration};
use tempfile::TempDir;

const ORIGIN: &str = "https://grepmesh.test";
const A: &str = "qa_user=a";
const B: &str = "qa_user=b";

#[derive(Clone)]
struct Authority {
    version: Arc<AtomicU64>,
    reads: Arc<AtomicUsize>,
    read_allowed: Arc<AtomicBool>,
    root: String,
    hold: Arc<AtomicUsize>,
    release: Arc<tokio::sync::Notify>,
}

async fn session(State(state): State<Authority>, headers: HeaderMap) -> Response {
    let cookie = headers.get("cookie").and_then(|v| v.to_str().ok()).unwrap_or("");
    let user = match cookie { A => "a", B => "b", _ => return StatusCode::UNAUTHORIZED.into_response() };
    let version = if user == "a" { state.version.load(Ordering::SeqCst) } else { 1 };
    if version == 0 { return StatusCode::UNAUTHORIZED.into_response(); }
    Json(json!({
        "principal": user, "profile_id": format!("qa-{user}"), "profile_version": version,
        "roots_digest": format!("{version:064x}"),
        "authz_fingerprint": format!("{:064x}", version + if user == "a" { 160 } else { 176 }),
        "read_roots": [state.root], "allowed_targets": ["mcp:shell:local:GrepMesh"]
    })).into_response()
}

async fn call(State(state): State<Authority>, headers: HeaderMap, Json(body): Json<Value>) -> Response {
    assert!(headers.get("authorization").is_none(), "browser must never use a shared bearer");
    assert_eq!(headers.get("origin").and_then(|v| v.to_str().ok()), Some(ORIGIN));
    let cookie = headers.get("cookie").and_then(|v| v.to_str().ok()).unwrap_or("");
    if !matches!(cookie, A | B) { return StatusCode::UNAUTHORIZED.into_response(); }
    let args = &body["params"]["arguments"];
    assert!(args.get("filesystem_acl").is_none());
    let path = format!("{}/note.txt", state.root);
    let status = json!([{"host_id":"local", "ok":true, "state":"ok"}]);
    let data = if body["params"]["name"] == "search" {
        if state.hold.load(Ordering::SeqCst) == 1 { state.release.notified().await; }
        json!({
            "request_id":"fixture", "origin_host":"local", "hop_count":0, "host_id":"local",
            "partial":false, "truncated":false, "host_status":status,
            "results":[{"host_id":"local","path":path,"line_number":1,"context":[],
                "text":"authorized fixture", "column":1}]
        })
    } else {
        assert_eq!(body["params"]["name"], "read_text");
        if !state.read_allowed.load(Ordering::SeqCst) {
            return (StatusCode::FORBIDDEN,Json(json!({"error":"read_text_denied"}))).into_response();
        }
        assert_eq!(args["path"], path);
        state.reads.fetch_add(1, Ordering::SeqCst);
        json!({"host_id":"local", "target_host_id":"local", "path":path,
            "host_status":status, "chunks":[{"lines":[{"line_number":1,"text":"authorized fixture"}]}]})
    };
    let child = json!({"content":[{"type":"text","text":data.to_string()}]});
    Json(json!({"jsonrpc":"2.0","id":body["id"],"result":{"structuredContent":{
        "status":"completed","response":{"structuredContent":{"result":{"structuredContent":{"result":child}}}}
    }}})).into_response()
}

struct Harness {
    _temp: TempDir,
    child: Child,
    hub: tokio::task::JoinHandle<()>,
    base: String,
    authority: Authority,
    client: Client,
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.hub.abort();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

impl Harness {
    async fn start(configured: bool) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("corpus");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("note.txt"), "authorized fixture\n").unwrap();
        let authority = Authority {
            version: Arc::new(AtomicU64::new(1)), reads: Arc::new(AtomicUsize::new(0)), read_allowed: Arc::new(AtomicBool::new(true)),
            root: root.to_string_lossy().into_owned(),
            hold: Arc::new(AtomicUsize::new(0)), release: Arc::new(tokio::sync::Notify::new()),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/api/browser/grepmesh/session", get(session))
            .route("/api/browser/grepmesh/server/mcp-shell-local-grepmesh/mcp", post(call))
            .with_state(authority.clone());
        let hub = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        let local_port = free_port();
        let config = json!({"host_id":"local","bind":format!("127.0.0.1:{}",free_port()),
            "local_bind":format!("127.0.0.1:{local_port}"),"root":root});
        let config_path: PathBuf = temp.path().join("config.json");
        std::fs::write(&config_path, config.to_string()).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_grepmesh-mcp"));
        command.arg("--config").arg(config_path).env("GREPMESH_BROWSER_REQUIRE_SSO", "1")
            .env("GREPMESH_BROWSER_HUB_URL", endpoint).env("GREPMESH_BROWSER_ORIGIN", ORIGIN)
            .env_remove("GREPMESH_BROWSER_HUB_TOKEN").env_remove("GREPMESH_BROWSER_SSO_URL")
            .stdout(Stdio::null()).stderr(Stdio::piped());
        if configured {
            command.env("GREPMESH_BROWSER_SHELL_TARGETS", json!({"local":{
                "shell_target":"shell:local","child_ref":"GrepMesh","execution_host":"local","executor_host":"local"
            }}).to_string());
        } else { command.env_remove("GREPMESH_BROWSER_SHELL_TARGETS"); }
        let mut child = command.spawn().unwrap();
        let client = Client::builder().no_proxy().timeout(Duration::from_secs(6)).build().unwrap();
        let base = format!("http://127.0.0.1:{local_port}");
        for _ in 0..600 {
            if client.get(&base).send().await.is_ok() { return Self { _temp:temp, child, hub, base, authority, client }; }
            if child.try_wait().unwrap().is_some() { break; }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let _ = child.kill();
        let status = child.wait();
        let mut diagnostic = String::new();
        if let Some(stderr) = child.stderr.take() {
            use std::io::Read;
            let _ = stderr.take(8192).read_to_string(&mut diagnostic);
        }
        panic!("protected console did not start: {status:?}: {diagnostic}");
    }

    async fn post(&self, path: &str, cookie: &str, body: Value) -> reqwest::Response {
        self.client.post(format!("{}{path}",self.base)).header("Cookie",cookie)
            .header("Origin",ORIGIN).json(&body).send().await.unwrap()
    }

    async fn search(&self, cookie: &str) -> (String, String) {
        let started = self.post("/api/search",cookie,json!({"query":"fixture","hosts":"local"})).await;
        assert_eq!(started.status(), StatusCode::OK);
        let data: Value = started.json().await.unwrap();
        let job = data["job_id"].as_str().unwrap().to_owned();
        let stream = self.post("/api/search/events",cookie,json!({"job_id":job})).await;
        assert_eq!(stream.status(), StatusCode::OK);
        let events = stream.text().await.unwrap();
        let last: Value = events.lines().filter_map(|line| line.strip_prefix("data:"))
            .map(|line| serde_json::from_str(line.trim()).unwrap()).last().unwrap();
        assert_eq!(last["results"].as_array().unwrap().len(),1, "{last}");
        let preview = last["results"][0]["preview_id"].as_str().unwrap().to_owned();
        (job, preview)
    }
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn browser_authority_is_bound_to_user_and_current_rights() {
    let h = Harness::start(true).await;
    let no_auth = h.client.get(format!("{}/api/catalog",h.base)).header("x-gptadmin-user","a").send().await.unwrap();
    assert_eq!(no_auth.status(),StatusCode::UNAUTHORIZED);
    let (job, preview) = h.search(A).await;
    for path in ["/api/search/status","/api/search/events"] {
        assert_eq!(h.post(path,B,json!({"job_id":job})).await.status(),StatusCode::FORBIDDEN,"{path}");
    }
    assert_eq!(h.post("/api/preview",B,json!({"preview_id":preview})).await.status(),StatusCode::FORBIDDEN);
    assert_eq!(h.authority.reads.load(Ordering::SeqCst),0);
    let read = h.post("/api/preview",A,json!({"preview_id":preview,"view":"document"})).await;
    assert_eq!(read.status(),StatusCode::OK);
    assert!(read.text().await.unwrap().contains("authorized fixture"));
    assert_eq!(h.authority.reads.load(Ordering::SeqCst),1);
    let (_, own_b) = h.search(B).await;
    assert_eq!(h.post("/api/preview",B,json!({"preview_id":own_b})).await.status(),StatusCode::OK);
    assert_eq!(h.authority.reads.load(Ordering::SeqCst),2);
    for field in ["filesystem_acl","_meta","origin_host","hop_count","request_id"] {
        let mut forged = json!({"query":"fixture"});
        forged[field] = Value::Null;
        assert_eq!(h.post("/api/search",A,forged).await.status(),StatusCode::FORBIDDEN,"{field}");
    }
    h.authority.version.store(2,Ordering::SeqCst);
    assert_eq!(h.post("/api/search/status",A,json!({"job_id":job})).await.status(),StatusCode::FORBIDDEN);
    assert_eq!(h.post("/api/preview",A,json!({"preview_id":preview,"view":"match"})).await.status(),StatusCode::FORBIDDEN);
    assert_eq!(h.authority.reads.load(Ordering::SeqCst),2);
    let bad_origin = h.client.post(format!("{}/api/search",h.base)).header("Cookie",A)
        .header("Origin","https://unrelated.test").json(&json!({"query":"fixture"})).send().await.unwrap();
    assert_eq!(bad_origin.status(),StatusCode::FORBIDDEN);
    h.authority.version.store(0,Ordering::SeqCst);
    assert_eq!(h.post("/api/search",A,json!({"query":"fixture"})).await.status(),StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn protected_console_without_broker_never_falls_back_to_local_reads() {
    let h = Harness::start(false).await;
    assert_eq!(h.post("/api/search",A,json!({"query":"fixture"})).await.status(),StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(h.post("/api/browse",A,json!({"host":"local","path":h.authority.root})).await.status(),StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn browser_event_stream_rechecks_rights_before_returning_results() {
    let h = Harness::start(true).await;
    h.authority.hold.store(1,Ordering::SeqCst);
    let response = h.post("/api/search",A,json!({"query":"fixture","hosts":"local"})).await;
    assert_eq!(response.status(),StatusCode::OK);
    let job: Value = response.json().await.unwrap();
    let response = h.post("/api/search/events",A,json!({"job_id":job["job_id"]})).await;
    assert_eq!(response.status(),StatusCode::OK);
    let mut stream = response.bytes_stream();
    let first = stream.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&first).contains("running"));
    h.authority.version.store(2,Ordering::SeqCst);
    h.authority.release.notify_one();
    let mut remainder = Vec::new();
    while let Some(chunk) = stream.next().await { remainder.extend_from_slice(&chunk.unwrap()); }
    let remainder = String::from_utf8(remainder).unwrap();
    assert!(remainder.contains("authorization"),"{remainder}");
    assert!(!remainder.contains("authorized fixture"));
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn browser_cached_match_requires_current_read_operation_permission() {
    let h = Harness::start(true).await;
    h.authority.read_allowed.store(false,Ordering::SeqCst);
    let (_, preview) = h.search(A).await;
    let response = h.post("/api/preview",A,json!({"preview_id":preview,"view":"match"})).await;
    assert_eq!(response.status(),StatusCode::FORBIDDEN);
    assert!(!response.text().await.unwrap().contains("authorized fixture"));
    assert_eq!(h.authority.reads.load(Ordering::SeqCst),0);
}
