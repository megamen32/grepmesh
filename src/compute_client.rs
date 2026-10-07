use crate::{
    config::OcrConfig,
    topology::{PeerConfig, Topology},
    topology_cache::{CacheFreshness, TopologySnapshot},
};
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Read,
    path::Path,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const MAX_INPUT_BYTES: u64 = 32 * 1024 * 1024;
const MAX_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;
const MIN_FREE_GPU_BYTES: u64 = 5 * 1024 * 1024 * 1024 / 2;
const STATUS_TTL: Duration = Duration::from_secs(10);
const FAILED_PEER_COOLDOWN: Duration = Duration::from_secs(60);
const MAX_PEERS: usize = 8;

#[derive(Clone, Debug, Deserialize)]
struct ComputeStatus {
    host_id: String,
    backend: String,
    #[serde(default)]
    gpu_uuid: Option<String>,
    #[serde(default)]
    gpu_name: Option<String>,
    #[serde(default)]
    compute_capability: Option<String>,
    #[serde(default)]
    gpu_free_bytes: Option<u64>,
    #[serde(default)]
    gpu_utilization_percent: Option<u32>,
    busy_count: usize,
    admission: String,
    observed_at: u64,
    #[serde(default)]
    performance_score: Option<f64>,
}

impl ComputeStatus {
    fn fresh_at(&self, now: u64) -> bool {
        self.observed_at <= now.saturating_add(5)
            && now.saturating_sub(self.observed_at) <= STATUS_TTL.as_secs()
    }

    fn eligible(&self, host: &str, now: u64) -> bool {
        self.host_id == host
            && self.backend == "cuda"
            && self.admission == "available"
            && self.busy_count == 0
            && self.gpu_uuid.as_ref().is_some_and(|id| !id.is_empty())
            && self.gpu_free_bytes.unwrap_or(0) >= MIN_FREE_GPU_BYTES
            && self.fresh_at(now)
    }

    fn performance(&self) -> f64 {
        if let Some(score) = self.performance_score.filter(|s| s.is_finite() && *s > 0.0) {
            return score;
        }
        // Coarse fallback only; live worker scores take precedence.
        let name = self.gpu_name.as_deref().unwrap_or_default().to_lowercase();
        for (model, score) in [
            ("5090", 500.0),
            ("4090", 400.0),
            ("3090", 310.0),
            ("3080 ti", 300.0),
            ("3080ti", 300.0),
            ("3080", 280.0),
        ] {
            if name.contains(model) {
                return score;
            }
        }
        self.compute_capability
            .as_deref()
            .and_then(|c| c.parse::<f64>().ok())
            .filter(|s| s.is_finite())
            .unwrap_or(0.0)
    }
}

/// Synchronous client: indexing already serializes OCR, so no client queues or
/// extra inference/preprocessing pools are created for mesh mode.
pub struct ComputeClient {
    config: OcrConfig,
    agent: ureq::Agent,
    statuses: BTreeMap<String, (Instant, ComputeStatus)>,
    failures: BTreeMap<String, Instant>,
}

impl ComputeClient {
    pub fn new(config: OcrConfig) -> Self {
        Self {
            config,
            agent: ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(2))
                .redirects(0)
                .build(),
            statuses: BTreeMap::new(),
            failures: BTreeMap::new(),
        }
    }

    fn peers(&self) -> Vec<PeerConfig> {
        let now = unix_seconds().saturating_mul(1000);
        let snapshot = self
            .config
            .mesh_topology_cache_path
            .as_deref()
            .and_then(|path| TopologySnapshot::load(path).ok());
        let topology = snapshot
            .filter(|snapshot| {
                matches!(
                    snapshot.freshness_at(now),
                    CacheFreshness::Fresh | CacheFreshness::StaleButUsable
                )
            })
            .and_then(|snapshot| Topology::from_snapshot(snapshot, now).ok())
            .unwrap_or_else(|| Topology::new("", Vec::new()));
        merge_compute_peers(topology, &self.config.mesh_peers)
    }

    pub fn extract(&mut self, path: &Path, input_type: &str) -> Result<String> {
        if !matches!(
            input_type,
            "png" | "jpg" | "jpeg" | "webp" | "tif" | "tiff" | "bmp" | "gif" | "avif" | "pdf"
        ) {
            return Err(anyhow!("unsupported mesh OCR input type"));
        }
        let deadline =
            Instant::now() + Duration::from_millis(self.config.remote_timeout_ms.clamp(1, 90_000));
        let peers = self.peers();
        let keys: Vec<_> = peers.iter().map(peer_key).collect();
        self.statuses
            .retain(|key, (at, _)| keys.contains(key) && at.elapsed() < STATUS_TTL);
        self.failures
            .retain(|key, at| keys.contains(key) && at.elapsed() < FAILED_PEER_COOLDOWN);
        let mut candidates = Vec::new();
        for peer in peers {
            if Instant::now() >= deadline {
                break;
            }
            let key = peer_key(&peer);
            if self.cooling_down(&key, Instant::now()) {
                continue;
            }
            let status = self
                .statuses
                .get(&key)
                .filter(|(_, status)| status.fresh_at(unix_seconds()))
                .map(|(_, status)| status.clone())
                .or_else(|| {
                    let probe_deadline = deadline.min(Instant::now() + Duration::from_secs(3));
                    let value = match self.call(&peer, "compute_status", json!({}), probe_deadline)
                    {
                        Ok(value) => value,
                        Err(_) => {
                            self.mark_failed(&peer);
                            return None;
                        }
                    };
                    let status: ComputeStatus = match serde_json::from_value(value) {
                        Ok(status) => status,
                        Err(_) => {
                            self.mark_failed(&peer);
                            return None;
                        }
                    };
                    self.statuses
                        .insert(key.clone(), (Instant::now(), status.clone()));
                    Some(status)
                });
            if status.as_ref().is_some_and(|status| {
                status.host_id != peer.host_id
                    || status.backend != "cuda"
                    || matches!(status.admission.as_str(), "ineligible" | "unavailable")
                    || !status.fresh_at(unix_seconds())
            }) {
                self.mark_failed(&peer);
                continue;
            }
            if let Some(status) =
                status.filter(|status| status.eligible(&peer.host_id, unix_seconds()))
            {
                candidates.push((peer, status));
            }
        }
        candidates.sort_by(|(_, a), (_, b)| {
            b.performance()
                .total_cmp(&a.performance())
                .then_with(|| b.gpu_free_bytes.cmp(&a.gpu_free_bytes))
                .then_with(|| {
                    a.gpu_utilization_percent
                        .unwrap_or(100)
                        .cmp(&b.gpu_utilization_percent.unwrap_or(100))
                })
                .then_with(|| a.host_id.cmp(&b.host_id))
        });
        if candidates.is_empty() || Instant::now() >= deadline {
            return Err(anyhow!("mesh OCR deferred: no available GPU worker"));
        }
        // GPU admission is known before opening potentially large inputs.
        let ceiling = mesh_input_limit(self.config.max_image_bytes);
        let mut bytes = Vec::new();
        File::open(path)
            .context("open mesh OCR input")?
            .take(ceiling + 1)
            .read_to_end(&mut bytes)
            .context("read mesh OCR input")?;
        if bytes.is_empty() || bytes.len() as u64 > ceiling {
            return Err(anyhow!("mesh OCR input is empty or exceeds its byte limit"));
        }
        let arguments = json!({"content_base64": STANDARD.encode(bytes), "input_type": input_type});
        for (peer, mut status) in candidates {
            if Instant::now() >= deadline {
                break;
            }
            if !status.fresh_at(unix_seconds()) {
                // A preceding extraction may consume longer than the status
                // TTL. Refresh admission so a still available fallback is not
                // silently discarded after the first worker fails.
                let refresh_deadline = deadline.min(Instant::now() + Duration::from_secs(3));
                let refreshed = self
                    .call(&peer, "compute_status", json!({}), refresh_deadline)
                    .ok()
                    .and_then(|value| serde_json::from_value::<ComputeStatus>(value).ok());
                let Some(refreshed) = refreshed else {
                    self.mark_failed(&peer);
                    continue;
                };
                status = refreshed;
            }
            if !status.eligible(&peer.host_id, unix_seconds()) {
                self.mark_failed(&peer);
                continue;
            }
            self.statuses
                .insert(peer_key(&peer), (Instant::now(), status.clone()));
            // Leave budget for another eligible candidate after a failed worker.
            let attempt_deadline = deadline.min(Instant::now() + Duration::from_secs(45));
            let result = self.call(&peer, "ocr_extract", arguments.clone(), attempt_deadline);
            let Ok(result) = result else {
                self.mark_failed(&peer);
                continue;
            };
            if result.get("host_id").and_then(Value::as_str) != Some(peer.host_id.as_str())
                || result.get("backend").and_then(Value::as_str) != Some("cuda")
                || result.get("gpu_uuid").and_then(Value::as_str) != status.gpu_uuid.as_deref()
            {
                self.mark_failed(&peer);
                continue;
            }
            if let Some(text) = result.get("text").and_then(Value::as_str) {
                if text.len() <= 4 * 1024 * 1024 {
                    return Ok(text.to_owned());
                }
            }
            self.mark_failed(&peer);
        }
        Err(anyhow!(
            "mesh OCR deferred: no available GPU worker completed extraction"
        ))
    }

    fn mark_failed(&mut self, peer: &PeerConfig) {
        let key = peer_key(peer);
        self.statuses.remove(&key);
        self.failures.insert(key, Instant::now());
    }

    fn cooling_down(&self, key: &str, now: Instant) -> bool {
        self.failures.get(key).is_some_and(|failed_at| {
            now.saturating_duration_since(*failed_at) < FAILED_PEER_COOLDOWN
        })
    }

    fn call(&self, peer: &PeerConfig, tool: &str, args: Value, deadline: Instant) -> Result<Value> {
        let payload = serde_json::to_string(
            &json!({"jsonrpc":"2.0", "id":1, "method":"tools/call", "params":{"name":tool,"arguments":args}}),
        )?;
        let relay_only = peer.gptadmin_relay_url.as_deref() == Some(peer.routable_url.as_str());
        let direct = self.request(&peer.routable_url, relay_only, tool, &payload, deadline);
        match direct {
            Ok(value) => Ok(value),
            Err((true, _)) if !relay_only => {
                let relay = peer
                    .gptadmin_relay_url
                    .as_deref()
                    .ok_or_else(|| anyhow!("mesh OCR direct transport unavailable"))?;
                self.request(relay, true, tool, &payload, deadline)
                    .map_err(|(_, err)| err)
            }
            Err((_, err)) => Err(err),
        }
    }

    fn request(
        &self,
        url: &str,
        relay: bool,
        tool: &str,
        payload: &str,
        deadline: Instant,
    ) -> std::result::Result<Value, (bool, anyhow::Error)> {
        let inner = || -> Result<Value> {
            let parsed = reqwest::Url::parse(url).map_err(|_| anyhow!("invalid OCR peer URL"))?;
            if !matches!(parsed.scheme(), "http" | "https")
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.query().is_some()
                || parsed.fragment().is_some()
            {
                return Err(anyhow!("unsafe OCR peer URL"));
            }
            if relay {
                let origin = self
                    .config
                    .mesh_relay_origin
                    .as_deref()
                    .and_then(|url| reqwest::Url::parse(url).ok())
                    .ok_or_else(|| anyhow!("OCR relay origin is not configured"))?;
                if parsed.origin() != origin.origin() {
                    return Err(anyhow!("OCR relay origin is not trusted"));
                }
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| anyhow!("mesh OCR deadline exceeded"))?;
            let mut request = self
                .agent
                .post(url)
                .timeout(remaining)
                .set("Content-Type", "application/json")
                .set("Accept", "application/json, text/event-stream")
                .set("MCP-Protocol-Version", "2026-07-28")
                .set("Mcp-Method", "tools/call")
                .set("Mcp-Name", tool);
            let env = if relay {
                &self.config.mesh_relay_token_env
            } else {
                &self.config.mesh_peer_token_env
            };
            if let Some(env) = env {
                let token = std::env::var(env)
                    .ok()
                    .filter(|t| !t.trim().is_empty())
                    .ok_or_else(|| anyhow!("OCR transport credentials unavailable"))?;
                request = request.set("Authorization", &format!("Bearer {token}"));
            }
            // Never include a transport error's URL, server body, or input payload.
            let response = request.send_string(payload).map_err(|error| match error {
                ureq::Error::Transport(error)
                    if matches!(
                        error.kind(),
                        ureq::ErrorKind::ConnectionFailed | ureq::ErrorKind::Dns
                    ) =>
                {
                    anyhow!(ConnectFailure)
                }
                _ => anyhow!("mesh OCR transport failed"),
            })?;
            let mut bytes = Vec::new();
            response
                .into_reader()
                .take(MAX_RESPONSE_BYTES + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| anyhow!("read OCR response failed"))?;
            if bytes.len() as u64 > MAX_RESPONSE_BYTES {
                return Err(anyhow!("OCR response exceeds limit"));
            }
            let body: Value =
                serde_json::from_slice(&bytes).map_err(|_| anyhow!("invalid OCR response"))?;
            unwrap_result(body)
        };
        inner().map_err(|err| (err.is::<ConnectFailure>(), err))
    }
}

#[derive(Debug)]
struct ConnectFailure;
impl std::fmt::Display for ConnectFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("mesh OCR direct connection unavailable")
    }
}
impl std::error::Error for ConnectFailure {}

fn unwrap_result(body: Value) -> Result<Value> {
    if body.get("error").is_some() {
        return Err(anyhow!("OCR worker rejected request"));
    }
    let result = body
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow!("missing OCR result"))?;
    if result.get("isError").and_then(Value::as_bool) == Some(true) || result.get("error").is_some()
    {
        return Err(anyhow!("OCR worker rejected request"));
    }
    if let Some(data) = result.get("structuredContent") {
        return Ok(data.clone());
    }
    if let Some(text) = result.pointer("/content/0/text").and_then(Value::as_str) {
        return serde_json::from_str(text).map_err(|_| anyhow!("invalid OCR tool result"));
    }
    Ok(result)
}

fn peer_key(peer: &PeerConfig) -> String {
    format!(
        "{}|{}|{}",
        peer.host_id,
        peer.routable_url,
        peer.gptadmin_relay_url.as_deref().unwrap_or_default()
    )
}

fn merge_compute_peers(mut topology: Topology, configured: &[PeerConfig]) -> Vec<PeerConfig> {
    topology.retain_known_peers(configured);
    // Search topology intentionally excludes self. An enabled compute worker
    // may still serve its own index through the explicitly configured MCP
    // loopback route, while its inference engine always uses backend=local.
    for peer in configured
        .iter()
        .filter(|peer| peer.host_id == topology.local_host_id)
    {
        if !topology
            .peers
            .iter()
            .any(|known| known.host_id == peer.host_id)
        {
            topology.peers.push(peer.clone());
        }
    }
    let mut hosts = BTreeSet::new();
    topology
        .peers
        .retain(|peer| hosts.insert(peer.host_id.clone()));
    topology.peers.truncate(MAX_PEERS);
    topology.peers
}

pub(crate) fn mesh_input_limit(configured: u64) -> u64 {
    if configured == 0 {
        MAX_INPUT_BYTES
    } else {
        configured.min(MAX_INPUT_BYTES)
    }
}
fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "ocr")]
    #[test]
    fn busy_best_gpu_retries_an_available_peer_without_local_inference() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        };
        type Requests = Arc<Mutex<Vec<(String, String)>>>;

        fn mock_peer(
            host: &'static str,
            score: f64,
            reject_ocr: bool,
            requests: Requests,
        ) -> (
            PeerConfig,
            tokio::sync::oneshot::Sender<()>,
            std::thread::JoinHandle<()>,
        ) {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = format!("http://{}/mcp", listener.local_addr().unwrap());
            let (stop, stopped) = tokio::sync::oneshot::channel();
            let thread = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async move {
                    let count = Arc::new(AtomicUsize::new(0));
                    let app = axum::Router::new().route("/mcp", axum::routing::post(
                        move |axum::Json(request): axum::Json<Value>| {
                            let count = count.clone();
                            let requests = requests.clone();
                            async move {
                                let tool = request.pointer("/params/name").and_then(Value::as_str).unwrap_or_default();
                                requests.lock().unwrap().push((host.into(), tool.into()));
                                let error = |message| json!({"jsonrpc":"2.0", "id":1, "error":{"code":-32000, "message":message}});
                                if count.fetch_add(1, Ordering::Relaxed) >= 2 {
                                    return axum::Json(error("mock request budget exceeded"));
                                }
                                let data = match tool {
                                    "compute_status" => json!({
                                        "host_id":host, "backend":"cuda", "gpu_uuid":format!("uuid-{host}"),
                                        "gpu_name":"mock CUDA GPU", "compute_capability":"8.6",
                                        "gpu_free_bytes":4u64*1024*1024*1024, "gpu_utilization_percent":0,
                                        "busy_count":0, "admission":"available", "observed_at":unix_seconds(),
                                        "performance_score":score
                                    }),
                                    "ocr_extract" => {
                                        let encoded = request.pointer("/params/arguments/content_base64").and_then(Value::as_str).unwrap_or_default();
                                        if STANDARD.decode(encoded).ok().as_deref() != Some(&[0xa5u8; 32][..])
                                            || request.pointer("/params/arguments/input_type").and_then(Value::as_str) != Some("png") {
                                            return axum::Json(error("unexpected OCR payload"));
                                        }
                                        if reject_ocr { return axum::Json(error("GPU worker is busy")); }
                                        json!({"text":"mesh failover fixture text", "host_id":host, "backend":"cuda", "gpu_uuid":format!("uuid-{host}")})
                                    }
                                    _ => return axum::Json(error("unexpected RPC tool")),
                                };
                                axum::Json(json!({"jsonrpc":"2.0", "id":1, "result":{
                                    "content":[{"type":"text", "text":serde_json::to_string(&data).unwrap()}], "isError":false
                                }}))
                            }
                        }
                    )).layer(axum::extract::DefaultBodyLimit::max(4096));
                    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                    let finished = tokio::time::timeout(Duration::from_secs(8), async {
                        axum::serve(listener, app).with_graceful_shutdown(async { let _ = stopped.await; }).await.unwrap();
                    }).await;
                    assert!(finished.is_ok(), "mock peer exceeded its finite lifetime");
                });
            });
            (
                PeerConfig {
                    host_id: host.into(),
                    local_url: url.clone(),
                    routable_url: url,
                    gptadmin_proxy_url: None,
                    gptadmin_relay_url: None,
                },
                stop,
                thread,
            )
        }

        let requests: Requests = Arc::new(Mutex::new(Vec::new()));
        let (best, stop_best, best_thread) = mock_peer("best", 400.0, true, requests.clone());
        let (alternate, stop_alternate, alternate_thread) =
            mock_peer("alternate", 100.0, false, requests.clone());
        let temp_root = Path::new(".tmp/compute-client-tests");
        std::fs::create_dir_all(temp_root).unwrap();
        let directory = tempfile::tempdir_in(temp_root).unwrap();
        let input = directory.path().join("fixture.png");
        std::fs::write(&input, [0xa5u8; 32]).unwrap();
        let engine = crate::ocr::OcrEngine::new(OcrConfig {
            backend: "mesh".into(),
            // Probe order deliberately differs from GPU performance order.
            mesh_peers: vec![alternate, best],
            remote_timeout_ms: 5_000,
            // Invalid image bytes and missing models make local inference
            // incapable of producing the fixture's remote text.
            det_model: directory
                .path()
                .join("missing-det.onnx")
                .display()
                .to_string(),
            rec_model: directory
                .path()
                .join("missing-rec.onnx")
                .display()
                .to_string(),
            dict: directory
                .path()
                .join("missing-dict.txt")
                .display()
                .to_string(),
            ..OcrConfig::default()
        })
        .unwrap();
        let result = engine.extract_image(&input);
        // Join both bounded servers before asserting the client outcome.
        let _ = stop_best.send(());
        let _ = stop_alternate.send(());
        best_thread.join().unwrap();
        alternate_thread.join().unwrap();
        assert_eq!(result.unwrap(), "mesh failover fixture text");
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 4, "one status and extraction per peer");
        assert_eq!(
            requests.as_slice(),
            &[
                ("alternate".into(), "compute_status".into()),
                ("best".into(), "compute_status".into()),
                ("best".into(), "ocr_extract".into()),
                ("alternate".into(), "ocr_extract".into()),
            ]
        );
    }

    #[test]
    fn local_compute_route_survives_search_topology_and_is_unique() {
        let local = PeerConfig {
            host_id: "local-gpu".into(),
            local_url: "http://127.0.0.1:9419/mcp".into(),
            routable_url: "http://127.0.0.1:9419/mcp".into(),
            gptadmin_proxy_url: None,
            gptadmin_relay_url: None,
        };
        let remote = PeerConfig {
            host_id: "remote-gpu".into(),
            routable_url: "http://remote/mcp".into(),
            ..local.clone()
        };
        let peers = merge_compute_peers(
            Topology::new("local-gpu", vec![remote.clone(), remote]),
            &[local.clone(), local],
        );
        assert_eq!(peers.len(), 2);
        assert_eq!(
            peers
                .iter()
                .find(|p| p.host_id == "local-gpu")
                .unwrap()
                .routable_url,
            "http://127.0.0.1:9419/mcp"
        );
    }
    #[test]
    fn mesh_payload_limit_caps_unlimited_and_large_configurations() {
        assert_eq!(mesh_input_limit(0), MAX_INPUT_BYTES);
        assert_eq!(mesh_input_limit(256 * 1024 * 1024), MAX_INPUT_BYTES);
        assert_eq!(mesh_input_limit(1024), 1024);
    }
    #[test]
    fn unavailable_workers_do_not_open_the_input() {
        let mut client = ComputeClient::new(OcrConfig::default());
        let error = client
            .extract(Path::new("missing-input.png"), "png")
            .unwrap_err();
        assert!(error.to_string().contains("no available GPU worker"));
    }

    #[test]
    fn failed_peer_cooldown_is_finite_and_route_scoped() {
        let peer = PeerConfig {
            host_id: "gpu".into(),
            local_url: "http://gpu/mcp".into(),
            routable_url: "http://gpu/mcp".into(),
            gptadmin_proxy_url: None,
            gptadmin_relay_url: None,
        };
        let mut client = ComputeClient::new(OcrConfig::default());
        let failed_at = Instant::now();
        client.failures.insert(peer_key(&peer), failed_at);
        assert!(client.cooling_down(&peer_key(&peer), failed_at + Duration::from_secs(59)));
        assert!(!client.cooling_down(&peer_key(&peer), failed_at + Duration::from_secs(60)));
        let moved = PeerConfig {
            routable_url: "http://gpu-new/mcp".into(),
            ..peer
        };
        assert!(!client.cooling_down(&peer_key(&moved), failed_at));
    }

    #[test]
    fn fallback_status_needs_refresh_after_a_long_first_attempt() {
        let mut status: ComputeStatus = serde_json::from_value(json!({"host_id":"gpu", "backend":"cuda", "gpu_uuid":"old-gpu", "gpu_free_bytes":MIN_FREE_GPU_BYTES, "busy_count":0, "admission":"available", "observed_at":100})).unwrap();
        assert!(status.fresh_at(110));
        assert!(!status.fresh_at(145));
        // The renewed status supplies the identity checked against extraction.
        status.observed_at = 145;
        status.gpu_uuid = Some("new-gpu".into());
        assert!(status.eligible("gpu", 145));
        assert_eq!(status.gpu_uuid.as_deref(), Some("new-gpu"));
    }
    #[test]
    fn stale_busy_and_cpu_workers_are_ineligible() {
        let mut status: ComputeStatus = serde_json::from_value(json!({"host_id":"gpu", "backend":"cuda", "gpu_uuid":"id", "gpu_free_bytes":MIN_FREE_GPU_BYTES, "busy_count":0, "admission":"available", "observed_at":100})).unwrap();
        assert!(status.eligible("gpu", 100));
        assert!(!status.eligible("other", 100));
        assert!(!status.eligible("gpu", 111));
        status.busy_count = 1;
        assert!(!status.eligible("gpu", 100));
        status.busy_count = 0;
        status.backend = "cpu".into();
        assert!(!status.eligible("gpu", 100));
    }
    #[test]
    fn reads_existing_mcp_content_and_rejects_error() {
        assert_eq!(
            unwrap_result(
                json!({"result":{"content":[{"type":"text","text":"{\"host_id\":\"gpu\"}"}]}})
            )
            .unwrap()["host_id"],
            "gpu"
        );
        assert!(unwrap_result(json!({"result":{"isError":true,"content":[]}})).is_err());
    }
}
