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
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const MAX_INPUT_BYTES: u64 = 32 * 1024 * 1024;
const MAX_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;
const MIN_FREE_GPU_BYTES: u64 = 5 * 1024 * 1024 * 1024 / 2;
const STATUS_TTL: Duration = Duration::from_secs(10);
const FAILED_PEER_COOLDOWN: Duration = Duration::from_secs(60);
const MAX_PEERS: usize = 8;
// Workers cache a short native load snapshot; avoid overlapping observations
// from this index's three clients while leaving inference fully parallel.
static STATUS_PROBE_LOCK: Mutex<()> = Mutex::new(());
pub(crate) const MAX_PARALLEL_OCR: usize = 3;
pub(crate) const MAX_RECOGNITION_FAILURES: usize = 4;

/// A completed native processing failure, never admission or transport loss.
#[derive(Debug)]
pub(crate) struct OcrRecognitionFailure {
    pub attempts: usize,
}
impl std::fmt::Display for OcrRecognitionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OCR processing failed")
    }
}
impl std::error::Error for OcrRecognitionFailure {}

struct WorkerLease {
    host: String,
    claimed: Arc<Mutex<BTreeSet<String>>>,
}
impl Drop for WorkerLease {
    fn drop(&mut self) {
        if let Ok(mut claimed) = self.claimed.lock() {
            claimed.remove(&self.host);
        }
    }
}
fn claim_worker(claimed: &Arc<Mutex<BTreeSet<String>>>, host: &str) -> Option<WorkerLease> {
    if !claimed.lock().ok()?.insert(host.to_owned()) {
        return None;
    }
    Some(WorkerLease {
        host: host.to_owned(),
        claimed: Arc::clone(claimed),
    })
}

#[derive(Debug)]
pub(crate) struct InvalidOcrInput;

impl std::fmt::Display for InvalidOcrInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("mesh OCR input is not decodable")
    }
}
impl std::error::Error for InvalidOcrInput {}

#[derive(Debug)]
struct WorkerBusy;
impl std::fmt::Display for WorkerBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OCR worker temporarily busy")
    }
}
impl std::error::Error for WorkerBusy {}

#[derive(Clone, Debug, Deserialize)]
struct ComputeStatus {
    host_id: String,
    backend: String,
    #[serde(default)]
    gpu_uuid: Option<String>,
    #[serde(default)]
    gpu_free_bytes: Option<u64>,
    busy_count: usize,
    admission: String,
    observed_at: u64,
    #[serde(default)]
    utilization_percent: Option<f64>,
    #[serde(default)]
    performance_by_task: BTreeMap<String, TaskPerformance>,
    #[serde(default)]
    supports_device_choice: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct TaskPerformance {
    #[serde(default)]
    performance_score: Option<f64>,
    #[serde(default)]
    observations: usize,
    #[serde(default)]
    warm: bool,
    #[serde(default)]
    basis: String,
    #[serde(default)]
    sample_age_seconds: u64,
}
fn device_statuses(value: Value) -> Vec<ComputeStatus> {
    if let Some(devices) = value.get("devices").and_then(Value::as_array) {
        devices
            .iter()
            .take(3)
            .filter_map(|device| {
                let mut device = device.clone();
                device["host_id"] = value["host_id"].clone();
                device["supports_device_choice"] = json!(true);
                serde_json::from_value(device).ok()
            })
            .collect()
    } else {
        serde_json::from_value(value).ok().into_iter().collect()
    }
}
fn task_descriptor(bytes: &[u8], input_type: &str) -> mesh_core::placement::TaskDescriptor {
    #[cfg(feature = "ocr")]
    let dimensions = image::ImageReader::new(std::io::Cursor::new(bytes))
        .with_guessed_format()
        .ok()
        .and_then(|reader| reader.into_dimensions().ok());
    #[cfg(not(feature = "ocr"))]
    let dimensions = (input_type == "png"
        && bytes.len() >= 24
        && &bytes[..8] == b"\x89PNG\r\n\x1a\n")
        .then(|| {
            (
                u32::from_be_bytes(bytes[16..20].try_into().unwrap()),
                u32::from_be_bytes(bytes[20..24].try_into().unwrap()),
            )
        });
    mesh_core::placement::TaskDescriptor {
        input_type: input_type.into(),
        input_bytes: bytes.len() as u64,
        width: dimensions.map(|n| n.0),
        height: dimensions.map(|n| n.1),
    }
}

impl ComputeStatus {
    fn fresh_at(&self, now: u64) -> bool {
        self.observed_at <= now.saturating_add(5)
            && now.saturating_sub(self.observed_at) <= STATUS_TTL.as_secs()
    }

    fn eligible(&self, host: &str, now: u64) -> bool {
        self.host_id == host
            && matches!(self.backend.as_str(), "cuda" | "cpu" | "coreml")
            && self.admission == "available"
            && self.busy_count == 0
            && (matches!(self.backend.as_str(), "cpu" | "coreml")
                || (self.gpu_uuid.as_ref().is_some_and(|id| !id.is_empty())
                    && self.gpu_free_bytes.unwrap_or(0) >= MIN_FREE_GPU_BYTES))
            && self.fresh_at(now)
            && self
                .utilization_percent
                .is_some_and(|n| n.is_finite() && (0.0..85.0).contains(&n))
    }

    fn task_performance(&self, class: Option<&str>) -> Option<f64> {
        let sample = self.performance_by_task.get(class?)?;
        (sample.warm
            && sample.observations > 0
            && sample.sample_age_seconds < 1800
            && sample.basis == "native_jobs_per_second_same_task_class")
            .then_some(sample.performance_score)
            .flatten()
            .filter(|n| n.is_finite() && *n > 0.0)
    }
}

/// One synchronous lane of the bounded queue dispatcher. Shared reservations
/// avoid duplicate assignment; each remote worker also admits one native job.
pub struct ComputeClient {
    config: OcrConfig,
    claimed: Arc<Mutex<BTreeSet<String>>>,
    agent: ureq::Agent,
    statuses: BTreeMap<String, (Instant, Vec<ComputeStatus>)>,
    failures: BTreeMap<String, Instant>,
}

impl ComputeClient {
    pub fn new(config: OcrConfig) -> Self {
        Self::with_shared_workers(config, Arc::new(Mutex::new(BTreeSet::new())))
    }

    pub(crate) fn with_shared_workers(
        config: OcrConfig,
        claimed: Arc<Mutex<BTreeSet<String>>>,
    ) -> Self {
        Self {
            config,
            claimed,
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
            .into_iter()
            .filter(|peer| {
                self.config.mesh_worker_hosts.is_empty()
                    || self.config.mesh_worker_hosts.contains(&peer.host_id)
            })
            .collect()
    }

    pub fn extract(&mut self, path: &Path, input_type: &str) -> Result<String> {
        self.extract_with_budget(path, input_type, MAX_RECOGNITION_FAILURES)
    }

    pub(crate) fn extract_with_budget(
        &mut self,
        path: &Path,
        input_type: &str,
        remaining: usize,
    ) -> Result<String> {
        let remaining = remaining.min(MAX_RECOGNITION_FAILURES);
        if remaining == 0 {
            return Err(anyhow!(OcrRecognitionFailure { attempts: 0 }));
        }
        let mut recognition_failures = 0;
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
            if self
                .claimed
                .lock()
                .map(|set| set.contains(&peer.host_id))
                .unwrap_or(true)
            {
                continue;
            }
            if self.cooling_down(&key, Instant::now()) {
                continue;
            }
            let statuses = self
                .statuses
                .get(&key)
                .filter(|(_, statuses)| statuses.iter().all(|s| s.fresh_at(unix_seconds())))
                .map(|(_, statuses)| statuses.clone())
                .unwrap_or_else(|| {
                    let probe_deadline = deadline.min(Instant::now() + Duration::from_secs(3));
                    let value = match self.call(&peer, "compute_status", json!({}), probe_deadline)
                    {
                        Ok(value) => value,
                        Err(_) => {
                            self.mark_failed(&peer);
                            return Vec::new();
                        }
                    };
                    let statuses = device_statuses(value);
                    self.statuses
                        .insert(key.clone(), (Instant::now(), statuses.clone()));
                    statuses
                });
            for status in statuses
                .into_iter()
                .filter(|status| status.eligible(&peer.host_id, unix_seconds()))
            {
                candidates.push((peer.clone(), status));
            }
        }
        if candidates.is_empty() || Instant::now() >= deadline {
            return Err(anyhow!("mesh OCR deferred: no available OCR worker"));
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
        let descriptor = task_descriptor(&bytes, input_type);
        let task_class = descriptor.task_class();
        let ratings: Vec<_> = candidates
            .iter()
            .map(|(_, status)| mesh_core::placement::TaskWorkerRating {
                host_id: status.host_id.clone(),
                backend: status.backend.clone(),
                performance: status.task_performance(task_class.as_deref()),
                utilization_percent: status.utilization_percent.unwrap_or(f64::NAN),
                observed_at: status.observed_at,
                available: true,
            })
            .collect();
        let order =
            mesh_core::placement::rank_task_workers(&ratings, unix_seconds(), STATUS_TTL.as_secs());
        let mut entries: Vec<_> = candidates.into_iter().map(Some).collect();
        let candidates: Vec<_> = order
            .into_iter()
            .filter_map(|i| entries[i].take())
            .collect();
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
                    .and_then(|value| {
                        device_statuses(value)
                            .into_iter()
                            .find(|s| s.backend == status.backend)
                    });
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
            let Some(_lease) = claim_worker(&self.claimed, &peer.host_id) else {
                continue;
            };
            self.statuses
                .insert(peer_key(&peer), (Instant::now(), vec![status.clone()]));
            let started = Instant::now();
            tracing::info!(worker_host = %peer.host_id, input = %path.display(), backend = %status.backend, via_hub = peer.gptadmin_relay_url.as_deref() == Some(peer.routable_url.as_str()), "mesh OCR dispatch");
            // Leave budget for another eligible candidate after a failed worker.
            let attempt_deadline = ocr_attempt_deadline(input_type, Instant::now(), deadline);
            let mut request = arguments.clone();
            if status.supports_device_choice {
                request["execution_provider"] = json!(status.backend);
                if let Some(class) = task_class.as_ref() {
                    request["task_class"] = json!(class);
                }
            }
            let result = self.call(&peer, "ocr_extract", request, attempt_deadline);
            let result = match result {
                Ok(value) => value,
                Err(error) if error.is::<InvalidOcrInput>() => {
                    tracing::info!(worker_host = %peer.host_id, input = %path.display(), backend = %status.backend, elapsed_ms = started.elapsed().as_millis() as u64, "mesh OCR processing failed");
                    return Err(anyhow!(OcrRecognitionFailure {
                        attempts: recognition_failures + 1
                    }));
                }
                Err(error) if error.is::<WorkerBusy>() => {
                    self.statuses.remove(&peer_key(&peer));
                    continue;
                }
                Err(error) if error.is::<OcrRecognitionFailure>() => {
                    recognition_failures += 1;
                    tracing::info!(worker_host = %peer.host_id, input = %path.display(), backend = %status.backend, elapsed_ms = started.elapsed().as_millis() as u64, "mesh OCR processing failed");
                    // A completed input failure does not make this healthy
                    // host unavailable for other queue files.
                    self.statuses.remove(&peer_key(&peer));
                    if recognition_failures >= remaining {
                        return Err(anyhow!(OcrRecognitionFailure {
                            attempts: recognition_failures
                        }));
                    }
                    continue;
                }
                Err(error) => {
                    tracing::warn!(worker_host = %peer.host_id, input = %path.display(), error = %error, "mesh OCR attempt deferred");
                    self.mark_failed(&peer);
                    continue;
                }
            };
            if result.get("host_id").and_then(Value::as_str) != Some(peer.host_id.as_str())
                || result.get("backend").and_then(Value::as_str) != Some(status.backend.as_str())
                || result.get("gpu_uuid").and_then(Value::as_str) != status.gpu_uuid.as_deref()
            {
                self.mark_failed(&peer);
                continue;
            }
            if let Some(text) = result.get("text").and_then(Value::as_str) {
                if text.len() <= 4 * 1024 * 1024 {
                    tracing::info!(worker_host = %peer.host_id, input = %path.display(), backend = %status.backend, elapsed_ms = started.elapsed().as_millis() as u64, "mesh OCR completed");
                    self.statuses.remove(&peer_key(&peer));
                    return Ok(text.to_owned());
                }
            }
            self.mark_failed(&peer);
        }
        if recognition_failures > 0 {
            return Err(anyhow!(OcrRecognitionFailure {
                attempts: recognition_failures
            }));
        }
        Err(anyhow!(
            "mesh OCR deferred: no available OCR worker completed extraction"
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
        let _probe = if tool == "compute_status" {
            Some(
                STATUS_PROBE_LOCK
                    .lock()
                    .map_err(|_| anyhow!("OCR status probe lock unavailable"))?,
            )
        } else {
            None
        };
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
                ureq::Error::Status(status, _) => anyhow!("mesh OCR HTTP status {}", status),
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
    if body.pointer("/error/code").and_then(Value::as_i64) == Some(-32043) {
        return Err(anyhow!(OcrRecognitionFailure { attempts: 1 }));
    }
    if body.pointer("/error/code").and_then(Value::as_i64) == Some(-32042) {
        return Err(anyhow!(InvalidOcrInput));
    }
    if body.get("error").is_some() {
        if matches!(
            body.pointer("/error/message").and_then(Value::as_str),
            Some(
                "GPU worker is busy"
                    | "GPU worker request intake is busy"
                    | "OCR worker rejected admission: host_cpu_busy"
                    | "OCR worker rejected admission: host_memory_busy"
                    | "OCR worker rejected admission: host_tasks_busy"
                    | "Apple GPU is busy or its native load is unavailable"
            )
        ) {
            return Err(anyhow!(WorkerBusy));
        }
        return Err(anyhow!(
            "OCR worker rejected request (RPC code {})",
            body.pointer("/error/code")
                .and_then(Value::as_i64)
                .unwrap_or(0)
        ));
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

pub(crate) fn merge_compute_peers(
    mut topology: Topology,
    configured: &[PeerConfig],
) -> Vec<PeerConfig> {
    topology.retain_known_peers(configured);
    // A configured OCR-only Hub route is an explicit pin. Registry discovery
    // may still retain the older ShellMCP route for the same host's searches.
    for configured_peer in configured
        .iter()
        .filter(|peer| peer.gptadmin_relay_url.as_deref() == Some(peer.routable_url.as_str()))
    {
        if let Some(peer) = topology
            .peers
            .iter_mut()
            .find(|peer| peer.host_id == configured_peer.host_id)
        {
            *peer = configured_peer.clone();
        }
    }
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
fn ocr_attempt_deadline(input_type: &str, now: Instant, deadline: Instant) -> Instant {
    // A PDF is one whole-document job. Cutting it off at 45 seconds can
    // discard a valid result while its native worker still owns the slot.
    // Keep the existing overall budget; early errors can still fall back.
    if input_type == "pdf" {
        deadline
    } else {
        deadline.min(now + Duration::from_secs(45))
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

    #[test]
    fn recognition_marker_excludes_transport_busy_and_zero_budget_input() {
        let error =
            unwrap_result(json!({"error":{"code":-32043,"message":"private worker details"}}))
                .unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<OcrRecognitionFailure>()
                .unwrap()
                .attempts,
            1
        );
        assert_eq!(error.to_string(), "OCR processing failed");
        for body in [
            json!({"error":{"code":-32000,"message":"GPU worker is busy"}}),
            json!({"error":{"code":-32000,"message":"transport failed"}}),
        ] {
            assert!(!unwrap_result(body)
                .unwrap_err()
                .is::<OcrRecognitionFailure>());
        }
        let error = ComputeClient::new(OcrConfig::default())
            .extract_with_budget(Path::new("missing-input.png"), "png", 0)
            .unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<OcrRecognitionFailure>()
                .unwrap()
                .attempts,
            0
        );
    }

    #[test]
    fn explicit_ocr_relay_replaces_a_stale_cached_route() {
        let old_url = "http://127.0.0.1:9001/server/mac-m1-grepmesh/mcp";
        let new_url = "http://127.0.0.1:9001/server/mac-m1-native-grepmesh/mcp";
        let peer = |url: &str| PeerConfig {
            host_id: "mac-m1".into(),
            local_url: url.into(),
            routable_url: url.into(),
            gptadmin_proxy_url: None,
            gptadmin_relay_url: Some(url.into()),
        };
        let topology = Topology::new("server-100", vec![peer(old_url)]);
        let actual = merge_compute_peers(topology, &[peer(new_url)]);
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].routable_url, new_url);
        assert_eq!(actual[0].gptadmin_relay_url.as_deref(), Some(new_url));
    }

    #[test]
    fn task_ratings_are_unknown_until_same_class_warm_measurement() {
        let mut status: ComputeStatus = serde_json::from_value(json!({"host_id":"server-44", "backend":"cpu", "busy_count":0,"admission":"available","observed_at":100,"utilization_percent":1,"performance_score":999999,"hardware_performance_score":999999})).unwrap();
        let class = "ocr:image:png:pixels:4mp";
        assert_eq!(status.task_performance(Some(class)), None);
        status.performance_by_task.insert(
            class.into(),
            TaskPerformance {
                performance_score: Some(2.0),
                observations: 2,
                warm: true,
                basis: "native_jobs_per_second_same_task_class".into(),
                sample_age_seconds: 1,
            },
        );
        assert_eq!(status.task_performance(Some(class)), Some(2.0));
        assert_eq!(status.task_performance(Some("ocr:pdf:bytes:1m")), None);
        status.performance_by_task.get_mut(class).unwrap().warm = false;
        assert_eq!(status.task_performance(Some(class)), None);
    }

    #[test]
    fn per_device_status_retains_backend_and_declared_worker_scope() {
        let values = device_statuses(
            json!({"host_id":"server-44", "devices":[{"backend":"cpu","busy_count":0,"admission":"available","observed_at":100,"utilization_percent":1},{"backend":"cuda","gpu_uuid":"gpu","gpu_free_bytes":MIN_FREE_GPU_BYTES,"busy_count":1,"admission":"busy","observed_at":100,"utilization_percent":10}]}),
        );
        assert_eq!(values.len(), 2);
        assert!(values[0].supports_device_choice && values[0].eligible("server-44", 100));
        assert!(!values[1].eligible("server-44", 100));
        let peer = |host: &str| PeerConfig {
            host_id: host.into(),
            local_url: "http://127.0.0.1:9419/mcp".into(),
            routable_url: "http://127.0.0.1:9419/mcp".into(),
            gptadmin_proxy_url: None,
            gptadmin_relay_url: None,
        };
        let client = ComputeClient::new(OcrConfig {
            mesh_worker_hosts: vec!["server-44".into(), "mac-m1".into(), "server-88".into()],
            mesh_peers: vec![
                peer("server-100"),
                peer("server-44"),
                peer("server-88"),
                peer("mac-m1"),
            ],
            ..Default::default()
        });
        assert_eq!(client.peers().len(), 3);
        assert!(client.peers().iter().all(|p| p.host_id != "server-100"));
    }

    #[test]
    fn parallel_worker_reservations_are_exclusive_and_released() {
        let claimed = std::sync::Arc::new(std::sync::Mutex::new(std::collections::BTreeSet::new()));
        let lease = super::claim_worker(&claimed, "server-44").unwrap();
        assert!(super::claim_worker(&claimed, "server-44").is_none());
        let other = super::claim_worker(&claimed, "mac-m1").unwrap();
        drop(lease);
        assert!(super::claim_worker(&claimed, "server-44").is_some());
        drop(other);
        assert!(claimed.lock().unwrap().is_empty());
    }

    #[test]
    fn pdf_attempt_keeps_its_existing_whole_document_budget() {
        let now = Instant::now();
        let deadline = now + Duration::from_secs(90);
        assert_eq!(ocr_attempt_deadline("pdf", now, deadline), deadline);
    }

    #[test]
    fn image_attempt_keeps_fallback_time_and_no_attempt_extends_its_budget() {
        let now = Instant::now();
        assert_eq!(
            ocr_attempt_deadline("png", now, now + Duration::from_secs(90)),
            now + Duration::from_secs(45)
        );
        for input_type in ["png", "pdf"] {
            let short = now + Duration::from_secs(3);
            assert_eq!(ocr_attempt_deadline(input_type, now, short), short);
            let expired = now - Duration::from_secs(1);
            assert_eq!(ocr_attempt_deadline(input_type, now, expired), expired);
        }
    }

    #[test]
    fn only_confirmed_input_rejection_is_terminal() {
        let error =
            unwrap_result(json!({"error":{"code":-32042,"message":"OCR input cannot be decoded"}}))
                .unwrap_err();
        assert!(error.is::<InvalidOcrInput>());
        let error = unwrap_result(json!({"error":{"code":-32000,"message":"GPU worker is busy"}}))
            .unwrap_err();
        assert!(!error.is::<InvalidOcrInput>());
        assert!(
            error.is::<WorkerBusy>(),
            "busy is not a failed peer requiring cooldown"
        );
    }

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
                                        "performance_score":score, "utilization_percent":0
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
        let (best, stop_best, best_thread) = mock_peer("a-best", 400.0, true, requests.clone());
        let (alternate, stop_alternate, alternate_thread) =
            mock_peer("z-alternate", 100.0, false, requests.clone());
        let temp_root = Path::new(".tmp/compute-client-tests");
        std::fs::create_dir_all(temp_root).unwrap();
        let directory = tempfile::tempdir_in(temp_root).unwrap();
        let input = directory.path().join("fixture.png");
        std::fs::write(&input, [0xa5u8; 32]).unwrap();
        let engine = crate::ocr::OcrEngine::new(OcrConfig {
            backend: "mesh".into(),
            // Cold device ordering deliberately differs from probe order.
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
                ("z-alternate".into(), "compute_status".into()),
                ("a-best".into(), "compute_status".into()),
                ("a-best".into(), "ocr_extract".into()),
                ("z-alternate".into(), "ocr_extract".into()),
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
        assert!(error.to_string().contains("no available OCR worker"));
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
        let mut status: ComputeStatus = serde_json::from_value(json!({"host_id":"gpu", "backend":"cuda", "gpu_uuid":"old-gpu", "gpu_free_bytes":MIN_FREE_GPU_BYTES, "busy_count":0, "admission":"available", "observed_at":100,"utilization_percent":0})).unwrap();
        assert!(status.fresh_at(110));
        assert!(!status.fresh_at(145));
        // The renewed status supplies the identity checked against extraction.
        status.observed_at = 145;
        status.gpu_uuid = Some("new-gpu".into());
        assert!(status.eligible("gpu", 145));
        assert_eq!(status.gpu_uuid.as_deref(), Some("new-gpu"));
    }
    #[test]
    fn stale_busy_and_unknown_workers_are_ineligible() {
        let mut status: ComputeStatus = serde_json::from_value(json!({"host_id":"gpu", "backend":"cuda", "gpu_uuid":"id", "gpu_free_bytes":MIN_FREE_GPU_BYTES, "busy_count":0, "admission":"available", "observed_at":100,"utilization_percent":0})).unwrap();
        assert!(status.eligible("gpu", 100));
        assert!(!status.eligible("other", 100));
        assert!(!status.eligible("gpu", 111));
        status.busy_count = 1;
        assert!(!status.eligible("gpu", 100));
        status.busy_count = 0;
        status.backend = "unknown".into();
        assert!(!status.eligible("gpu", 100));
    }
    // Fast unit; explicit CPU admission must retain host, freshness and busy fences.
    // Expected <1s, maximum 5s; catches rejecting valid Mac CPU workers.
    #[test]
    fn explicit_cpu_worker_needs_no_cuda_but_retains_admission_fences() {
        let mut status: ComputeStatus = serde_json::from_value(json!({
            "host_id":"mac-m1", "backend":"cpu", "busy_count":0,
            "admission":"available", "observed_at":100, "utilization_percent":0
        }))
        .unwrap();
        assert!(status.eligible("mac-m1", 100));
        assert!(!status.eligible("other", 100));
        assert!(!status.eligible("mac-m1", 111));
        status.busy_count = 1;
        assert!(!status.eligible("mac-m1", 100));
        status.busy_count = 0;
        status.admission = "ineligible".into();
        assert!(!status.eligible("mac-m1", 100));
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
