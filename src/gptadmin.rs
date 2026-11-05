use crate::topology_cache::{ProviderSnapshot, TopologyNode, TopologySnapshot};
use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    env,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone)]
pub struct GptAdminTopologyClient {
    endpoint: String,
    token: Option<String>,
    local_host_id: String,
    ttl_ms: u64,
    client: reqwest::Client,
}

/// Browser-only task client. The credential remains in the backend; the Hub
/// creates and revalidates a separate filesystem policy for every operation.
#[derive(Clone)]
pub(crate) struct BrowserTaskClient {
    token: String,
    targets: BTreeMap<String, BrowserRoute>,
    client: reqwest::Client,
    endpoint: String,
    receipts: std::sync::Arc<std::sync::Mutex<BTreeMap<String, BrowserReceipt>>>,
}

struct BrowserReceipt { created: std::time::Instant, job_id: Option<String> }

async fn bounded_browser_response(response: reqwest::Response) -> Result<Value> {
    const LIMIT: usize = 4 * 1024 * 1024;
    anyhow::ensure!(response.content_length().map_or(true, |length| length <= LIMIT as u64), "GPTAdmin response exceeds limit");
    let mut stream = response.bytes_stream();
    let mut data = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        anyhow::ensure!(data.len().saturating_add(chunk.len()) <= LIMIT, "GPTAdmin response exceeds limit");
        data.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&data)?)
}

#[derive(Clone, Deserialize)]
pub(crate) struct BrowserRoute {
    pub(crate) shell_target: String,
    pub(crate) child_ref: String,
    pub(crate) execution_host: String,
    pub(crate) executor_host: String,
}

impl BrowserTaskClient {
    pub(crate) fn from_env() -> Result<Option<Self>> {
        let Ok(token) = env::var("GREPMESH_BROWSER_HUB_TOKEN") else { return Ok(None) };
        anyhow::ensure!(!token.trim().is_empty(), "browser Hub credential is empty");
        anyhow::ensure!(env::var("GREPMESH_BROWSER_REQUIRE_SSO").as_deref() == Ok("1"),
            "browser Hub access requires browser SSO verification");
        let targets: BTreeMap<String, BrowserRoute> = serde_json::from_str(
            &env::var("GREPMESH_BROWSER_SHELL_TARGETS")
                .context("browser shell target mapping is unavailable")?)?;
        anyhow::ensure!(!targets.is_empty() && targets.iter().all(|(host, route)|
            !host.trim().is_empty() && route.shell_target.starts_with("shell:")
                && !route.shell_target[6..].is_empty() && !route.shell_target.contains(['/', '\\'])
                && route.child_ref == "GrepMesh" && !route.executor_host.trim().is_empty()
                && ((route.execution_host == "local" && route.executor_host == *host)
                    || (route.execution_host == *host && route.executor_host != *host))),
            "invalid browser shell target mapping");
        let endpoint = env::var("GREPMESH_BROWSER_HUB_URL").unwrap_or_else(|_| "http://127.0.0.1:9001".into());
        let parsed = reqwest::Url::parse(&endpoint)?;
        anyhow::ensure!(parsed.scheme() == "http" && parsed.host_str() == Some("127.0.0.1")
            && parsed.username().is_empty() && parsed.password().is_none()
            && parsed.path() == "/" && parsed.query().is_none() && parsed.fragment().is_none(),
            "browser Hub URL must be a loopback HTTP origin");
        Ok(Some(Self { token, targets, client: reqwest::Client::builder()
            .no_proxy().redirect(reqwest::redirect::Policy::none()).timeout(Duration::from_secs(10)).build()?,
            endpoint: endpoint.trim_end_matches('/').into(), receipts: Default::default() }))
    }

    pub(crate) fn route(&self, host: &str) -> Result<&BrowserRoute> {
        self.targets.get(host).ok_or_else(|| anyhow!("no GPTAdmin executor mapped for {host}"))
    }

    pub(crate) async fn call(&self, host: &str, name: &str, args: Value, id: &str, deadline: Duration,
        on_admitted: Option<Box<dyn Fn(&str) -> Result<()> + Send + Sync>>) -> Result<Value> {
        let started = std::time::Instant::now();
        anyhow::ensure!(matches!(name, "search_text" | "read_text"), "unsupported browser operation");
        let route = self.route(host)?;
        let known_job = {
            let mut receipts = self.receipts.lock().map_err(|_| anyhow!("browser task receipt lock failed"))?;
            receipts.retain(|_, entry| entry.created.elapsed() < Duration::from_secs(3600));
            match receipts.get(id) {
                Some(entry) => Some(entry.job_id.clone().ok_or_else(|| anyhow!("browser task admission is pending or unknown; do not resubmit"))?),
                None => {
                    anyhow::ensure!(receipts.len() < 4096, "browser task receipt capacity reached");
                    receipts.insert(id.to_owned(), BrowserReceipt { created: std::time::Instant::now(), job_id: None });
                    None
                }
            }
        };
        // The Hub authorizes a filesystem-scoped profile on the registered
        // child target, then creates the bound shell mcp_call itself. A direct
        // shell mcp_call is intentionally denied by its coarse policy.
        let target = format!("mcp:{}:{}", route.shell_target, route.child_ref);
        let mut body = json!({"target": target, "tool":name, "arguments":args,
            "background":true, "detail":"full", "idempotency_key":id});
        let mut result: Value = if let Some(job_id) = known_job {
            json!({"status":"running", "job_id":job_id})
        } else {
        let remaining = deadline.checked_sub(started.elapsed()).filter(|d| !d.is_zero())
            .ok_or_else(|| anyhow!("browser task deadline elapsed before submission"))?;
        let mut response = self.client.post(format!("{}/mcp-relay/call", self.endpoint))
            .timeout(remaining).bearer_auth(&self.token).json(&body).send().await
            .map_err(|_| anyhow!("GPTAdmin task submission outcome unknown; do not resubmit"))?;
        if response.status() == reqwest::StatusCode::PRECONDITION_REQUIRED {
            let refusal = bounded_browser_response(response).await?;
            // Only the Hub's explicit unexecuted instruction bootstrap may be
            // retried. Approval refusals and ambiguous outcomes fail closed.
            if !(refusal.get("executed").and_then(Value::as_bool) == Some(false)
                && refusal.pointer("/error/code").and_then(Value::as_str) == Some("instructions_required")
                && refusal.get("instructions").and_then(Value::as_str).is_some()) {
                let reason = refusal.pointer("/error/code").and_then(Value::as_str)
                    .or_else(|| refusal.get("status").and_then(Value::as_str)).unwrap_or("precondition_unknown");
                let approval = refusal.get("approval_id").and_then(Value::as_str).unwrap_or("");
                anyhow::bail!("GPTAdmin precondition: {} {}", safe_diagnostic(reason, 100, None, Some(&self.token)),
                    safe_diagnostic(approval, 100, None, Some(&self.token)));
            }
            let context = refusal.get("context").and_then(Value::as_str)
                .filter(|v| !v.is_empty()).ok_or_else(|| anyhow!("GPTAdmin instruction context is missing"))?;
            body["context"] = json!(context);
            let remaining = deadline.checked_sub(started.elapsed()).filter(|d| !d.is_zero())
                .ok_or_else(|| anyhow!("browser task deadline elapsed before instruction continuation"))?;
            response = self.client.post(format!("{}/mcp-relay/call", self.endpoint))
                .timeout(remaining).bearer_auth(&self.token).json(&body).send().await
                .map_err(|_| anyhow!("GPTAdmin task submission outcome unknown; do not resubmit"))?;
        }
        let status = response.status();
        if !status.is_success() {
            let refusal = bounded_browser_response(response).await.unwrap_or(Value::Null);
            let reason = refusal.get("error").and_then(Value::as_str)
                .or_else(|| refusal.pointer("/error/code").and_then(Value::as_str))
                .or_else(|| refusal.get("detail").and_then(Value::as_str))
                .unwrap_or("no reason supplied");
            anyhow::bail!("GPTAdmin task admission failed: HTTP {status}: {}",
                safe_diagnostic(reason, 300, None, Some(&self.token)));
        }
        let result = bounded_browser_response(response).await?;
        if let Some(job_id) = result.get("job_id").and_then(Value::as_str) {
            anyhow::ensure!(job_id.len() <= 128 && job_id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'), "invalid GPTAdmin task ID");
            self.receipts.lock().map_err(|_| anyhow!("browser task receipt lock failed after admission; job {job_id} must not be resubmitted"))?
                .get_mut(id).ok_or_else(|| anyhow!("browser task receipt missing after admission; job {job_id} must not be resubmitted"))?
                .job_id = Some(job_id.to_owned());
            if let Some(record) = on_admitted.as_ref() {
                record(job_id).map_err(|_| anyhow!("could not persist GPTAdmin task {job_id}; do not resubmit"))?;
            }
        }
        result
        };
        let job_id = result.get("job_id").and_then(Value::as_str).map(str::to_owned);
        loop {
            match result.get("status").and_then(Value::as_str) {
                Some("completed") => break,
                Some("failed" | "cancelled" | "canceled") => {
                    let reason = result.get("error").and_then(Value::as_str).unwrap_or("no reason supplied");
                    anyhow::bail!("GPTAdmin task {} failed: {}", job_id.as_deref().unwrap_or("unknown"),
                        safe_diagnostic(reason, 300, None, Some(&self.token)))
                },
                Some("running" | "queued" | "pending") => {},
                _ => anyhow::bail!("GPTAdmin returned an invalid task state"),
            }
            let job_id = job_id.as_deref().ok_or_else(|| anyhow!("GPTAdmin task has no job ID"))?;
            anyhow::ensure!(job_id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'), "invalid GPTAdmin task ID");
            let remaining = deadline.checked_sub(started.elapsed()).filter(|v| *v > Duration::from_millis(300))
                .ok_or_else(|| anyhow!("GPTAdmin task {job_id} is still running; do not resubmit"))?;
            tokio::time::sleep(Duration::from_millis(250)).await;
            let poll = self.client.get(format!("{}/mcp-relay/job/{job_id}?detail=full", self.endpoint))
                .timeout(remaining.saturating_sub(Duration::from_millis(250)))
                .bearer_auth(&self.token).send().await
                .map_err(|_| anyhow!("GPTAdmin task {job_id} polling failed; do not resubmit"))?;
            anyhow::ensure!(poll.status().is_success(), "GPTAdmin task {job_id} polling returned HTTP {}; do not resubmit", poll.status());
            result = bounded_browser_response(poll).await.map_err(|_| anyhow!("GPTAdmin task {job_id} returned malformed status; do not resubmit"))?;
        }
        let child = result.pointer("/response/structuredContent/result/structuredContent/result")
            .cloned().ok_or_else(|| anyhow!("GPTAdmin task result is missing the child response"))?;
        if child.get("isError").and_then(Value::as_bool) == Some(true) {
            let reason = child.pointer("/content/0/text").and_then(Value::as_str).unwrap_or("no reason supplied");
            anyhow::bail!("GrepMesh child task {} failed: {}", job_id.as_deref().unwrap_or("unknown"),
                safe_diagnostic(reason, 300, None, Some(&self.token)));
        }
        Ok(child)
    }
}

#[derive(Debug, Deserialize)]
struct TopologyResponse {
    nodes: Vec<TopologyNodeWire>,
}

#[derive(Debug, Deserialize)]
struct TopologyNodeWire {
    host_id: String,
    #[serde(default)]
    local_url: Option<String>,
    #[serde(alias = "advertise_url", alias = "routable_url", alias = "endpoint")]
    #[serde(default)]
    peer_url: Option<String>,
    #[serde(default)]
    capabilities: Vec<String>,
    #[serde(default)]
    roots: Vec<String>,
    #[serde(default)]
    generation: u64,
}

const GPTADMIN_REGISTRY_SCAN_TIMEOUT: Duration = Duration::from_secs(50);
const GPTADMIN_CHILD_PROBE_TIMEOUT: Duration = Duration::from_secs(6);
const GPTADMIN_MAX_REGISTRY_CANDIDATES: usize = 8;

struct InstructionGate<'a> {
    executed: Option<bool>,
    context: Option<&'a str>,
    profile: Option<&'a str>,
    instructions: Option<&'a str>,
}

impl InstructionGate<'_> {
    /// The Hub builds this refusal before the child is contacted, so
    /// `executed == false` proves the refused call had no effect and may be
    /// repeated once. A missing marker is an unknown outcome: never repeat it.
    fn retry_context(&self) -> Result<&str, &'static str> {
        match self.executed {
            Some(false) => {}
            Some(true) => return Err("executed_true"),
            None => return Err("executed_marker_missing_or_invalid"),
        }
        match self.context {
            Some(context) if !context.trim().is_empty() => Ok(context),
            _ => Err("context_missing"),
        }
    }

    /// Bounded diagnostics: the effective profile and its instruction text.
    /// The instruction context receipt and the service token never appear here.
    fn diagnostic(
        &self,
        code: &str,
        reason: &str,
        detail: Option<&str>,
        token: Option<&str>,
    ) -> String {
        let profile = safe_diagnostic(
            self.profile.unwrap_or("unknown profile"),
            128,
            self.context,
            token,
        );
        let instructions = safe_diagnostic(
            self.instructions
                .unwrap_or("profile instructions unavailable"),
            1024,
            self.context,
            token,
        );
        let mut diagnostic = format!(
            "GPTAdmin child MCP call failed (code={code}; reason={reason}; profile={profile}; instructions={instructions}"
        );
        if let Some(detail) = detail {
            let detail = safe_diagnostic(detail, 512, self.context, token);
            if !detail.is_empty() {
                diagnostic.push_str(&format!("; detail={detail}"));
            }
        }
        diagnostic.push(')');
        diagnostic
    }
}

fn instruction_gate(response: &Value) -> Option<InstructionGate<'_>> {
    if response.pointer("/result/isError").and_then(Value::as_bool) != Some(true) {
        return None;
    }
    let content = response.pointer("/result/structuredContent")?;
    if content.pointer("/status")?.as_str()? != "failed" {
        return None;
    }
    instruction_refusal(response)
}

fn instruction_refusal(response: &Value) -> Option<InstructionGate<'_>> {
    let content = response.pointer("/result/structuredContent")?;
    if content.pointer("/error/code")?.as_str()? != "instructions_required" {
        return None;
    }
    Some(InstructionGate {
        executed: content.get("executed").and_then(Value::as_bool),
        context: content.get("context").and_then(Value::as_str),
        profile: content.get("profile").and_then(Value::as_str),
        instructions: content.get("instructions").and_then(Value::as_str),
    })
}

fn compact_diagnostic(value: &str, max_chars: usize) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(max_chars)
        .collect()
}

fn safe_diagnostic(
    value: &str,
    max_chars: usize,
    context: Option<&str>,
    token: Option<&str>,
) -> String {
    let mut value = value.to_string();
    for secret in [context, token].into_iter().flatten() {
        if !secret.is_empty() {
            value = value.replace(secret, "[redacted]");
        }
    }
    compact_diagnostic(&value, max_chars)
}

fn registry_probe_label(value: &str) -> String {
    let value = value
        .strip_prefix("/server/")
        .and_then(|value| value.strip_suffix("/mcp"))
        .unwrap_or(value);
    let label = value
        .chars()
        .take(96)
        .map(|character| {
            if character.is_ascii_alphanumeric() || "._-".contains(character) {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if label.is_empty() {
        "child".to_string()
    } else {
        label
    }
}

fn record_registry_probe_failure(failures: &mut Vec<String>, label: &str, cause: &str) {
    if failures.len() < 8 {
        failures.push(format!(
            "{label}: {}",
            safe_diagnostic(cause, 200, None, None)
        ));
    }
}

fn retry_transport_reason(error: &anyhow::Error) -> String {
    let Some(error) = error.downcast_ref::<reqwest::Error>() else {
        return "retry_transport_failed".to_string();
    };
    if error.is_timeout() {
        "retry_timeout".to_string()
    } else if error.is_connect() {
        "retry_connect_failed".to_string()
    } else if let Some(status) = error.status() {
        format!("retry_http_status_{}", status.as_u16())
    } else if error.is_decode() {
        "retry_response_decode_failed".to_string()
    } else {
        "retry_transport_failed".to_string()
    }
}

fn initial_transport_reason(error: &anyhow::Error) -> String {
    let reason = retry_transport_reason(error);
    format!("initial_{}", reason.trim_start_matches("retry_"))
}

fn registry_scan_timeout(candidate_count: usize) -> Duration {
    let count = candidate_count.max(1).min(GPTADMIN_MAX_REGISTRY_CANDIDATES) as u64;
    (Duration::from_secs(GPTADMIN_CHILD_PROBE_TIMEOUT.as_secs() * count) + Duration::from_secs(2))
        .min(GPTADMIN_REGISTRY_SCAN_TIMEOUT)
}

fn retry_error_code(body: &Value) -> String {
    response_error_code(body).unwrap_or_else(|| {
        if body.get("result").is_none() {
            "missing_result".to_string()
        } else {
            "child_call_failed".to_string()
        }
    })
}

impl GptAdminTopologyClient {
    pub fn from_env(
        endpoint: impl Into<String>,
        local_host_id: impl Into<String>,
        token_env: Option<&str>,
        ttl_ms: u64,
    ) -> Self {
        let token = token_env
            .and_then(|name| env::var(name).ok())
            .filter(|value| !value.trim().is_empty());
        Self {
            endpoint: endpoint.into(),
            token,
            local_host_id: local_host_id.into(),
            ttl_ms,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(10))
                .build()
                .expect("build GPTAdmin client"),
        }
    }

    pub async fn fetch(&self) -> Result<ProviderSnapshot> {
        let mut request = self.client.get(&self.endpoint);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let body = request
            .send()
            .await
            .with_context(|| format!("fetch GPTAdmin topology {}", self.endpoint))?
            .error_for_status()
            .context("GPTAdmin topology returned an error")?
            .json::<Value>()
            .await
            .context("decode GPTAdmin topology")?;
        if body.get("agents").is_some() {
            return self.from_registry(body).await;
        }
        let projection: TopologyResponse = serde_json::from_value(body)?;
        let mut projected =
            parse_topology_response(projection, &self.local_host_id, now_ms(), self.ttl_ms)?;
        // Older Hub projections do not include dynamically exposed child MCPs.
        // Reuse the Hub's real registry instead of registering a second fleet.
        let mut registry = reqwest::Url::parse(&self.endpoint)?;
        registry.set_path("/mcp-relay/agents");
        registry.set_query(None);
        let mut request = self.client.get(registry);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let discovered: Result<ProviderSnapshot> = async {
            let body = request
                .send()
                .await?
                .error_for_status()?
                .json::<Value>()
                .await?;
            self.from_registry(body).await
        }
        .await;
        match discovered {
            Ok(registry) => {
                for peer in registry.peers {
                    if let Some(known) = projected
                        .peers
                        .iter_mut()
                        .find(|known| known.host_id == peer.host_id)
                    {
                        // The existing projection knows a direct address; the
                        // child registry adds a relay without replacing it.
                        known.gptadmin_relay_url = peer.gptadmin_relay_url;
                    } else {
                        projected.peers.push(peer);
                    }
                }
                projected.validate()?;
                Ok(projected)
            }
            Err(_) if !projected.peers.is_empty() => Ok(projected),
            Err(error) => Err(error),
        }
    }

    fn relay_url(&self, path: &str) -> Result<String> {
        if !path.starts_with("/server/")
            || !path.ends_with("/mcp")
            || path.contains(['?', '#', '\\'])
            || path.contains("..")
        {
            return Err(anyhow!("invalid GPTAdmin child MCP path"));
        }
        let mut url = reqwest::Url::parse(&self.endpoint)?;
        if !url.username().is_empty() || url.password().is_some() {
            return Err(anyhow!("GPTAdmin credentials must come from token_env"));
        }
        url.set_path(path);
        url.set_query(None);
        url.set_fragment(None);
        Ok(url.to_string())
    }

    pub async fn call_relay(&self, url: &str, tool: &str, arguments: Value) -> Result<Value> {
        let parsed = reqwest::Url::parse(url)?;
        if self.relay_url(parsed.path())? != url {
            return Err(anyhow!("GPTAdmin relay must use the configured Hub origin"));
        }
        let body = self
            .send_relay_call(url, tool, &arguments, None)
            .await
            .map_err(|error| {
                anyhow!(
                    "GPTAdmin child MCP call failed (reason={})",
                    initial_transport_reason(&error)
                )
            })?;
        let gate = match instruction_gate(&body) {
            Some(gate) => gate,
            None => {
                if let Some(refusal) = instruction_refusal(&body) {
                    return Err(anyhow!(refusal.diagnostic(
                        "instructions_required",
                        "envelope_shape_unmatched",
                        None,
                        self.token.as_deref(),
                    )));
                }
                return relay_result(body);
            }
        };
        let context = match gate.retry_context() {
            Ok(context) => context,
            Err(reason) => {
                return Err(anyhow!(gate.diagnostic(
                    "instructions_required",
                    reason,
                    None,
                    self.token.as_deref(),
                )));
            }
        };
        // Exactly one retry with the same URL, tool, arguments and
        // Authorization. The context receipt travels only in the JSON-RPC
        // `_meta` envelope, never in the tool arguments.
        let retried = self
            .send_relay_call(url, tool, &arguments, Some(context))
            .await
            .map_err(|error| {
                let reason = retry_transport_reason(&error);
                anyhow!(gate.diagnostic(
                    "instructions_required",
                    &reason,
                    None,
                    self.token.as_deref(),
                ))
            })?;
        let code = retry_error_code(&retried);
        let detail = response_error_details(&retried);
        match relay_result(retried) {
            Ok(result) => Ok(result),
            Err(error) => {
                let detail = detail.unwrap_or_else(|| error.to_string());
                Err(anyhow!(gate.diagnostic(
                    &code,
                    "retry_failed",
                    Some(&detail),
                    self.token.as_deref(),
                )))
            }
        }
    }

    async fn send_relay_call(
        &self,
        url: &str,
        tool: &str,
        arguments: &Value,
        context: Option<&str>,
    ) -> Result<Value> {
        let mut params = json!({"name":tool,"arguments":arguments});
        if let Some(context) = context {
            params["_meta"] = json!({"io.gptadmin/context":context});
        }
        let mut request = self.client.post(url).json(&json!({
            "jsonrpc":"2.0", "id":1, "method":"tools/call", "params":params
        }));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        Ok(request.send().await?.error_for_status()?.json().await?)
    }

    async fn from_registry(&self, body: Value) -> Result<ProviderSnapshot> {
        let agents = body
            .get("agents")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("missing GPTAdmin agents registry"))?;
        let fetched_at_ms = now_ms();
        let mut peers = BTreeMap::new();
        let mut probe_failures = Vec::new();
        let all_candidates = agents
            .iter()
            .take(256)
            .filter(|agent| {
                agent.get("kind").and_then(Value::as_str) == Some("child_mcp")
                    && agent
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .eq_ignore_ascii_case("grepmesh")
                    && agent.get("status").and_then(Value::as_str) == Some("online")
            })
            .collect::<Vec<_>>();
        if all_candidates.len() > GPTADMIN_MAX_REGISTRY_CANDIDATES {
            record_registry_probe_failure(
                &mut probe_failures,
                "registry",
                "candidate_limit_exceeded",
            );
        }
        let candidates = all_candidates
            .into_iter()
            .take(GPTADMIN_MAX_REGISTRY_CANDIDATES)
            .collect::<Vec<_>>();
        let deadline = tokio::time::Instant::now() + registry_scan_timeout(candidates.len());
        for agent in candidates {
            if tokio::time::Instant::now() >= deadline {
                record_registry_probe_failure(&mut probe_failures, "registry", "scan_deadline");
                break;
            }
            let Some(path) = agent
                .pointer("/meta/public_mcp_path")
                .and_then(Value::as_str)
            else {
                let label = agent
                    .get("agent_id")
                    .and_then(Value::as_str)
                    .map(registry_probe_label)
                    .unwrap_or_else(|| "child".to_string());
                record_registry_probe_failure(
                    &mut probe_failures,
                    &label,
                    "missing_public_mcp_path",
                );
                continue;
            };
            let label = registry_probe_label(path);
            let Ok(url) = self.relay_url(path) else {
                record_registry_probe_failure(&mut probe_failures, &label, "invalid_relay_path");
                continue;
            };
            // A forwarded request must stay local even if that peer knows a mesh.
            // One child probe may need a refusal round trip and one exact retry.
            let probe_deadline = tokio::time::Instant::now() + GPTADMIN_CHILD_PROBE_TIMEOUT;
            let result = match tokio::time::timeout_at(
                probe_deadline,
                self.call_relay(
                    &url,
                    "list_locations",
                    json!({"hosts":"local", "hop_count":1}),
                ),
            )
            .await
            {
                Ok(Ok(result)) => result,
                Ok(Err(error)) => {
                    record_registry_probe_failure(&mut probe_failures, &label, &error.to_string());
                    continue;
                }
                Err(_) => {
                    record_registry_probe_failure(&mut probe_failures, &label, "probe_timeout");
                    continue;
                }
            };
            let payload = result.get("structuredContent").cloned().or_else(|| {
                result.get("content")?.as_array()?.iter().find_map(|item| {
                    serde_json::from_str::<Value>(item.get("text")?.as_str()?).ok()
                })
            });
            let Some(payload) = payload else {
                record_registry_probe_failure(
                    &mut probe_failures,
                    &label,
                    "missing_result_payload",
                );
                continue;
            };
            let Some(host) = payload.get("host_id").and_then(Value::as_str) else {
                record_registry_probe_failure(&mut probe_failures, &label, "missing_host_id");
                continue;
            };
            if host == self.local_host_id {
                continue;
            }
            peers.entry(host.to_string()).or_insert(TopologyNode {
                host_id: host.to_string(),
                local_url: url.clone(),
                routable_url: url.clone(),
                gptadmin_relay_url: Some(url),
                capabilities: vec![],
                roots: vec![],
                generation: 1,
                fetched_at_ms,
                expires_at_ms: fetched_at_ms.saturating_add(self.ttl_ms),
                last_refresh_error: None,
            });
        }
        if !probe_failures.is_empty() {
            tracing::warn!(
                failed_registry_probes = ?probe_failures,
                "GPTAdmin GrepMesh child registry probes failed"
            );
            // A registry snapshot with failed probes is incomplete. Returning
            // its successful subset as a fresh provider result would make
            // callers treat missing routes as absent and could refresh the
            // wrong topology. Let refresh_cache preserve the prior snapshot
            // without extending its freshness instead.
            return Err(anyhow!(
                "GPTAdmin GrepMesh child registry probe was partial (reachable: {}, failures: {})",
                peers.len(),
                probe_failures.join("; ")
            ));
        }
        if peers.is_empty() {
            let causes = if probe_failures.is_empty() {
                "no eligible child-MCP records".to_string()
            } else {
                probe_failures.join("; ")
            };
            return Err(anyhow!(
                "GPTAdmin registry returned no reachable remote GrepMesh children (probes: {causes})"
            ));
        }
        let result = ProviderSnapshot {
            local_host_id: self.local_host_id.clone(),
            generation: 1,
            fetched_at_ms,
            ttl_ms: self.ttl_ms,
            peers: peers.into_values().collect(),
        };
        result.validate()?;
        Ok(result)
    }

    pub async fn refresh_cache(
        &self,
        current: &TopologySnapshot,
        cache_path: Option<&Path>,
    ) -> Result<TopologySnapshot> {
        let mut provider = self.fetch().await?;
        // Registry probes are partial. Preserve missing cached hosts across
        // restarts without renewing their old freshness timestamps.
        for previous in current
            .peers
            .iter()
            .filter(|peer| peer.gptadmin_relay_url.is_some())
        {
            if let Some(peer) = provider
                .peers
                .iter_mut()
                .find(|peer| peer.host_id == previous.host_id)
            {
                if peer.gptadmin_relay_url.is_none() {
                    peer.gptadmin_relay_url = previous.gptadmin_relay_url.clone();
                }
            } else {
                provider.peers.push(previous.clone());
            }
        }
        let merged = current.merge_fresh_provider_result(provider)?;
        if let Some(path) = cache_path {
            merged.save_atomic(path)?;
        }
        Ok(merged)
    }
}

fn relay_result(body: Value) -> Result<Value> {
    if has_json_error(body.get("error"))
        || has_json_error(body.pointer("/result/error"))
        || body.pointer("/result/isError").and_then(Value::as_bool) == Some(true)
    {
        if let Some(code) = response_error_code(&body) {
            return Err(anyhow!("GPTAdmin child MCP call failed (code={code})"));
        }
        return Err(anyhow!("GPTAdmin child MCP call failed"));
    }
    body.get("result")
        .cloned()
        .ok_or_else(|| anyhow!("missing GPTAdmin MCP result"))
}

/// A present but null or false `error` field is not a failure.
fn has_json_error(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Null) | Some(Value::Bool(false)) => false,
        Some(_) => true,
        None => false,
    }
}

/// Keeps the child's failure code instead of collapsing every error into one
/// generic message. Only short, printable codes are accepted.
fn response_error_code(body: &Value) -> Option<String> {
    if let Some(code) = body
        .pointer("/result/structuredContent/error/code")
        .and_then(Value::as_str)
        .filter(|code| is_safe_error_code(code))
    {
        return Some(code.to_string());
    }
    let code = body.pointer("/error/code")?;
    let text = match code {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        _ => return None,
    };
    if is_safe_error_code(&text) {
        Some(text)
    } else {
        None
    }
}

fn response_error_details(body: &Value) -> Option<String> {
    let status = body
        .pointer("/result/structuredContent/status")
        .and_then(Value::as_str);
    let message = body
        .pointer("/result/structuredContent/error/message")
        .and_then(Value::as_str)
        .or_else(|| body.pointer("/error/message").and_then(Value::as_str));
    match (status, message) {
        (Some(status), Some(message)) => Some(format!("status={status}; message={message}")),
        (Some(status), None) => Some(format!("status={status}")),
        (None, Some(message)) => Some(message.to_string()),
        (None, None) => None,
    }
}

fn is_safe_error_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 80
        && code
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
}

fn parse_topology_response(
    response: TopologyResponse,
    local_host_id: &str,
    fetched_at_ms: u64,
    ttl_ms: u64,
) -> Result<ProviderSnapshot> {
    let generation = response
        .nodes
        .iter()
        .map(|node| node.generation)
        .max()
        .unwrap_or(1)
        .max(1);
    let peers = response
        .nodes
        .into_iter()
        .filter(|node| node.host_id != local_host_id)
        .map(|node| -> Result<TopologyNode> {
            Ok(TopologyNode {
                host_id: node.host_id,
                gptadmin_relay_url: None,
                local_url: node
                    .local_url
                    .clone()
                    .or_else(|| node.peer_url.clone())
                    .ok_or_else(|| anyhow!("topology node has no local_url or peer_url"))?,
                routable_url: node
                    .peer_url
                    .or_else(|| node.local_url.clone())
                    .ok_or_else(|| anyhow!("topology node has no peer_url or local_url"))?,
                capabilities: node.capabilities,
                roots: node.roots,
                generation: node.generation.max(generation),
                fetched_at_ms,
                expires_at_ms: fetched_at_ms.saturating_add(ttl_ms),
                last_refresh_error: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let result = ProviderSnapshot {
        local_host_id: local_host_id.to_string(),
        generation,
        fetched_at_ms,
        ttl_ms,
        peers,
    };
    result.validate()?;
    Ok(result)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn browser_task_handles_instruction_bootstrap_and_reads_job_once() {
        use axum::{extract::State, http::StatusCode, routing::{get, post}, Json, Router};
        use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/mcp-relay/call", post(|State(calls): State<Arc<AtomicUsize>>, Json(body): Json<Value>| async move {
                assert_eq!(body["target"], "mcp:shell:node:GrepMesh");
                assert_eq!(body["tool"], "read_text");
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    (StatusCode::PRECONDITION_REQUIRED, Json(json!({"status":"failed","executed":false,
                        "error":{"code":"instructions_required"},"context":"fixture-context","instructions":"fixture"})))
                } else {
                    assert_eq!(body["context"], "fixture-context");
                    (StatusCode::OK, Json(json!({"status":"running","job_id":"job-fixture"})))
                }
            }))
            .route("/mcp-relay/job/:id", get(|| async { Json(json!({"status":"completed",
                "response":{"structuredContent":{"result":{"structuredContent":{"result":
                    {"content":[{"type":"text","text":"fixture"}]}}}}}})) }))
            .with_state(calls.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let hub = BrowserTaskClient { token:"fixture-secret-value".into(), endpoint:format!("http://{address}"),
            client:reqwest::Client::new(), receipts:Default::default(), targets:BTreeMap::from([("node".into(), BrowserRoute {
                shell_target:"shell:node".into(), child_ref:"GrepMesh".into(),
                execution_host:"local".into(), executor_host:"node".into() })]) };
        let result = hub.call("node", "read_text", json!({"host":"node","path":"/fixture"}),
            "fixture-read", Duration::from_secs(5), None).await.unwrap();
        assert_eq!(result["content"][0]["text"], "fixture");
        let again = hub.call("node", "read_text", json!({"host":"node","path":"/fixture"}),
            "fixture-read", Duration::from_secs(5), None).await.unwrap();
        assert_eq!(again["content"][0]["text"], "fixture");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();
    }

    #[tokio::test]
    async fn browser_task_never_retries_approval_refusal() {
        use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
        use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route("/mcp-relay/call", post(|State(calls): State<Arc<AtomicUsize>>| async move {
            calls.fetch_add(1, Ordering::SeqCst);
            (StatusCode::PRECONDITION_REQUIRED, Json(json!({"status":"approval_required","approval_id":"approval-fixture"})))
        })).with_state(calls.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let hub = BrowserTaskClient { token:"fixture-secret-value".into(), endpoint:format!("http://{address}"),
            client:reqwest::Client::new(), receipts:Default::default(), targets:BTreeMap::from([("node".into(), BrowserRoute {
                shell_target:"shell:node".into(), child_ref:"GrepMesh".into(),
                execution_host:"local".into(), executor_host:"node".into() })]) };
        let error = hub.call("node", "read_text", json!({}), "fixture", Duration::from_secs(5), None).await.unwrap_err();
        assert!(error.to_string().contains("approval-fixture"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test]
    async fn browser_task_keeps_denial_reason_without_retrying() {
        use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
        use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route("/mcp-relay/call", post(|State(calls): State<Arc<AtomicUsize>>| async move {
            calls.fetch_add(1, Ordering::SeqCst);
            (StatusCode::FORBIDDEN, Json(json!({"status":"failed","error":"personal MCP requires its exact owner profile"})))
        })).with_state(calls.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let hub = BrowserTaskClient { token:"fixture-secret-value".into(), endpoint:format!("http://{address}"),
            client:reqwest::Client::new(), receipts:Default::default(), targets:BTreeMap::from([("node".into(), BrowserRoute {
                shell_target:"shell:node".into(), child_ref:"GrepMesh".into(),
                execution_host:"local".into(), executor_host:"node".into() })]) };
        let error = hub.call("node", "read_text", json!({}), "fixture", Duration::from_secs(5), None).await.unwrap_err();
        assert!(error.to_string().contains("personal MCP requires its exact owner profile"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test]
    async fn registry_outage_preserves_cached_relay_routes_across_restart() {
        use axum::{http::StatusCode, routing::get, Json};
        let app = axum::Router::new()
            .route(
                "/mcp-relay/grepmesh",
                get(|| async {
                    Json(json!({"nodes":[
                        {"host_id":"B","peer_url":"http://direct-b/mcp","generation":9}
                    ]}))
                }),
            )
            .route(
                "/mcp-relay/agents",
                get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut cached = parse_topology_response(
            serde_json::from_value(json!({"nodes":[
                {"host_id":"B","peer_url":"http://old-b/mcp"},
                {"host_id":"C","peer_url":"http://old-c/mcp"}
            ]}))
            .unwrap(),
            "A",
            1,
            1,
        )
        .unwrap();
        for node in &mut cached.peers {
            node.gptadmin_relay_url = Some(format!("http://{address}/server/{}/mcp", node.host_id));
        }
        let folder = tempfile::tempdir_in(".tmp").unwrap();
        let path = folder.path().join("topology.json");
        TopologySnapshot::empty("A")
            .merge_fresh_provider_result(cached)
            .unwrap()
            .save_atomic(&path)
            .unwrap();
        let before = TopologySnapshot::load(&path).unwrap();
        let client = GptAdminTopologyClient::from_env(
            format!("http://{address}/mcp-relay/grepmesh"),
            "A",
            None,
            30_000,
        );
        client.refresh_cache(&before, Some(&path)).await.unwrap();
        let after = TopologySnapshot::load(&path).unwrap();
        assert_eq!(after.peers.len(), 2);
        assert_eq!(after.peer("B").unwrap().routable_url, "http://direct-b/mcp");
        assert_eq!(
            after.peer("B").unwrap().gptadmin_relay_url,
            before.peer("B").unwrap().gptadmin_relay_url
        );
        assert_eq!(after.peer("C").unwrap(), before.peer("C").unwrap());
        assert_eq!(after.peer("C").unwrap().expires_at_ms, 2);
        let known = crate::topology::Topology::from_snapshot(before, now_ms()).unwrap();
        let mut runtime = crate::topology::Topology::from_snapshot(after, now_ms()).unwrap();
        runtime
            .peers
            .iter_mut()
            .find(|p| p.host_id == "B")
            .unwrap()
            .gptadmin_relay_url = None;
        runtime.retain_known_peers(&known.peers);
        assert_eq!(
            runtime.peer("B").unwrap().gptadmin_relay_url,
            known.peer("B").unwrap().gptadmin_relay_url
        );
    }

    #[tokio::test]
    async fn mixed_projection_and_registry_preserve_direct_and_add_child_hosts() {
        use axum::{
            routing::{get, post},
            Json,
        };
        let app = axum::Router::new()
            .route("/mcp-relay/grepmesh", get(|| async { Json(json!({"nodes":[
                {"host_id":"B","peer_url":"http://direct-b/mcp","generation":9},
                {"host_id":"C","peer_url":"http://direct-c/mcp","generation":9}
            ]})) }))
            .route("/mcp-relay/agents", get(|| async { Json(json!({"agents":[
                {"name":"grepmesh","kind":"child_mcp","status":"online","meta":{"public_mcp_path":"/server/B/mcp"}},
                {"name":"grepmesh","kind":"child_mcp","status":"online","meta":{"public_mcp_path":"/server/D/mcp"}}
            ]})) }))
            .route("/server/:child/mcp", post(|axum::extract::Path(child): axum::extract::Path<String>| async move {
                Json(json!({"result":{"structuredContent":{"host_id":child}}}))
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = GptAdminTopologyClient::from_env(
            format!("http://{address}/mcp-relay/grepmesh"),
            "A",
            None,
            30_000,
        );
        let result = client.fetch().await.unwrap();
        assert_eq!(result.peers.len(), 3);
        let b = result.peers.iter().find(|p| p.host_id == "B").unwrap();
        assert_eq!(b.routable_url, "http://direct-b/mcp");
        assert_eq!(
            b.gptadmin_relay_url,
            Some(format!("http://{address}/server/B/mcp"))
        );
        assert!(result.peers.iter().any(|p| p.host_id == "D"));
    }

    #[tokio::test]
    async fn actual_registry_children_are_probed_locally_deduplicated_and_authenticated() {
        use axum::{
            http::HeaderMap,
            routing::{get, post},
            Json,
        };
        let app = axum::Router::new()
            .route("/mcp-relay/grepmesh", get(|| async { Json(json!({"nodes":[]})) }))
            .route("/mcp-relay/agents", get(|headers: HeaderMap| async move {
                assert_eq!(headers["authorization"], "Bearer hub-only");
                Json(json!({"agents":[
                    {"name":"grepmesh","kind":"child_mcp","status":"online","meta":{"public_mcp_path":"/server/b/mcp","public_mcp_endpoint":"https://untrusted.example/mcp"}},
                    {"name":"grepmesh","kind":"child_mcp","status":"online","meta":{"public_mcp_path":"/server/b-alias/mcp"}},
                    {"name":"grepmesh","kind":"child_mcp","status":"online","meta":{"public_mcp_path":"/server/self/mcp"}},
                    {"name":"grepmesh","kind":"child_mcp","status":"stale","meta":{"public_mcp_path":"/server/offline/mcp"}},
                ]}))
            }))
            .route("/server/:child/mcp", post(|axum::extract::Path(child): axum::extract::Path<String>, headers: HeaderMap, Json(body): Json<Value>| async move {
                assert_eq!(headers["authorization"], "Bearer hub-only");
                assert_eq!(body.pointer("/params/arguments/hosts"), Some(&json!("local")));
                assert_eq!(body.pointer("/params/arguments/hop_count"), Some(&json!(1)));
                assert_ne!(child, "offline");
                let host = if child == "self" { "A" } else { "B" };
                Json(json!({"result":{"isError":false,"content":[{"type":"text","text":json!({"host_id":host}).to_string()}]}}))
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut client = GptAdminTopologyClient::from_env(
            format!("http://{address}/mcp-relay/grepmesh"),
            "A",
            None,
            30_000,
        );
        client.token = Some("hub-only".into());
        let result = client.fetch().await.unwrap();
        assert_eq!(result.peers.len(), 1);
        assert_eq!(result.peers[0].host_id, "B");
        assert_eq!(
            result.peers[0].gptadmin_relay_url,
            Some(format!("http://{address}/server/b/mcp"))
        );
        let snapshot = TopologySnapshot::empty("A")
            .merge_fresh_provider_result(result)
            .unwrap();
        assert!(!serde_json::to_string(&snapshot)
            .unwrap()
            .contains("hub-only"));
        let mut partial_cache = snapshot.clone();
        let mut missing = partial_cache.peers[0].clone();
        missing.host_id = "previously-discovered".into();
        missing.fetched_at_ms = 1;
        missing.expires_at_ms = 2;
        partial_cache.peers.push(missing);
        let refreshed = client.refresh_cache(&partial_cache, None).await.unwrap();
        assert_eq!(
            refreshed
                .peer("previously-discovered")
                .unwrap()
                .expires_at_ms,
            2
        );
        assert_eq!(refreshed.peers.len(), 2);
        assert!(client
            .call_relay(
                "https://untrusted.example/server/b/mcp",
                "list_locations",
                json!({})
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn mixed_registry_probe_success_and_failure_is_not_a_fresh_snapshot() {
        use axum::{routing::post, Json};
        let app = axum::Router::new().route(
            "/server/:child/mcp",
            post(|axum::extract::Path(child): axum::extract::Path<String>| async move {
                if child == "failed" {
                    return Json(json!({"result":{"error":"child unavailable"}}));
                }
                Json(json!({"result":{"content":[{"type":"text","text":"{\"host_id\":\"B\"}"}]}}))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = GptAdminTopologyClient::from_env(
            format!("http://{address}/mcp-relay/agents"),
            "A",
            None,
            30_000,
        );

        let error = client
            .from_registry(json!({"agents":[
                {"name":"grepmesh","kind":"child_mcp","status":"online","meta":{"public_mcp_path":"/server/ok/mcp"}},
                {"name":"grepmesh","kind":"child_mcp","status":"online","meta":{"public_mcp_path":"/server/failed/mcp"}}
            ]}))
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("probe was partial"));
        assert!(error.contains("reachable: 1"));
        assert!(error.contains("failed"));
    }

    #[tokio::test]
    async fn empty_registry_does_not_erase_cache_and_redirects_are_not_followed() {
        use axum::routing::get;
        let app = axum::Router::new()
            .route(
                "/mcp-relay/agents",
                get(|| async { axum::Json(json!({"agents":[]})) }),
            )
            .route(
                "/redirect",
                get(|| async { axum::response::Redirect::temporary("/mcp-relay/agents") }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let current = TopologySnapshot::empty("A");
        let client = GptAdminTopologyClient::from_env(
            format!("http://{address}/mcp-relay/agents"),
            "A",
            None,
            30_000,
        );
        assert!(client.refresh_cache(&current, None).await.is_err());
        let redirect = GptAdminTopologyClient::from_env(
            format!("http://{address}/redirect"),
            "A",
            None,
            30_000,
        );
        assert!(redirect.fetch().await.is_err());
    }

    #[test]
    fn topology_response_excludes_local_and_preserves_peer_url() {
        let response: TopologyResponse = serde_json::from_value(json!({
            "nodes": [
                {"host_id":"A","local_url":"http://127.0.0.1:9419/mcp","peer_url":"https://192.0.2.10:9419/mcp","generation":4},
                {"host_id":"B","local_url":"http://127.0.0.1:9419/mcp","advertise_url":"https://192.0.2.11:9419/mcp","generation":5,"roots":["home"]}
            ]
        })).unwrap();
        let result = parse_topology_response(response, "A", 1_700_000_000_000, 30_000).unwrap();
        assert_eq!(result.peers.len(), 1);
        assert_eq!(result.peers[0].host_id, "B");
        assert_eq!(result.peers[0].routable_url, "https://192.0.2.11:9419/mcp");
    }

    #[test]
    fn endpoint_only_discovery_record_is_usable_for_both_urls() {
        let response: TopologyResponse = serde_json::from_value(json!({
            "nodes": [{
                "host_id": "B",
                "endpoint": "https://192.0.2.11:9419/mcp",
                "generation": 5,
                "capabilities": ["search_text"],
                "roots": ["home"]
            }]
        }))
        .unwrap();
        let result = parse_topology_response(response, "A", 1_700_000_000_000, 30_000).unwrap();
        assert_eq!(result.peers[0].local_url, result.peers[0].routable_url);
        assert_eq!(result.peers[0].routable_url, "https://192.0.2.11:9419/mcp");
    }

    #[tokio::test]
    async fn unavailable_registry_preserves_valid_control_plane_projection() {
        let app = axum::Router::new().route(
            "/mcp-relay/grepmesh",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({
                    "nodes": [{
                        "host_id": "B",
                        "local_url": "http://127.0.0.1:9419/mcp",
                        "peer_url": "http://127.0.0.1:29419/mcp",
                        "generation": 9
                    }]
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = GptAdminTopologyClient::from_env(
            format!("http://{address}/mcp-relay/grepmesh"),
            "A",
            None,
            30_000,
        );
        let result = client.fetch().await.unwrap();
        assert_eq!(result.peers[0].host_id, "B");
        assert_eq!(result.peers[0].routable_url, "http://127.0.0.1:29419/mcp");
    }

    #[tokio::test]
    async fn relay_retries_once_with_context_only_after_unexecuted_instruction_gate() {
        use axum::{extract::State, http::HeaderMap, routing::post, Json};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let attempts = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new()
            .route(
                "/server/mac-mini-grepmesh/mcp",
                post(
                    |State(attempts): State<Arc<AtomicUsize>>,
                     headers: HeaderMap,
                     Json(body): Json<Value>| async move {
                        assert_eq!(headers["authorization"], "Bearer hub-only");
                        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                        if attempt == 0 {
                            assert!(body.pointer("/params/_meta").is_none());
                            Json(json!({"jsonrpc":"2.0","id":1,"result":{
                                "isError":true,
                                "structuredContent":{
                                    "status":"failed",
                                    "executed":false,
                                    "error":{"code":"instructions_required"},
                                    "profile":"grepmesh-fleet",
                                    "instructions":"Use GrepMesh MCP before shell search.",
                                    "context":"signed-context"
                                },
                                "content":[]
                            }}))
                        } else {
                            assert_eq!(
                                body["params"]["_meta"]["io.gptadmin/context"],
                                "signed-context"
                            );
                            assert_eq!(body["params"]["name"], "list_locations");
                            assert_eq!(body["params"]["arguments"], json!({"hosts":"local"}));
                            Json(json!({"jsonrpc":"2.0","id":1,"result":{
                                "structuredContent":{"host_id":"mac-mini","partial":false}
                            }}))
                        }
                    },
                ),
            )
            .with_state(attempts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = GptAdminTopologyClient {
            endpoint: format!("http://{address}/mcp-relay/agents"),
            token: Some("hub-only".into()),
            local_host_id: "server-100".into(),
            ttl_ms: 30_000,
            client: reqwest::Client::new(),
        };

        let result = client
            .call_relay(
                &format!("http://{address}/server/mac-mini-grepmesh/mcp"),
                "list_locations",
                json!({"hosts":"local"}),
            )
            .await
            .unwrap();

        assert_eq!(result["structuredContent"]["host_id"], "mac-mini");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn relay_does_not_retry_instruction_error_without_unexecuted_proof() {
        use axum::{extract::State, routing::post, Json};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let attempts = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new()
            .route(
                "/server/mac-mini-grepmesh/mcp",
                post(|State(attempts): State<Arc<AtomicUsize>>,
                     Json(_body): Json<Value>| async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Json(json!({"jsonrpc":"2.0","id":1,"result":{
                        "isError":true,
                        "structuredContent":{
                            "status":"failed",
                            "executed":true,
                            "error":{"code":"instructions_required"},
                            "profile":"grepmesh-fleet",
                            "instructions":"Use GrepMesh MCP before shell search.",
                            "context":"must-not-be-retried"
                        },
                        "content":[]
                    }}))
                }),
            )
            .with_state(attempts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = GptAdminTopologyClient {
            endpoint: format!("http://{address}/mcp-relay/agents"),
            token: Some("hub-only".into()),
            local_host_id: "server-100".into(),
            ttl_ms: 30_000,
            client: reqwest::Client::new(),
        };

        let error = client
            .call_relay(
                &format!("http://{address}/server/mac-mini-grepmesh/mcp"),
                "list_locations",
                json!({"hosts":"local"}),
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("instructions_required"));
        assert!(error.to_string().contains("executed_true"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn relay_retries_once_when_only_the_required_proof_is_present() {
        use axum::{extract::State, http::HeaderMap, routing::post, Json};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let attempts = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new()
            .route(
                "/server/mac-mini-grepmesh/mcp",
                post(
                    |State(attempts): State<Arc<AtomicUsize>>,
                     headers: HeaderMap,
                     Json(body): Json<Value>| async move {
                        assert_eq!(headers["authorization"], "Bearer hub-only");
                        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                        if attempt == 0 {
                            // No profile or instruction text, but the refusal
                            // still carries the full retry proof.
                            Json(json!({"jsonrpc":"2.0","id":1,"result":{
                                "isError":true,
                                "structuredContent":{
                                    "status":"failed",
                                    "executed":false,
                                    "error":{"code":"instructions_required"},
                                    "context":"ic1_only-proof"
                                }
                            }}))
                        } else {
                            assert_eq!(
                                body["params"]["_meta"]["io.gptadmin/context"],
                                "ic1_only-proof"
                            );
                            assert_eq!(body["params"]["name"], "list_locations");
                            assert_eq!(
                                body["params"]["arguments"],
                                json!({"hosts":"local","hop_count":1})
                            );
                            Json(json!({"jsonrpc":"2.0","id":1,"result":{
                                "structuredContent":{"host_id":"mac-mini"}
                            }}))
                        }
                    },
                ),
            )
            .with_state(attempts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = GptAdminTopologyClient {
            endpoint: format!("http://{address}/mcp-relay/agents"),
            token: Some("hub-only".into()),
            local_host_id: "server-100".into(),
            ttl_ms: 30_000,
            client: reqwest::Client::new(),
        };

        let result = client
            .call_relay(
                &format!("http://{address}/server/mac-mini-grepmesh/mcp"),
                "list_locations",
                json!({"hosts":"local","hop_count":1}),
            )
            .await
            .unwrap();

        assert_eq!(result["structuredContent"]["host_id"], "mac-mini");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn relay_does_not_retry_other_child_failures_or_accepted_results() {
        use axum::{extract::State, routing::post, Json};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let attempts = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new()
            .route(
                "/server/mac-mini-grepmesh/mcp",
                post(
                    |State(attempts): State<Arc<AtomicUsize>>, Json(body): Json<Value>| async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        match body.pointer("/params/name").and_then(Value::as_str) {
                            Some("list_locations") => {
                                Json(json!({"jsonrpc":"2.0","id":1,"result":{
                                    "isError":true,
                                    "structuredContent":{
                                        "status":"failed",
                                        "executed":true,
                                        "error":{"code":"no_such_root"}
                                    }
                                }}))
                            }
                            _ => Json(json!({"jsonrpc":"2.0","id":1,"result":{
                                "isError":false,
                                "structuredContent":{"host_id":"mac-mini"}
                            }})),
                        }
                    },
                ),
            )
            .with_state(attempts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = GptAdminTopologyClient {
            endpoint: format!("http://{address}/mcp-relay/agents"),
            token: Some("hub-only".into()),
            local_host_id: "server-100".into(),
            ttl_ms: 30_000,
            client: reqwest::Client::new(),
        };
        let url = format!("http://{address}/server/mac-mini-grepmesh/mcp");

        let error = client
            .call_relay(&url, "list_locations", json!({"hosts":"local"}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("no_such_root"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        let accepted = client
            .call_relay(&url, "search_text", json!({"query":"needle"}))
            .await
            .unwrap();
        assert_eq!(accepted["structuredContent"]["host_id"], "mac-mini");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn relay_diagnostics_carry_profile_instructions_but_never_the_context_or_token() {
        use axum::{extract::State, routing::post, Json};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let attempts = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new()
            .route(
                "/server/mac-mini-grepmesh/mcp",
                post(
                    |State(attempts): State<Arc<AtomicUsize>>,
                     Json(_body): Json<Value>| async move {
                        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                        if attempt == 0 {
                            Json(json!({"jsonrpc":"2.0","id":1,"result":{
                                "isError":true,
                                "structuredContent":{
                                    "status":"failed",
                                    "executed":false,
                                    "error":{"code":"instructions_required"},
                                    "profile":"grepmesh-fleet",
                                    "instructions":"Read the profile instructions before calling GrepMesh.",
                                    "context":"ic1_super_secret_receipt"
                                }
                            }}))
                        } else {
                            Json(json!({"jsonrpc":"2.0","id":1,"result":{
                                "isError":true,
                                "structuredContent":{
                                    "status":"failed",
                                    "executed":true,
                                    "error":{
                                        "code":"child_unavailable",
                                        "message":"child failure context=ic1_super_secret_receipt token=hub-only"
                                    }
                                }
                            }}))
                        }
                    },
                ),
            )
            .with_state(attempts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = GptAdminTopologyClient {
            endpoint: format!("http://{address}/mcp-relay/agents"),
            token: Some("hub-only".into()),
            local_host_id: "server-100".into(),
            ttl_ms: 30_000,
            client: reqwest::Client::new(),
        };

        let error = client
            .call_relay(
                &format!("http://{address}/server/mac-mini-grepmesh/mcp"),
                "list_locations",
                json!({"hosts":"local"}),
            )
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("child_unavailable"));
        assert!(error.contains("status=failed"));
        assert!(error.contains("child failure"));
        assert!(error.contains("grepmesh-fleet"));
        assert!(error.contains("Read the profile instructions before calling GrepMesh."));
        assert!(!error.contains("ic1_super_secret_receipt"));
        assert!(!error.contains("hub-only"));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn short_context_is_redacted_from_diagnostics() {
        assert_eq!(
            super::safe_diagnostic("receipt=xyz", 32, Some("xyz"), None),
            "receipt=[redacted]"
        );
        let message =
            super::safe_diagnostic("token=very-long-secret", 8, None, Some("very-long-secret"));
        assert!(!message.contains("very-long-secret"));
    }

    #[test]
    fn instruction_gate_requires_the_hub_refusal_envelope() {
        let mut response = json!({"result":{
            "isError":true,
            "structuredContent":{
                "status":"failed",
                "executed":false,
                "error":{"code":"instructions_required"},
                "context":"ic1_signed"
            }
        }});
        assert!(super::instruction_gate(&response).is_some());

        response["result"]["isError"] = Value::Bool(false);
        assert!(super::instruction_gate(&response).is_none());
        response["result"]["isError"] = Value::Bool(true);
        response["result"]["structuredContent"]["status"] = Value::String("accepted".into());
        assert!(super::instruction_gate(&response).is_none());
    }

    #[tokio::test]
    async fn relay_retry_transport_error_keeps_instruction_diagnostics() {
        use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let attempts = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new()
            .route(
                "/server/mac-mini-grepmesh/mcp",
                post(|State(attempts): State<Arc<AtomicUsize>>, Json(_body): Json<Value>| async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        Json(json!({"jsonrpc":"2.0","id":1,"result":{
                            "isError":true,
                            "structuredContent":{
                                "status":"failed",
                                "executed":false,
                                "error":{"code":"instructions_required"},
                                "profile":"grepmesh-fleet",
                                "instructions":"Read the profile before retrying.",
                                "context":"ic1_transport_secret"
                            }
                        }})).into_response()
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE.into_response()
                    }
                }),
            )
            .with_state(attempts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = GptAdminTopologyClient {
            endpoint: format!("http://{address}/mcp-relay/agents"),
            token: Some("hub-only".into()),
            local_host_id: "server-100".into(),
            ttl_ms: 30_000,
            client: reqwest::Client::new(),
        };

        let error = client
            .call_relay(
                &format!("http://{address}/server/mac-mini-grepmesh/mcp"),
                "list_locations",
                json!({"hosts":"local"}),
            )
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("instructions_required"));
        assert!(error.contains("retry_http_status_503"));
        assert!(error.contains("grepmesh-fleet"));
        assert!(error.contains("Read the profile before retrying."));
        assert!(!error.contains("ic1_transport_secret"));
        assert!(!error.contains("hub-only"));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn registry_probe_failure_names_the_child_and_preserves_its_cause() {
        use axum::{routing::post, Json};

        let app = axum::Router::new().route(
            "/server/mac-mini-grepmesh/mcp",
            post(|| async {
                Json(json!({"jsonrpc":"2.0","id":1,"result":{
                    "isError":true,
                    "structuredContent":{
                        "status":"failed",
                        "executed":true,
                        "error":{"code":"child_auth_denied"}
                    }
                }}))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = GptAdminTopologyClient {
            endpoint: format!("http://{address}/mcp-relay/agents"),
            token: Some("hub-only".into()),
            local_host_id: "server-100".into(),
            ttl_ms: 30_000,
            client: reqwest::Client::new(),
        };

        let error = client
            .from_registry(json!({"agents":[{
                "kind":"child_mcp",
                "name":"GrepMesh",
                "status":"online",
                "meta":{"public_mcp_path":"/server/mac-mini-grepmesh/mcp"}
            }]}))
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("mac-mini-grepmesh"));
        assert!(error.contains("child_auth_denied"));
    }

    #[tokio::test]
    async fn registry_probe_budget_covers_both_instruction_round_trips() {
        use axum::{extract::State, routing::post, Json};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let attempts = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new()
            .route(
                "/server/mac-mini-grepmesh/mcp",
                post(
                    |State(attempts): State<Arc<AtomicUsize>>, Json(body): Json<Value>| async move {
                        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(1600)).await;
                        if attempt == 0 {
                            Json(json!({"jsonrpc":"2.0","id":1,"result":{
                                "isError":true,
                                "structuredContent":{
                                    "status":"failed",
                                    "executed":false,
                                    "error":{"code":"instructions_required"},
                                    "context":"ic1_probe_budget",
                                    "profile":"grepmesh-fleet",
                                    "instructions":"Read before retrying."
                                }
                            }}))
                        } else {
                            assert_eq!(
                                body["params"]["_meta"]["io.gptadmin/context"],
                                "ic1_probe_budget"
                            );
                            Json(json!({"jsonrpc":"2.0","id":1,"result":{
                                "structuredContent":{"host_id":"mac-mini"}
                            }}))
                        }
                    },
                ),
            )
            .with_state(attempts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = GptAdminTopologyClient {
            endpoint: format!("http://{address}/mcp-relay/agents"),
            token: Some("hub-only".into()),
            local_host_id: "server-100".into(),
            ttl_ms: 30_000,
            client: reqwest::Client::new(),
        };
        let started = tokio::time::Instant::now();

        let snapshot = client
            .from_registry(json!({"agents":[{
                "kind":"child_mcp",
                "name":"GrepMesh",
                "status":"online",
                "meta":{"public_mcp_path":"/server/mac-mini-grepmesh/mcp"}
            }]}))
            .await
            .unwrap();

        assert!(started.elapsed() > Duration::from_secs(3));
        assert_eq!(snapshot.peers.len(), 1);
        assert_eq!(snapshot.peers[0].host_id, "mac-mini");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn mismatched_instruction_envelope_is_diagnosed_without_retry() {
        use axum::{extract::State, routing::post, Json};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let attempts = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new()
            .route(
                "/server/mac-mini-grepmesh/mcp",
                post(|State(attempts): State<Arc<AtomicUsize>>, Json(_body): Json<Value>| async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Json(json!({"jsonrpc":"2.0","id":1,"result":{
                        "isError":true,
                        "structuredContent":{
                            "status":"unexpected",
                            "executed":false,
                            "error":{"code":"instructions_required"},
                            "profile":"grepmesh-fleet",
                            "instructions":"Check the current envelope shape.",
                            "context":"ic1_mismatched_shape"
                        }
                    }}))
                }),
            )
            .with_state(attempts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = GptAdminTopologyClient {
            endpoint: format!("http://{address}/mcp-relay/agents"),
            token: Some("hub-only".into()),
            local_host_id: "server-100".into(),
            ttl_ms: 30_000,
            client: reqwest::Client::new(),
        };

        let error = client
            .call_relay(
                &format!("http://{address}/server/mac-mini-grepmesh/mcp"),
                "list_locations",
                json!({"hosts":"local"}),
            )
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("envelope_shape_unmatched"));
        assert!(error.contains("grepmesh-fleet"));
        assert!(error.contains("Check the current envelope shape."));
        assert!(!error.contains("ic1_mismatched_shape"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn missing_retry_result_has_a_distinct_error_code() {
        assert_eq!(super::retry_error_code(&json!({})), "missing_result");
    }

    #[test]
    fn registry_scan_budget_scales_to_cover_each_bounded_probe() {
        assert_eq!(super::registry_scan_timeout(6), Duration::from_secs(38));
        assert_eq!(super::registry_scan_timeout(256), Duration::from_secs(50));
    }

    #[tokio::test]
    async fn initial_registry_transport_error_has_a_safe_classification() {
        use std::net::TcpListener as StdTcpListener;

        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let client = GptAdminTopologyClient {
            endpoint: format!("http://{address}/mcp-relay/agents"),
            token: Some("hub-only".into()),
            local_host_id: "server-100".into(),
            ttl_ms: 30_000,
            client: reqwest::Client::new(),
        };

        let error = client
            .from_registry(json!({"agents":[{
                "kind":"child_mcp",
                "name":"GrepMesh",
                "status":"online",
                "meta":{"public_mcp_path":"/server/mac-mini-grepmesh/mcp"}
            }]}))
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("mac-mini-grepmesh"));
        assert!(error.contains("initial_connect_failed"));
        assert!(!error.contains("hub-only"));
    }
}
