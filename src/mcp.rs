use crate::{
    backend::{
        dedup_hits, DirectoryEntry, DuplicateFileHash, DuplicateScan, LocalBackend, Location,
        PerHostStatus, ReadResponse, SearchHit, SearchMode, SearchResponse, StatusResponse,
    },
    ranked::{RankedCoverage, RankedHit, RankedOutcome, RankedRequest},
    topology::Topology,
};
use anyhow::{anyhow, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio::time::timeout;

pub use mesh_core::request::{normalize_request, HostsInput, NormalizedRequest};
#[cfg(test)]
use mesh_core::transport::validated_loopback_proxy_url;
use mesh_core::transport::{is_direct_connect_error, PeerTransport};

#[derive(Debug, Clone, Deserialize)]
pub struct SearchArgs {
    pub query: String,
    #[serde(default)]
    pub verbose: bool,
    #[serde(default)]
    pub hosts: Option<HostsInput>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub origin_host: Option<String>,
    #[serde(default)]
    pub hop_count: Option<u8>,
    #[serde(default, alias = "max_matches")]
    pub limit: Option<usize>,
    #[serde(default)]
    pub context_lines: Option<usize>,
    #[serde(default)]
    pub mode: SearchMode,
    #[serde(default)]
    pub path_globs: Vec<String>,
    #[serde(default)]
    pub roots: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) filesystem_acl: Option<FilesystemAcl>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RankedSearchArgs {
    pub query: String,
    #[serde(default)]
    pub hosts: Option<HostsInput>,
    #[serde(default)]
    pub roots: Vec<String>,
    #[serde(default)]
    pub path_globs: Vec<String>,
    #[serde(default, alias = "max_matches")]
    pub limit: Option<usize>,
    #[serde(default)]
    pub context_lines: Option<usize>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub origin_host: Option<String>,
    #[serde(default)]
    pub hop_count: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) filesystem_acl: Option<FilesystemAcl>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RankedIndexState {
    host_id: String,
    index_generation: u64,
    index_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RankedSearchResponse {
    request_id: String,
    origin_host: String,
    hop_count: u8,
    host_id: String,
    partial: bool,
    truncated: bool,
    coverage: RankedCoverage,
    results: Vec<RankedHit>,
    host_status: Vec<PerHostStatus>,
    index_status: Vec<RankedIndexState>,
    warnings: Vec<String>,
}

struct RankedHostResult {
    hits: Vec<RankedHit>,
    status: Vec<PerHostStatus>,
    index_status: Vec<RankedIndexState>,
    warnings: Vec<String>,
    partial: bool,
    truncated: bool,
    coverage: RankedCoverage,
}

/// The existing GPTAdmin filesystem ACL is carried only from the authenticated
/// shell queue. It is not a user tool argument or a service identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FilesystemAcl {
    version: u8,
    profile_id: String,
    profile_version: i64,
    roots_digest: String,
    job_id: String,
    target: String,
    tool: String,
    args_sha256: String,
    read_roots: Option<Vec<String>>,
    #[serde(default)]
    write_roots: Option<Vec<String>>,
}

impl FilesystemAcl {
    fn validate_read(&self) -> Result<&[String]> {
        let roots = self.read_roots.as_deref().unwrap_or_default();
        if self.version != 1
            || self.profile_id.trim().is_empty()
            || self.profile_version < 1
            || self.job_id.trim().is_empty()
            || self.target.trim().is_empty()
            || self.tool != "mcp_call"
            || !valid_sha256_hex(&self.roots_digest)
            || !valid_sha256_hex(&self.args_sha256)
            || roots.is_empty()
            || roots
                .iter()
                .any(|root| !std::path::Path::new(root).is_absolute())
        {
            anyhow::bail!("invalid or empty GPTAdmin filesystem read ACL")
        }
        Ok(roots)
    }

    fn allows_path(&self, path: &std::path::Path) -> bool {
        let Ok(roots) = self.validate_read() else {
            return false;
        };
        path.is_absolute()
            && !path
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir))
            && roots.iter().any(|root| path.starts_with(root))
    }
}

pub(crate) fn validate_filesystem_acl_value(value: &Value) -> Result<()> {
    serde_json::from_value::<FilesystemAcl>(value.clone())?.validate_read()?;
    Ok(())
}

fn valid_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Debug, Clone, Deserialize)]
pub struct FindPathsArgs {
    #[serde(alias = "pattern")]
    pub query: String,
    #[serde(default)]
    pub hosts: Option<HostsInput>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub origin_host: Option<String>,
    #[serde(default)]
    pub hop_count: Option<u8>,
    #[serde(default)]
    #[serde(alias = "max_matches")]
    pub limit: Option<usize>,
    #[serde(default)]
    pub roots: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FindDuplicatesArgs {
    #[serde(default)]
    pub hosts: Option<HostsInput>,
    pub roots: Vec<String>,
    #[serde(default)]
    pub paths: Vec<PathBuf>,
    #[serde(default)]
    pub max_files: Option<usize>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub origin_host: Option<String>,
    #[serde(default)]
    pub hop_count: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DuplicateGroup {
    sha256: String,
    files: Vec<DuplicateFileHash>,
    logical_duplicate_bytes: u64,
    physical_reclaimable_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DuplicateResult {
    groups: Vec<DuplicateGroup>,
    files: Vec<DuplicateFileHash>,
    scanned_files: usize,
    bytes_hashed: u64,
    skipped_oversized: usize,
    skipped_unsafe_paths: usize,
    partial: bool,
    truncated: bool,
    host_status: Vec<PerHostStatus>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReadTextArgs {
    pub host: String,
    pub path: PathBuf,
    #[serde(default)]
    pub start_line: Option<usize>,
    #[serde(default)]
    pub end_line: Option<usize>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub origin_host: Option<String>,
    #[serde(default)]
    pub hop_count: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) filesystem_acl: Option<FilesystemAcl>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StatusArgs {
    #[serde(default)]
    pub hosts: Option<HostsInput>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub origin_host: Option<String>,
    #[serde(default)]
    pub hop_count: Option<u8>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListLocationsArgs {
    #[serde(default)]
    pub hosts: Option<HostsInput>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub origin_host: Option<String>,
    #[serde(default)]
    pub hop_count: Option<u8>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListDirectoryArgs {
    pub host: String,
    pub path: PathBuf,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub origin_host: Option<String>,
    #[serde(default)]
    pub hop_count: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BrowseResponse<T> {
    request_id: String,
    origin_host: String,
    hop_count: u8,
    host_id: String,
    target_host_id: String,
    partial: bool,
    truncated: bool,
    host_status: Vec<PerHostStatus>,
    #[serde(skip_serializing, alias = "locations", alias = "entries")]
    items: Vec<T>,
}

fn browse_data<T: Serialize>(response: BrowseResponse<T>, field: &str) -> Result<Value> {
    let items = serde_json::to_value(&response.items)?;
    let mut value = serde_json::to_value(response)?;
    value[field] = items;
    Ok(value)
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolResult {
    pub request_id: String,
    pub origin_host: String,
    pub hop_count: u8,
    pub host_id: String,
    pub partial: bool,
    pub truncated: bool,
    pub data: Value,
    pub host_status: Vec<PerHostStatus>,
}

fn with_search_matches_alias<T: Serialize>(response: SearchResponse<T>) -> Result<Value> {
    let mut value = serde_json::to_value(response)?;
    if let Some(results) = value.get("results").cloned() {
        value["matches"] = results;
    }
    Ok(value)
}

#[derive(Debug, Clone, Serialize)]
struct CompactSearchResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    host: Option<String>,
    path: String,
    data: String,
    loc: String,
}

#[derive(Debug)]
struct CompactSearchRange {
    host_id: String,
    path: String,
    start_line: usize,
    end_line: usize,
    lines: BTreeMap<usize, String>,
}

fn compact_search_results(mut hits: Vec<SearchHit>) -> Vec<CompactSearchResult> {
    hits.sort_by(|a, b| {
        a.host_id
            .cmp(&b.host_id)
            .then(a.path.cmp(&b.path))
            .then(a.line_number.cmp(&b.line_number))
    });

    let mut ranges = Vec::<CompactSearchRange>::new();
    for hit in hits {
        let mut lines = hit.context;
        if lines.is_empty() && hit.line_number != 0 {
            lines.push(crate::backend::MatchLine {
                line_number: hit.line_number,
                text: hit.text,
            });
        }
        lines.sort_by_key(|line| line.line_number);
        let Some(start_line) = lines.first().map(|line| line.line_number) else {
            continue;
        };
        let end_line = lines
            .last()
            .map(|line| line.line_number)
            .unwrap_or(start_line);

        if let Some(previous) = ranges.last_mut().filter(|range| {
            range.host_id == hit.host_id
                && range.path == hit.path
                && start_line <= range.end_line.saturating_add(1)
        }) {
            previous.end_line = previous.end_line.max(end_line);
            for line in lines {
                previous.lines.entry(line.line_number).or_insert(line.text);
            }
        } else {
            let mut range_lines = BTreeMap::new();
            for line in lines {
                range_lines.entry(line.line_number).or_insert(line.text);
            }
            ranges.push(CompactSearchRange {
                host_id: hit.host_id,
                path: hit.path,
                start_line,
                end_line,
                lines: range_lines,
            });
        }
    }

    let mut hosts_by_path = BTreeMap::<String, BTreeSet<String>>::new();
    for range in &ranges {
        hosts_by_path
            .entry(range.path.clone())
            .or_default()
            .insert(range.host_id.clone());
    }
    ranges
        .into_iter()
        .map(|range| {
            let show_host = hosts_by_path
                .get(&range.path)
                .is_some_and(|hosts| hosts.len() > 1);
            CompactSearchResult {
                host: show_host.then_some(range.host_id),
                path: range.path,
                data: range.lines.into_values().collect::<Vec<_>>().join("\n"),
                loc: if range.start_line == range.end_line {
                    range.start_line.to_string()
                } else {
                    format!("{}-{}", range.start_line, range.end_line)
                },
            }
        })
        .collect()
}

fn render_search_response(response: SearchResponse<SearchHit>, verbose: bool) -> Result<Value> {
    if verbose {
        return with_search_matches_alias(response);
    }
    let SearchResponse {
        request_id,
        origin_host,
        hop_count,
        host_id,
        partial,
        truncated,
        results,
        host_status,
    } = response;
    Ok(serde_json::to_value(SearchResponse {
        request_id,
        origin_host,
        hop_count,
        host_id,
        partial,
        truncated,
        results: compact_search_results(results),
        host_status,
    })?)
}

pub fn compact_search_response_data(data: Value) -> Result<Value> {
    render_search_response(serde_json::from_value(data)?, false)
}

fn with_search_paths_alias<T: Serialize>(
    response: SearchResponse<T>,
    paths: Vec<Value>,
) -> Result<Value> {
    let mut value = serde_json::to_value(response)?;
    value["paths"] = Value::Array(paths);
    Ok(value)
}

#[derive(Clone)]
pub struct MeshService {
    pub local: LocalBackend,
    pub topology: Arc<RwLock<Topology>>,
    pub client: Client,
    peer_auth_token: Option<String>,
    gptadmin_client: Option<crate::gptadmin::GptAdminTopologyClient>,
    seen_requests: Arc<std::sync::Mutex<BTreeMap<String, Instant>>>,
}

impl MeshService {
    pub fn new(local: LocalBackend, topology: Topology) -> Self {
        Self {
            local,
            topology: Arc::new(RwLock::new(topology)),
            // Peer URLs are private mesh routes. They must not inherit a
            // desktop or launchd HTTP proxy that cannot reach the peer LAN.
            client: Client::builder()
                .no_proxy()
                .build()
                .expect("build direct peer HTTP client"),
            peer_auth_token: None,
            gptadmin_client: None,
            seen_requests: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
        }
    }

    pub fn with_peer_auth_token(mut self, token: Option<String>) -> Self {
        self.peer_auth_token = token.filter(|value| !value.trim().is_empty());
        self
    }

    pub fn with_gptadmin_client(
        mut self,
        client: Option<crate::gptadmin::GptAdminTopologyClient>,
    ) -> Self {
        self.gptadmin_client = client;
        self
    }

    pub fn replace_topology(&self, mut topology: Topology) {
        if let Ok(mut current) = self.topology.write() {
            topology.retain_known_peers(&current.peers);
            // GPTAdmin topology is intentionally a credential-free projection.
            // Retain only the local, optional loopback connector setting for a
            // known host when a refreshed projection omits it.
            for peer in &mut topology.peers {
                if peer.gptadmin_proxy_url.is_none() {
                    peer.gptadmin_proxy_url = current
                        .peer(&peer.host_id)
                        .and_then(|known| known.gptadmin_proxy_url.clone());
                }
            }
            *current = topology;
        }
    }

    fn known_host_ids(&self) -> Vec<String> {
        self.topology
            .read()
            .map(|topology| topology.known_host_ids().collect())
            .unwrap_or_else(|_| vec![self.local.host_id.clone()])
    }

    /// Resolve a user-facing search target once, before `SearchJobs` fans it
    /// out. This deliberately does not claim a request id: every background
    /// host call receives its own id and still uses the normal peer path.
    pub fn resolve_search_hosts(&self, args: &SearchArgs) -> Result<Vec<String>> {
        let normalized = normalize_request(
            &self.local.host_id,
            self.known_host_ids(),
            args.hosts.clone(),
            None,
            None,
            None,
        )?;
        Ok(unique_hosts(&normalized.hosts))
    }

    fn peer_config(&self, host_id: &str) -> Option<crate::topology::PeerConfig> {
        self.topology
            .read()
            .ok()
            .and_then(|topology| topology.peer(host_id).cloned())
    }

    fn claim_request(&self, request_id: &str) -> Result<()> {
        let mut seen = self
            .seen_requests
            .lock()
            .map_err(|_| anyhow!("request deduplication lock poisoned"))?;
        let now = Instant::now();
        seen.retain(|_, first_seen| now.duration_since(*first_seen) < Duration::from_secs(60));
        if seen.contains_key(request_id) {
            return Err(anyhow!("duplicate request_id {}", request_id));
        }
        if seen.len() >= 4096 {
            if let Some(oldest) = seen
                .iter()
                .min_by_key(|(_, first_seen)| *first_seen)
                .map(|(id, _)| id.clone())
            {
                seen.remove(&oldest);
            }
        }
        seen.insert(request_id.to_string(), now);
        Ok(())
    }

    pub async fn call_search(&self, args: SearchArgs) -> Result<ToolResult> {
        self.call_search_with_overall_timeout(
            args,
            Duration::from_millis(self.local.limits.overall_timeout_ms),
        )
        .await
    }

    /// Background jobs deliberately use their own bounded deadline. The
    /// interactive mesh timeout remains unchanged for ordinary callers.
    pub async fn call_search_with_overall_timeout(
        &self,
        args: SearchArgs,
        overall_timeout: Duration,
    ) -> Result<ToolResult> {
        let SearchArgs {
            query,
            verbose,
            hosts,
            request_id,
            origin_host,
            hop_count,
            limit,
            context_lines,
            mode,
            path_globs,
            roots,
            filesystem_acl,
        } = args;
        let read_roots = filesystem_acl
            .as_ref()
            .map(|acl| acl.validate_read().map(|roots| roots.to_vec()))
            .transpose()?;
        let normalized = normalize_request(
            &self.local.host_id,
            self.known_host_ids(),
            hosts,
            request_id,
            origin_host,
            hop_count,
        )?;
        self.claim_request(&normalized.request_id)?;
        let limit = limit
            .unwrap_or(self.local.limits.max_results)
            .min(self.local.limits.max_results);
        let context_lines = context_lines.unwrap_or(self.local.limits.context_lines);
        let (mut results, host_status, partial, mut truncated) = self
            .search_across(
                &normalized,
                &query,
                limit,
                context_lines,
                mode,
                path_globs,
                roots,
                filesystem_acl,
                read_roots,
                overall_timeout,
            )
            .await?;
        results.sort_by(|a, b| {
            a.host_id
                .cmp(&b.host_id)
                .then(a.path.cmp(&b.path))
                .then(a.line_number.cmp(&b.line_number))
        });
        results = dedup_hits(results, |item| {
            (item.host_id.clone(), item.path.clone(), item.line_number)
        });
        truncated |= results.len() > limit;
        results.truncate(limit);
        let request_id = normalized.request_id.clone();
        let origin_host = normalized.origin_host.clone();
        let hop_count = normalized.hop_count;
        let data = render_search_response(
            SearchResponse {
                request_id: request_id.clone(),
                origin_host: origin_host.clone(),
                hop_count,
                host_id: self.local.host_id.clone(),
                partial,
                truncated,
                results,
                host_status: host_status.clone(),
            },
            verbose,
        )?;
        Ok(self.wrap_result(
            request_id.clone(),
            origin_host.clone(),
            hop_count,
            partial,
            truncated,
            host_status.clone(),
            data,
        ))
    }

    pub async fn call_ranked_search(&self, args: RankedSearchArgs) -> Result<ToolResult> {
        self.call_ranked_search_with_overall_timeout(
            args,
            Duration::from_millis(self.local.limits.overall_timeout_ms),
        )
        .await
    }

    async fn call_ranked_search_with_overall_timeout(
        &self,
        args: RankedSearchArgs,
        overall_timeout: Duration,
    ) -> Result<ToolResult> {
        let RankedSearchArgs {
            query,
            hosts,
            roots,
            path_globs,
            limit,
            context_lines,
            request_id,
            origin_host,
            hop_count,
            filesystem_acl,
        } = args;
        let acl_read_roots = filesystem_acl
            .as_ref()
            .map(|acl| acl.validate_read().map(|roots| roots.to_vec()))
            .transpose()?;
        let normalized = normalize_request(
            &self.local.host_id,
            self.known_host_ids(),
            hosts,
            request_id,
            origin_host,
            hop_count,
        )?;
        self.claim_request(&normalized.request_id)?;
        let limit = limit
            .unwrap_or(self.local.limits.max_results)
            .min(self.local.limits.max_results);
        anyhow::ensure!(limit > 0, "ranked search limit must be at least one");
        let context_lines = context_lines
            .unwrap_or(self.local.limits.context_lines)
            .min(12);
        let local_read_roots = acl_read_roots.unwrap_or_else(|| {
            self.local
                .list_locations()
                .into_iter()
                .filter(|location| {
                    roots.is_empty() || roots.iter().any(|root| root == &location.name)
                })
                .map(|location| location.path)
                .collect()
        });

        let mut pending = FuturesUnordered::new();
        let mut pending_hosts = BTreeSet::new();
        for target in unique_hosts(&normalized.hosts) {
            pending_hosts.insert(target.clone());
            let service = self.clone();
            let query = query.clone();
            let roots = roots.clone();
            let path_globs = path_globs.clone();
            let filesystem_acl = filesystem_acl.clone();
            let local_read_roots = local_read_roots.clone();
            let request_id = normalized.request_id.clone();
            let origin_host = normalized.origin_host.clone();
            let base_hop_count = normalized.hop_count;
            pending.push(async move {
                let result = if target == service.local.host_id {
                    service
                        .local
                        .ranked_search_bounded(RankedRequest {
                            query,
                            roots,
                            read_roots: local_read_roots,
                            path_globs,
                            limit,
                            context_lines,
                            overall_timeout,
                        })
                        .await
                        .map(|outcome| ranked_local_result(&service.local.host_id, outcome))
                } else if filesystem_acl.is_none() {
                    Ok(RankedHostResult {
                        hits: Vec::new(),
                        status: vec![PerHostStatus::partial(
                            target.clone(),
                            "remote ranked search requires a verified GPTAdmin filesystem ACL",
                        )],
                        index_status: Vec::new(),
                        warnings: vec![
                            "remote ranked search requires a verified GPTAdmin filesystem ACL"
                                .into(),
                        ],
                        partial: true,
                        truncated: false,
                        coverage: RankedCoverage::NoCoverage,
                    })
                } else {
                    async {
                        let peer = service
                            .peer_config(&target)
                            .ok_or_else(|| anyhow!("unknown peer {target}"))?;
                        let hop_count =
                            hop_count_for(&service.local.host_id, &target, base_hop_count)?;
                        let value = service
                            .call_remote(
                                &peer,
                                "ranked_search",
                                json!({
                                    "query": query,
                                    "hosts": ["local"],
                                    "roots": roots,
                                    "path_globs": path_globs,
                                    "limit": limit,
                                    "context_lines": context_lines,
                                    "request_id": request_id,
                                    "origin_host": origin_host,
                                    "hop_count": hop_count,
                                    "filesystem_acl": filesystem_acl,
                                }),
                            )
                            .await?;
                        let mut response = decode_tool_result::<RankedSearchResponse>(value)?;
                        anyhow::ensure!(
                            response.host_id == target,
                            "ranked peer identity mismatch: expected {target}, got {}",
                            response.host_id
                        );
                        normalize_host_statuses(&mut response.host_status, response.partial);
                        for hit in &response.results {
                            anyhow::ensure!(
                                hit.host_id == target,
                                "ranked peer returned a hit for another host"
                            );
                            anyhow::ensure!(
                                filesystem_acl.as_ref().is_some_and(
                                    |acl| acl.allows_path(std::path::Path::new(&hit.path))
                                ),
                                "ranked peer returned a path outside the verified filesystem ACL"
                            );
                            // Ranked scores are query-relative and clamped to
                            // [0, 100] on every peer.
                            anyhow::ensure!(
                                hit.score.is_finite() && (0.0..=100.0).contains(&hit.score),
                                "ranked peer returned an invalid normalized score"
                            );
                        }
                        anyhow::ensure!(
                            response
                                .host_status
                                .iter()
                                .all(|status| status.host_id == target),
                            "ranked peer returned status for another host"
                        );
                        anyhow::ensure!(
                            response
                                .index_status
                                .iter()
                                .all(|status| status.host_id == target),
                            "ranked peer returned index state for another host"
                        );
                        Ok(RankedHostResult {
                            hits: response.results,
                            status: response.host_status,
                            index_status: response.index_status,
                            warnings: response.warnings,
                            partial: response.partial,
                            truncated: response.truncated,
                            coverage: response.coverage,
                        })
                    }
                    .await
                };
                (target, result)
            });
        }

        let mut results = Vec::new();
        let mut host_status = Vec::new();
        let mut index_status = Vec::new();
        let mut warnings = Vec::new();
        let mut partial = false;
        let mut coverage = RankedCoverage::NoCoverage;
        let mut truncated = false;
        let deadline = Instant::now() + overall_timeout;
        while !pending_hosts.is_empty() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                partial = true;
                host_status.extend(
                    pending_hosts
                        .iter()
                        .cloned()
                        .map(|host| PerHostStatus::failed(host, "overall timeout")),
                );
                break;
            }
            match timeout(remaining, pending.next()).await {
                Ok(Some((host, Ok(mut outcome)))) => {
                    pending_hosts.remove(&host);
                    partial |= outcome.partial || outcome.status.iter().any(|status| !status.ok);
                    truncated |= outcome.truncated;
                    coverage = merge_ranked_coverage(coverage, outcome.coverage);
                    for warning in outcome.warnings.drain(..) {
                        warnings.push(format!("{host}: {warning}"));
                    }
                    results.append(&mut outcome.hits);
                    index_status.append(&mut outcome.index_status);
                    if outcome.status.is_empty() {
                        outcome.status.push(if outcome.partial {
                            PerHostStatus::partial(host.clone(), "ranked coverage is partial")
                        } else {
                            PerHostStatus::complete(host.clone())
                        });
                    }
                    host_status.append(&mut outcome.status);
                }
                Ok(Some((host, Err(error)))) => {
                    pending_hosts.remove(&host);
                    partial = true;
                    host_status.push(PerHostStatus::failed(host.clone(), error.to_string()));
                    warnings.push(format!("{host}: {error}"));
                }
                Ok(None) => break,
                Err(_) => {
                    partial = true;
                    host_status.extend(
                        pending_hosts
                            .iter()
                            .cloned()
                            .map(|host| PerHostStatus::failed(host, "overall timeout")),
                    );
                    break;
                }
            }
        }

        results.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.host_id.cmp(&b.host_id))
                .then_with(|| a.path.cmp(&b.path))
                .then_with(|| a.line_number.cmp(&b.line_number))
        });
        let mut seen = BTreeSet::new();
        results.retain(|hit| seen.insert((hit.host_id.clone(), hit.path.clone(), hit.line_number)));
        truncated |= results.len() > limit;
        results.truncate(limit);
        partial |= host_status.iter().any(|status| !status.ok);
        let mut response = RankedSearchResponse {
            request_id: normalized.request_id.clone(),
            origin_host: normalized.origin_host.clone(),
            hop_count: normalized.hop_count,
            host_id: self.local.host_id.clone(),
            partial,
            truncated,
            coverage,
            results,
            host_status: host_status.clone(),
            index_status,
            warnings,
        };
        let max_response_bytes = self.local.limits.max_response_bytes;
        if max_response_bytes > 0 {
            for warning in &mut response.warnings {
                *warning = truncate_utf8(warning, 512);
            }
            for status in &mut response.host_status {
                if let Some(error) = status.error.as_mut() {
                    *error = truncate_utf8(error, 512);
                }
            }
            while ranked_tool_wire_size(&response)? > max_response_bytes {
                if response.results.pop().is_some() {
                    response.truncated = true;
                    continue;
                }
                if response.warnings.pop().is_some() {
                    response.truncated = true;
                    continue;
                }
                let mut shortened = false;
                for status in &mut response.host_status {
                    if let Some(error) = status.error.as_mut() {
                        if error.len() > 96 {
                            *error = truncate_utf8(error, 96);
                            shortened = true;
                        }
                    }
                }
                if shortened {
                    response.truncated = true;
                    continue;
                }
                anyhow::bail!(
                    "ranked response metadata exceeds configured max_response_bytes ({max_response_bytes})"
                );
            }
        }
        let partial = response.partial;
        let truncated = response.truncated;
        let host_status = response.host_status.clone();
        let data = serde_json::to_value(response)?;
        Ok(self.wrap_result(
            normalized.request_id,
            normalized.origin_host,
            normalized.hop_count,
            partial,
            truncated,
            host_status,
            data,
        ))
    }

    pub async fn call_find_paths(&self, args: FindPathsArgs) -> Result<ToolResult> {
        let FindPathsArgs {
            query,
            hosts,
            request_id,
            origin_host,
            hop_count,
            limit,
            roots,
        } = args;
        let normalized = normalize_request(
            &self.local.host_id,
            self.known_host_ids(),
            hosts,
            request_id,
            origin_host,
            hop_count,
        )?;
        self.claim_request(&normalized.request_id)?;
        let limit = limit
            .unwrap_or(self.local.limits.max_results)
            .min(self.local.limits.max_results);
        let (mut results, host_status, partial, mut truncated) = self
            .find_paths_across(&normalized, &query, limit, roots)
            .await?;
        results.sort_by(|a, b| {
            a.host_id
                .cmp(&b.host_id)
                .then(a.path.cmp(&b.path))
                .then(a.line_number.cmp(&b.line_number))
        });
        results = dedup_hits(results, |item| {
            (item.host_id.clone(), item.path.clone(), item.line_number)
        });
        truncated |= results.len() > limit;
        results.truncate(limit);
        let request_id = normalized.request_id.clone();
        let origin_host = normalized.origin_host.clone();
        let hop_count = normalized.hop_count;
        let paths = results
            .iter()
            .map(|item| {
                json!({
                    "host": item.host_id,
                    "path": item.path,
                    "kind": "file",
                })
            })
            .collect::<Vec<_>>();
        let data = with_search_paths_alias(
            SearchResponse {
                request_id: request_id.clone(),
                origin_host: origin_host.clone(),
                hop_count,
                host_id: self.local.host_id.clone(),
                partial,
                truncated,
                results,
                host_status: host_status.clone(),
            },
            paths,
        )?;
        Ok(self.wrap_result(
            request_id.clone(),
            origin_host.clone(),
            hop_count,
            partial,
            truncated,
            host_status.clone(),
            data,
        ))
    }

    pub async fn call_find_duplicates(&self, args: FindDuplicatesArgs) -> Result<ToolResult> {
        let FindDuplicatesArgs {
            hosts,
            roots,
            paths,
            max_files,
            request_id,
            origin_host,
            hop_count,
        } = args;
        anyhow::ensure!(!roots.is_empty(), "find_duplicates requires explicit roots");
        let normalized = normalize_request(
            &self.local.host_id,
            self.known_host_ids(),
            hosts,
            request_id,
            origin_host,
            hop_count,
        )?;
        self.claim_request(&normalized.request_id)?;
        let max_files = max_files
            .unwrap_or(100)
            .clamp(2, 250)
            .min(self.local.limits.max_results.max(2));
        let mut files = Vec::new();
        let mut statuses = Vec::new();
        let mut scanned_files = 0usize;
        let mut bytes_hashed = 0u64;
        let mut skipped_oversized = 0usize;
        let mut skipped_unsafe_paths = 0usize;
        let mut partial = false;
        let mut truncated = false;
        for target in unique_hosts(&normalized.hosts) {
            let scan: Result<DuplicateScan> = if target == self.local.host_id {
                self.local
                    .find_duplicates_bounded(max_files, roots.clone(), paths.clone())
                    .await
            } else {
                async {
                    let peer = self
                        .peer_config(&target)
                        .ok_or_else(|| anyhow!("unknown peer {target}"))?;
                    let value = self
                        .call_remote(
                            &peer,
                            "find_duplicates",
                            json!({
                                "hosts": ["local"],
                                "roots": roots.clone(),
                                "paths": paths.clone(),
                                "max_files": max_files,
                                "request_id": normalized.request_id.clone(),
                                "origin_host": normalized.origin_host.clone(),
                                "hop_count": hop_count_for(&self.local.host_id, &target, normalized.hop_count)?,
                            }),
                        )
                        .await
                        .map_err(|error| anyhow!("remote duplicate scan failed: {error}"))?;
                    decode_tool_result::<DuplicateResult>(value).map(|response| DuplicateScan {
                        files: response.files,
                        scanned_files: response.scanned_files,
                        bytes_hashed: response.bytes_hashed,
                        skipped_oversized: response.skipped_oversized,
                        skipped_unsafe_paths: response.skipped_unsafe_paths,
                        partial: response.partial,
                        truncated: response.truncated,
                    })
                }
                .await
            };
            match scan {
                Ok(scan) => {
                    scanned_files += scan.scanned_files;
                    bytes_hashed = bytes_hashed.saturating_add(scan.bytes_hashed);
                    skipped_oversized += scan.skipped_oversized;
                    skipped_unsafe_paths += scan.skipped_unsafe_paths;
                    partial |= scan.partial;
                    truncated |= scan.truncated;
                    files.extend(scan.files);
                    statuses.push(if scan.partial {
                        PerHostStatus::partial(target, "duplicate scan was partial")
                    } else {
                        PerHostStatus::complete(target)
                    });
                }
                Err(error) => {
                    partial = true;
                    statuses.push(PerHostStatus::failed(target, error.to_string()));
                }
            }
        }
        let all_files = files.clone();
        let mut by_hash = BTreeMap::<String, Vec<DuplicateFileHash>>::new();
        for file in files {
            by_hash.entry(file.sha256.clone()).or_default().push(file);
        }
        let mut groups = Vec::new();
        for (sha256, mut files) in by_hash {
            if files.len() < 2 {
                continue;
            }
            files.sort_by(|a, b| a.host_id.cmp(&b.host_id).then(a.path.cmp(&b.path)));
            let total_bytes = files.iter().map(|file| file.size).sum::<u64>();
            let largest = files.iter().map(|file| file.size).max().unwrap_or_default();
            let mut physical = BTreeMap::<String, (u64, u64, usize)>::new();
            let mut physical_known = true;
            for file in &files {
                let (Some(allocated_bytes), Some(physical_file_id), Some(hard_link_count)) = (
                    file.allocated_bytes,
                    file.physical_file_id.as_ref(),
                    file.hard_link_count,
                ) else {
                    physical_known = false;
                    continue;
                };
                let key = format!("{}:{}", file.host_id, physical_file_id);
                let entry = physical
                    .entry(key)
                    .or_insert((allocated_bytes, hard_link_count, 0));
                entry.2 += 1;
            }
            let external_link_bytes = physical
                .values()
                .filter(|(_, links, paths)| *links > *paths as u64)
                .map(|(bytes, _, _)| *bytes)
                .sum::<u64>();
            let physical_reclaimable_bytes = if physical_known {
                let allocated = physical.values().map(|(bytes, _, _)| *bytes).sum::<u64>();
                let retained = if external_link_bytes > 0 {
                    external_link_bytes
                } else {
                    physical
                        .values()
                        .map(|(bytes, _, _)| *bytes)
                        .max()
                        .unwrap_or_default()
                };
                Some(allocated.saturating_sub(retained))
            } else {
                None
            };
            groups.push(DuplicateGroup {
                sha256,
                files,
                logical_duplicate_bytes: total_bytes.saturating_sub(largest),
                physical_reclaimable_bytes,
            });
        }
        groups.sort_by(|a, b| {
            b.physical_reclaimable_bytes
                .unwrap_or_default()
                .cmp(&a.physical_reclaimable_bytes.unwrap_or_default())
                .then(a.sha256.cmp(&b.sha256))
        });
        let data = serde_json::to_value(DuplicateResult {
            groups,
            files: all_files,
            scanned_files,
            bytes_hashed,
            skipped_oversized,
            skipped_unsafe_paths,
            partial,
            truncated,
            host_status: statuses.clone(),
        })?;
        Ok(self.wrap_result(
            normalized.request_id.clone(),
            normalized.origin_host.clone(),
            normalized.hop_count,
            partial,
            truncated,
            statuses,
            data,
        ))
    }

    pub async fn call_read_text(&self, args: ReadTextArgs) -> Result<ToolResult> {
        let ReadTextArgs {
            host,
            path,
            start_line,
            end_line,
            request_id,
            origin_host,
            hop_count,
            filesystem_acl,
        } = args;
        if let Some(acl) = filesystem_acl.as_ref() {
            acl.validate_read()?;
            if !acl.allows_path(&path) {
                anyhow::bail!("filesystem ACL denies this document path")
            }
        }
        let normalized = normalize_request(
            &self.local.host_id,
            self.known_host_ids(),
            Some(HostsInput::One(host.clone())),
            request_id,
            origin_host,
            hop_count,
        )?;
        self.claim_request(&normalized.request_id)?;
        let target = normalized
            .hosts
            .first()
            .cloned()
            .unwrap_or_else(|| self.local.host_id.clone());
        if target != self.local.host_id && filesystem_acl.is_none() {
            anyhow::bail!("remote read requires a verified GPTAdmin filesystem ACL")
        }
        let response = if target == self.local.host_id {
            let chunks = self.local.read_text(&path, start_line, end_line)?;
            ReadResponse {
                request_id: normalized.request_id.clone(),
                origin_host: normalized.origin_host.clone(),
                hop_count: normalized.hop_count,
                host_id: self.local.host_id.clone(),
                target_host_id: self.local.host_id.clone(),
                partial: false,
                truncated: false,
                path: path.display().to_string(),
                start_line: start_line.unwrap_or(1),
                end_line: end_line.unwrap_or(usize::MAX),
                chunks,
                host_status: vec![PerHostStatus::complete(self.local.host_id.clone())],
            }
        } else {
            let peer = self
                .peer_config(&target)
                .ok_or_else(|| anyhow!("unknown peer {}", target))?;
            let value = self
                .call_remote(
                    &peer,
                    "read_text",
                    json!({
                        "host": "local",
                        "path": path,
                        "start_line": start_line,
                        "end_line": end_line,
                        "request_id": normalized.request_id.clone(),
                        "origin_host": normalized.origin_host.clone(),
                        "hop_count": 1u8,
                        "filesystem_acl": filesystem_acl,
                    }),
                )
                .await?;
            let mut response = decode_tool_result::<ReadResponse>(value)?;
            normalize_host_statuses(&mut response.host_status, response.partial);
            response.host_id = self.local.host_id.clone();
            response.target_host_id = target.clone();
            response
        };
        let host_status = response.host_status.clone();
        let request_id = normalized.request_id.clone();
        let origin_host = normalized.origin_host.clone();
        let hop_count = normalized.hop_count;
        Ok(self.wrap_result(
            request_id,
            origin_host,
            hop_count,
            response.partial,
            response.truncated,
            host_status,
            json!(response),
        ))
    }

    pub async fn call_list_locations(&self, args: ListLocationsArgs) -> Result<ToolResult> {
        let normalized = normalize_request(
            &self.local.host_id,
            self.known_host_ids(),
            args.hosts,
            args.request_id,
            args.origin_host,
            args.hop_count,
        )?;
        self.claim_request(&normalized.request_id)?;
        let mut locations = Vec::new();
        let mut host_status = Vec::new();
        let mut partial = false;
        for target in unique_hosts(&normalized.hosts) {
            if target == self.local.host_id {
                locations.extend(self.local.list_locations());
                host_status.push(PerHostStatus::complete(target));
                continue;
            }
            let result = async {
                let peer = self
                    .peer_config(&target)
                    .ok_or_else(|| anyhow!("unknown peer {}", target))?;
                let value = self
                    .call_remote(
                        &peer,
                        "list_locations",
                        json!({
                            "hosts": ["local"],
                            "request_id": normalized.request_id,
                            "origin_host": normalized.origin_host,
                            "hop_count": 1u8,
                        }),
                    )
                    .await?;
                let mut response = decode_tool_result::<BrowseResponse<Location>>(value)?;
                normalize_host_statuses(&mut response.host_status, response.partial);
                Ok::<_, anyhow::Error>(response)
            }
            .await;
            match result {
                Ok(response) => {
                    locations.extend(response.items);
                    if response.host_status.is_empty() {
                        host_status.push(if response.partial {
                            PerHostStatus::partial(target, "remote response was partial")
                        } else {
                            PerHostStatus::complete(target)
                        });
                    } else {
                        host_status.extend(
                            response
                                .host_status
                                .into_iter()
                                .map(|status| status.normalize_legacy(response.partial)),
                        );
                    }
                    partial |= response.partial;
                }
                Err(err) => {
                    partial = true;
                    host_status.push(PerHostStatus::failed(target, err.to_string()));
                }
            }
        }
        locations.sort_by(|a, b| a.host.cmp(&b.host).then(a.path.cmp(&b.path)));
        let response = BrowseResponse {
            request_id: normalized.request_id.clone(),
            origin_host: normalized.origin_host.clone(),
            hop_count: normalized.hop_count,
            host_id: self.local.host_id.clone(),
            target_host_id: self.local.host_id.clone(),
            partial,
            truncated: false,
            host_status: host_status.clone(),
            items: locations,
        };
        Ok(self.wrap_result(
            normalized.request_id,
            normalized.origin_host,
            normalized.hop_count,
            partial,
            false,
            host_status,
            browse_data(response, "locations")?,
        ))
    }

    pub async fn call_list_directory(&self, args: ListDirectoryArgs) -> Result<ToolResult> {
        let normalized = normalize_request(
            &self.local.host_id,
            self.known_host_ids(),
            Some(HostsInput::One(args.host.clone())),
            args.request_id,
            args.origin_host,
            args.hop_count,
        )?;
        self.claim_request(&normalized.request_id)?;
        let target = normalized
            .hosts
            .first()
            .cloned()
            .unwrap_or_else(|| self.local.host_id.clone());
        let response = if target == self.local.host_id {
            BrowseResponse {
                request_id: normalized.request_id.clone(),
                origin_host: normalized.origin_host.clone(),
                hop_count: normalized.hop_count,
                host_id: self.local.host_id.clone(),
                target_host_id: self.local.host_id.clone(),
                partial: false,
                truncated: false,
                host_status: vec![PerHostStatus::complete(self.local.host_id.clone())],
                items: self.local.list_directory(&args.path)?,
            }
        } else {
            let peer = self
                .peer_config(&target)
                .ok_or_else(|| anyhow!("unknown peer {}", target))?;
            let value = self
                .call_remote(
                    &peer,
                    "list_directory",
                    json!({
                        "host": "local",
                        "path": args.path,
                        "request_id": normalized.request_id,
                        "origin_host": normalized.origin_host,
                        "hop_count": 1u8,
                    }),
                )
                .await?;
            let mut response = decode_tool_result::<BrowseResponse<DirectoryEntry>>(value)?;
            normalize_host_statuses(&mut response.host_status, response.partial);
            response.host_id = self.local.host_id.clone();
            response.target_host_id = target;
            response
        };
        let host_status = response.host_status.clone();
        Ok(self.wrap_result(
            normalized.request_id,
            normalized.origin_host,
            normalized.hop_count,
            response.partial,
            response.truncated,
            host_status,
            browse_data(response, "entries")?,
        ))
    }

    pub async fn call_status(&self, args: StatusArgs) -> Result<ToolResult> {
        let StatusArgs {
            hosts,
            request_id,
            origin_host,
            hop_count,
        } = args;
        let normalized = normalize_request(
            &self.local.host_id,
            self.known_host_ids(),
            hosts,
            request_id,
            origin_host,
            hop_count,
        )?;
        self.claim_request(&normalized.request_id)?;
        let local = self.local.status()?;
        let mut nodes = vec![local.clone()];
        let mut host_status = vec![PerHostStatus::complete(self.local.host_id.clone())];
        let mut partial = false;

        if normalized.hop_count == 0 {
            for target in normalized
                .hosts
                .iter()
                .filter(|host| *host != &self.local.host_id)
            {
                match self.remote_status(target, &normalized).await {
                    Ok(response) => {
                        host_status.extend(response.host_status);
                        if response.nodes.is_empty() {
                            nodes.push(response.local);
                        } else {
                            nodes.extend(response.nodes);
                        }
                    }
                    Err(err) => {
                        partial = true;
                        host_status.push(PerHostStatus::failed(target.clone(), err.to_string()));
                    }
                }
            }
        }
        nodes.sort_by(|a, b| a.host_id.cmp(&b.host_id));
        nodes.dedup_by(|a, b| a.host_id == b.host_id);

        let request_id = normalized.request_id.clone();
        let origin_host = normalized.origin_host.clone();
        let hop_count = normalized.hop_count;
        Ok(self.wrap_result(
            request_id.clone(),
            origin_host.clone(),
            hop_count,
            partial,
            false,
            host_status.clone(),
            json!(StatusResponse {
                request_id,
                origin_host,
                hop_count,
                host_id: self.local.host_id.clone(),
                partial,
                host_status,
                local,
                nodes,
                topology: self
                    .topology
                    .read()
                    .map(|topology| topology.status())
                    .unwrap_or_default(),
            }),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn search_across(
        &self,
        normalized: &NormalizedRequest,
        query: &str,
        limit: usize,
        context_lines: usize,
        mode: SearchMode,
        path_globs: Vec<String>,
        roots: Vec<String>,
        filesystem_acl: Option<FilesystemAcl>,
        read_roots: Option<Vec<String>>,
        overall_timeout: Duration,
    ) -> Result<(Vec<SearchHit>, Vec<PerHostStatus>, bool, bool)> {
        let mut futures = Vec::new();
        for target in unique_hosts(&normalized.hosts) {
            let service = self.clone();
            let target_for_error = target.clone();
            let request_id = normalized.request_id.clone();
            let origin_host = normalized.origin_host.clone();
            let query = query.to_string();
            let mode = mode.clone();
            let path_globs = path_globs.clone();
            let roots = roots.clone();
            let filesystem_acl = filesystem_acl.clone();
            let read_roots = read_roots.clone();
            let hop_count = hop_count_for(&service.local.host_id, &target, normalized.hop_count)?;
            futures.push((target.clone(), async move {
                let result: Result<(Vec<SearchHit>, Vec<PerHostStatus>, bool, bool)> = if target
                    == service.local.host_id
                {
                    let outcome = service
                        .local
                        .search_text_with_overall_timeout(
                            &query,
                            limit,
                            context_lines,
                            mode.clone(),
                            path_globs.clone(),
                            roots.clone(),
                            overall_timeout,
                            read_roots.clone(),
                        )
                        .await?;
                    let truncated = outcome.truncated;
                    let partial = outcome.partial;
                    Ok((
                        outcome.hits,
                        vec![if partial {
                            PerHostStatus::partial(
                                target.clone(),
                                outcome.partial_error.unwrap_or_else(|| {
                                    "host returned a partial result".to_string()
                                }),
                            )
                        } else {
                            PerHostStatus::complete(target.clone())
                        }],
                        partial,
                        truncated,
                    ))
                } else {
                    if filesystem_acl.is_none() {
                        return Ok((
                            Vec::new(),
                            vec![PerHostStatus::partial(
                                target.clone(),
                                "remote search requires a verified GPTAdmin filesystem ACL",
                            )],
                            true,
                            false,
                        ));
                    }
                    let peer = service
                        .peer_config(&target)
                        .ok_or_else(|| anyhow!("unknown peer {}", target))?;
                    let value = service
                        .call_remote(
                            &peer,
                            "search_text",
                            json!({
                                "query": query,
                                "verbose": true,
                                "hosts": ["local"],
                                "request_id": request_id,
                                "origin_host": origin_host,
                                "hop_count": hop_count,
                                "limit": limit,
                                "context_lines": context_lines,
                                "mode": mode,
                                "path_globs": path_globs,
                                "roots": roots,
                                "filesystem_acl": filesystem_acl,
                                // Peer requests already run inside the
                                // caller's bounded search job. Starting a
                                // second default-500ms job on the peer returns
                                // an empty partial envelope instead of hits.
                                "wait_ms": 0,
                            }),
                        )
                        .await?;
                    let response = decode_tool_result::<SearchResponse<SearchHit>>(value)?;
                    let remote_status = if response.host_status.is_empty() {
                        vec![if response.partial {
                            PerHostStatus::partial(target.clone(), "remote response was partial")
                        } else {
                            PerHostStatus::complete(target.clone())
                        }]
                    } else {
                        response
                            .host_status
                            .into_iter()
                            .map(|status| status.normalize_legacy(response.partial))
                            .collect()
                    };
                    Ok((
                        response.results,
                        remote_status,
                        response.partial,
                        response.truncated,
                    ))
                };
                match result {
                    Ok(value) => Ok(value),
                    Err(err) => Ok((
                        Vec::new(),
                        vec![PerHostStatus::failed(target_for_error, err.to_string())],
                        true,
                        false,
                    )),
                }
            }));
        }
        self.join_hosts_with_timeout(futures, overall_timeout).await
    }

    async fn find_paths_across(
        &self,
        normalized: &NormalizedRequest,
        query: &str,
        limit: usize,
        roots: Vec<String>,
    ) -> Result<(Vec<SearchHit>, Vec<PerHostStatus>, bool, bool)> {
        let mut futures = Vec::new();
        for target in unique_hosts(&normalized.hosts) {
            let service = self.clone();
            let target_for_error = target.clone();
            let request_id = normalized.request_id.clone();
            let origin_host = normalized.origin_host.clone();
            let query = query.to_string();
            let roots = roots.clone();
            let hop_count = hop_count_for(&service.local.host_id, &target, normalized.hop_count)?;
            futures.push((target.clone(), async move {
                let result: Result<(Vec<SearchHit>, Vec<PerHostStatus>, bool, bool)> = if target
                    == service.local.host_id
                {
                    let outcome = service
                        .local
                        .find_paths_bounded(&query, limit, roots.clone())
                        .await?;
                    Ok((
                        outcome.hits,
                        vec![if outcome.partial {
                            PerHostStatus::partial(
                                target.clone(),
                                outcome.partial_error.unwrap_or_else(|| {
                                    "host returned a partial result".to_string()
                                }),
                            )
                        } else {
                            PerHostStatus::complete(target.clone())
                        }],
                        outcome.partial,
                        outcome.truncated,
                    ))
                } else {
                    let peer = service
                        .peer_config(&target)
                        .ok_or_else(|| anyhow!("unknown peer {}", target))?;
                    let value = service
                        .call_remote(
                            &peer,
                            "find_paths",
                            json!({
                                "query": query,
                                "hosts": ["local"],
                                "request_id": request_id,
                                "origin_host": origin_host,
                                "hop_count": hop_count,
                                "limit": limit,
                                "roots": roots,
                            }),
                        )
                        .await?;
                    let response = decode_tool_result::<SearchResponse<SearchHit>>(value)?;
                    let remote_status = if response.host_status.is_empty() {
                        vec![if response.partial {
                            PerHostStatus::partial(target.clone(), "remote response was partial")
                        } else {
                            PerHostStatus::complete(target.clone())
                        }]
                    } else {
                        response
                            .host_status
                            .into_iter()
                            .map(|status| status.normalize_legacy(response.partial))
                            .collect()
                    };
                    Ok((
                        response.results,
                        remote_status,
                        response.partial,
                        response.truncated,
                    ))
                };
                match result {
                    Ok(value) => Ok(value),
                    Err(err) => Ok((
                        Vec::new(),
                        vec![PerHostStatus::failed(target_for_error, err.to_string())],
                        true,
                        false,
                    )),
                }
            }));
        }
        self.join_hosts(futures).await
    }

    async fn remote_status(
        &self,
        target: &str,
        normalized: &NormalizedRequest,
    ) -> Result<StatusResponse> {
        let peer = self
            .peer_config(target)
            .ok_or_else(|| anyhow!("unknown peer {}", target))?;
        let value = self
            .call_remote(
                &peer,
                "search_status",
                json!({
                    "hosts": ["local"],
                    "request_id": normalized.request_id,
                    "origin_host": normalized.origin_host,
                    "hop_count": 1u8,
                }),
            )
            .await?;
        let mut response = decode_tool_result::<StatusResponse>(value)?;
        normalize_host_statuses(&mut response.host_status, response.partial);
        Ok(response)
    }

    async fn join_hosts<F>(
        &self,
        futures: Vec<(String, F)>,
    ) -> Result<(Vec<SearchHit>, Vec<PerHostStatus>, bool, bool)>
    where
        F: std::future::Future<Output = Result<(Vec<SearchHit>, Vec<PerHostStatus>, bool, bool)>>
            + Send,
    {
        self.join_hosts_with_timeout(
            futures,
            Duration::from_millis(self.local.limits.overall_timeout_ms),
        )
        .await
    }

    async fn join_hosts_with_timeout<F>(
        &self,
        futures: Vec<(String, F)>,
        overall_timeout: Duration,
    ) -> Result<(Vec<SearchHit>, Vec<PerHostStatus>, bool, bool)>
    where
        F: std::future::Future<Output = Result<(Vec<SearchHit>, Vec<PerHostStatus>, bool, bool)>>
            + Send,
    {
        let mut pending = FuturesUnordered::new();
        let mut pending_hosts = BTreeSet::new();
        for (host_id, future) in futures {
            pending_hosts.insert(host_id.clone());
            pending.push(async move { (host_id, future.await) });
        }
        let mut merged = Vec::new();
        let mut statuses = Vec::new();
        let mut partial = false;
        let mut truncated = false;
        let deadline = Instant::now() + overall_timeout;
        while !pending_hosts.is_empty() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                partial = true;
                for host_id in pending_hosts {
                    statuses.push(PerHostStatus::failed(host_id, "overall timeout"));
                }
                break;
            }
            match timeout(remaining, pending.next()).await {
                Ok(Some((host_id, result))) => {
                    pending_hosts.remove(&host_id);
                    match result {
                        Ok((mut items, mut host_status, host_partial, host_truncated)) => {
                            partial |= host_partial || host_status.iter().any(|status| !status.ok);
                            truncated |= host_truncated;
                            merged.append(&mut items);
                            if host_status.is_empty() {
                                host_status.push(if host_partial {
                                    PerHostStatus::partial(
                                        host_id,
                                        "host returned a partial result",
                                    )
                                } else {
                                    PerHostStatus::complete(host_id)
                                });
                            }
                            statuses.append(&mut host_status);
                        }
                        Err(err) => {
                            partial = true;
                            statuses.push(PerHostStatus::failed(host_id, err.to_string()));
                        }
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    partial = true;
                    for host_id in pending_hosts {
                        statuses.push(PerHostStatus::failed(host_id, "overall timeout"));
                    }
                    break;
                }
            }
        }
        Ok((merged, statuses, partial, truncated))
    }

    async fn call_remote(
        &self,
        peer: &crate::topology::PeerConfig,
        tool: &str,
        arguments: Value,
    ) -> Result<Value> {
        if peer.gptadmin_relay_url.as_deref() == Some(peer.routable_url.as_str()) {
            return self
                .gptadmin_client
                .as_ref()
                .ok_or_else(|| anyhow!("GPTAdmin relay credentials are not configured"))?
                .call_relay(&peer.routable_url, tool, arguments)
                .await;
        }
        match self
            .call_remote_direct(&peer.routable_url, tool, arguments.clone())
            .await
        {
            Ok(value) => Ok(value),
            Err(direct_error) if is_direct_connect_error(&direct_error) => {
                if let (Some(client), Some(url)) = (&self.gptadmin_client, &peer.gptadmin_relay_url)
                {
                    return client.call_relay(url, tool, arguments).await;
                }
                let proxy_url = peer
                    .gptadmin_proxy_url
                    .as_deref()
                    .filter(|url| !url.trim().is_empty())
                    .ok_or_else(|| {
                        anyhow!("direct transport unavailable; no GPTAdmin fallback is configured")
                    })?;
                self.call_remote_via_gptadmin_proxy(proxy_url, &peer.routable_url, tool, arguments)
                    .await
                    .map_err(|fallback_error| {
                        anyhow!(
                            "direct transport unavailable; GPTAdmin fallback failed: {:#}",
                            fallback_error
                        )
                    })
            }
            Err(error) => Err(error),
        }
    }

    fn peer_transport(&self) -> PeerTransport<'_> {
        PeerTransport {
            client: &self.client,
            peer_auth_token: self.peer_auth_token.as_deref(),
            max_response_bytes: self.local.limits.max_response_bytes,
            peer_timeout_ms: self.local.limits.peer_timeout_ms,
        }
    }
    async fn call_remote_direct(&self, url: &str, tool: &str, arguments: Value) -> Result<Value> {
        self.peer_transport()
            .call_remote_direct(url, tool, arguments)
            .await
    }
    async fn call_remote_via_gptadmin_proxy(
        &self,
        proxy: &str,
        url: &str,
        tool: &str,
        arguments: Value,
    ) -> Result<Value> {
        self.peer_transport()
            .call_remote_via_gptadmin_proxy(proxy, url, tool, arguments)
            .await
    }
    #[cfg(test)]
    fn redacted_outbound_error(error: &reqwest::Error) -> String {
        PeerTransport::redacted_outbound_error(error)
    }

    #[allow(clippy::too_many_arguments)]
    fn wrap_result(
        &self,
        request_id: String,
        origin_host: String,
        hop_count: u8,
        partial: bool,
        truncated: bool,
        host_status: Vec<PerHostStatus>,
        data: Value,
    ) -> ToolResult {
        ToolResult {
            request_id,
            origin_host,
            hop_count,
            host_id: self.local.host_id.clone(),
            partial,
            truncated,
            data,
            host_status,
        }
    }
}

fn unique_hosts(hosts: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for host in hosts {
        if seen.insert(host.clone()) {
            out.push(host.clone());
        }
    }
    out
}

fn hop_count_for(local: &str, target: &str, base: u8) -> Result<u8> {
    let hop = if target == local { base } else { base + 1 };
    if hop > 1 {
        Err(anyhow!("hop_count above one is rejected"))
    } else {
        Ok(hop)
    }
}

pub fn decode_tool_result<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T> {
    if let Some(content) = value.get("content").and_then(Value::as_array) {
        if let Some(first) = content.first() {
            if let Some(text) = first.get("text").and_then(Value::as_str) {
                return Ok(serde_json::from_str(text)?);
            }
        }
    }
    Ok(serde_json::from_value(value)?)
}

fn normalize_host_statuses(statuses: &mut [PerHostStatus], response_partial: bool) {
    for status in statuses {
        *status = status.clone().normalize_legacy(response_partial);
    }
}

fn ranked_local_result(host_id: &str, outcome: RankedOutcome) -> RankedHostResult {
    let status = if outcome.partial {
        PerHostStatus::partial(
            host_id,
            if outcome.warnings.is_empty() {
                "ranked coverage is partial".to_string()
            } else {
                outcome.warnings.join("; ")
            },
        )
    } else {
        PerHostStatus::complete(host_id)
    };
    RankedHostResult {
        hits: outcome.hits,
        status: vec![status],
        index_status: vec![RankedIndexState {
            host_id: host_id.to_string(),
            index_generation: outcome.index_generation,
            index_state: outcome.index_state,
        }],
        warnings: outcome.warnings,
        partial: outcome.partial,
        truncated: outcome.truncated,
        coverage: outcome.coverage,
    }
}

fn merge_ranked_coverage(a: RankedCoverage, b: RankedCoverage) -> RankedCoverage {
    match (a, b) {
        (RankedCoverage::StrongMatch, _) | (_, RankedCoverage::StrongMatch) => {
            RankedCoverage::StrongMatch
        }
        (RankedCoverage::WeakMatch, _) | (_, RankedCoverage::WeakMatch) => {
            RankedCoverage::WeakMatch
        }
        _ => RankedCoverage::NoCoverage,
    }
}

fn ranked_tool_wire_size(response: &RankedSearchResponse) -> Result<usize> {
    let text = serde_json::to_string(response)?;
    let tool_result = json!({
        "content": [{"type": "text", "text": text}],
        "isError": false
    });
    // Leave room for the enclosing JSON-RPC result and id fields.
    Ok(serde_json::to_vec(&tool_result)?.len().saturating_add(128))
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes.saturating_sub(3).min(value.len());
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}...", &value[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::HostSearchState;

    #[tokio::test]
    async fn lossless_body_copy_find_duplicates_returns_hashes_and_measured_reclaimable_bytes_without_deleting(
    ) {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("docs");
        std::fs::create_dir_all(&root).unwrap();
        let first = root.join("first.md");
        let second = root.join("second.md");
        std::fs::write(&first, b"same content").unwrap();
        std::fs::write(&second, b"same content").unwrap();
        let local = LocalBackend {
            host_id: "test".into(),
            root: root.clone(),
            root_paths: BTreeMap::from([("docs".into(), vec![root.clone()])]),
            limits: Default::default(),
            exclude_globs: Vec::new(),
            index: crate::index::IndexManager::disabled(),
            remote_index: None,
        };
        let service = MeshService::new(local, Topology::new("test", vec![]));
        let response = service
            .call_find_duplicates(FindDuplicatesArgs {
                hosts: Some(HostsInput::One("test".into())),
                roots: vec!["docs".into()],
                paths: vec![root],
                max_files: Some(10),
                request_id: None,
                origin_host: None,
                hop_count: None,
            })
            .await
            .unwrap();
        assert_eq!(response.data["groups"].as_array().unwrap().len(), 1);
        assert_eq!(response.data["scanned_files"], 2);
        assert_eq!(response.data["groups"][0]["logical_duplicate_bytes"], 12);
        let physical = response.data["groups"][0]["physical_reclaimable_bytes"].as_u64();
        let allocated = response.data["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|file| file["allocated_bytes"].as_u64())
            .collect::<Option<Vec<_>>>();
        if let (Some(physical), Some(allocated)) = (physical, allocated) {
            let allocated = allocated.into_iter().sum::<u64>();
            assert!(physical <= allocated);
        } else {
            assert!(cfg!(not(unix)));
        }
        assert!(first.exists() && second.exists());

        let outside = temp.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        let outside_file = outside.join("not-authorized.txt");
        std::fs::write(&outside_file, b"must not be hashed").unwrap();
        let denied = service
            .call_find_duplicates(FindDuplicatesArgs {
                hosts: Some(HostsInput::One("test".into())),
                roots: vec!["docs".into()],
                paths: vec![outside],
                max_files: Some(10),
                request_id: None,
                origin_host: None,
                hop_count: None,
            })
            .await
            .unwrap();
        assert_eq!(denied.partial, true);
        assert_eq!(denied.data["files"].as_array().unwrap().len(), 0);
        assert_eq!(denied.host_status[0].ok, false);
        assert!(outside_file.exists());
    }

    #[tokio::test]
    async fn background_deadline_reaches_local_scan_without_extending_interactive_calls() {
        // Focused integration, expected <1s/max5s. Real rg and local files;
        // zero interactive time makes the incorrect deadline reset deterministic.
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("needle.txt"), "deadline needle\n").unwrap();
        let limits = crate::config::LimitsConfig {
            overall_timeout_ms: 0,
            ..Default::default()
        };
        let local = LocalBackend {
            host_id: "test".into(),
            root: temp.path().into(),
            root_paths: BTreeMap::from([("local".into(), vec![temp.path().into()])]),
            limits,
            exclude_globs: vec![],
            index: crate::index::IndexManager::disabled(),
            remote_index: None,
        };
        let service = MeshService::new(local, Topology::new("test", vec![]));
        let args = || {
            serde_json::from_value::<SearchArgs>(json!({
                "query":"deadline needle", "hosts":["test"], "mode":"literal",
                "max_matches":5, "context_lines":0
            }))
            .unwrap()
        };
        let result = service
            .call_search_with_overall_timeout(args(), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(result.data["partial"], false);
        assert_eq!(result.data["results"].as_array().unwrap().len(), 1);
        let interactive = service.call_search(args()).await.unwrap();
        assert_eq!(interactive.data["partial"], true);
        assert!(interactive.data["results"].as_array().unwrap().is_empty());
    }

    fn hit(host: &str, path: &str, lines: &[(usize, &str)]) -> SearchHit {
        SearchHit {
            host_id: host.into(),
            path: path.into(),
            line_number: lines[0].0,
            context: lines
                .iter()
                .map(|(line_number, text)| crate::backend::MatchLine {
                    line_number: *line_number,
                    text: (*text).into(),
                })
                .collect(),
            text: lines[0].1.into(),
            column: 1,
            ..Default::default()
        }
    }

    fn response(results: Vec<SearchHit>) -> SearchResponse<SearchHit> {
        SearchResponse {
            request_id: "request".into(),
            origin_host: "A".into(),
            hop_count: 0,
            host_id: "A".into(),
            partial: false,
            truncated: false,
            results,
            host_status: vec![],
        }
    }

    #[test]
    fn legacy_peer_status_is_normalized_for_read_browse_and_status_responses() {
        let legacy_status = serde_json::json!({
            "host_id": "B",
            "ok": false,
            "error": "legacy partial response"
        });
        let mut read: ReadResponse = serde_json::from_value(serde_json::json!({
            "request_id": "request",
            "origin_host": "A",
            "hop_count": 1,
            "host_id": "B",
            "target_host_id": "B",
            "partial": true,
            "truncated": false,
            "path": "/tmp/example",
            "start_line": 1,
            "end_line": 1,
            "chunks": [],
            "host_status": [legacy_status.clone()]
        }))
        .unwrap();
        let mut browse: BrowseResponse<DirectoryEntry> =
            serde_json::from_value(serde_json::json!({
                "request_id": "request",
                "origin_host": "A",
                "hop_count": 1,
                "host_id": "B",
                "target_host_id": "B",
                "partial": true,
                "truncated": false,
                "entries": [],
                "host_status": [legacy_status.clone()]
            }))
            .unwrap();
        let mut status: StatusResponse = serde_json::from_value(serde_json::json!({
            "request_id": "request",
            "origin_host": "A",
            "hop_count": 1,
            "host_id": "B",
            "partial": true,
            "host_status": [legacy_status],
            "local": {
                "host_id": "B",
                "root": "/tmp",
                "backend": "rg",
                "file_count": 0
            },
            "nodes": [],
            "topology": {
                "freshness": null,
                "generation": 0,
                "last_refresh_error": null
            }
        }))
        .unwrap();

        normalize_host_statuses(&mut read.host_status, read.partial);
        normalize_host_statuses(&mut browse.host_status, browse.partial);
        normalize_host_statuses(&mut status.host_status, status.partial);

        for statuses in [&read.host_status, &browse.host_status, &status.host_status] {
            assert_eq!(statuses[0].state, Some(HostSearchState::Partial));
        }
    }

    #[test]
    fn search_text_defaults_to_compact_host_path_ranges() {
        let rendered = render_search_response(
            response(vec![
                hit(
                    "B",
                    "/same.rs",
                    &[
                        (204, "line 204"),
                        (205, "line 205"),
                        (206, "line 206"),
                        (207, "line 207"),
                    ],
                ),
                hit("A", "/other.rs", &[(9, "only line")]),
                hit("A", "/same.rs", &[(17, "line 17")]),
                hit("A", "/same.rs", &[(16, "line 16")]),
            ]),
            false,
        )
        .unwrap();

        assert_eq!(
            rendered["results"],
            serde_json::json!([
                {"path": "/other.rs", "data": "only line", "loc": "9"},
                {"host": "A", "path": "/same.rs", "data": "line 16\nline 17", "loc": "16-17"},
                {"host": "B", "path": "/same.rs", "data": "line 204\nline 205\nline 206\nline 207", "loc": "204-207"}
            ])
        );
        assert!(rendered.get("matches").is_none());
    }

    #[test]
    fn verbose_search_text_keeps_legacy_raw_results_and_matches_alias() {
        let rendered = render_search_response(
            response(vec![hit("A", "/same.rs", &[(16, "line 16")])]),
            true,
        )
        .unwrap();

        assert_eq!(rendered["results"][0]["line_number"], 16);
        assert_eq!(rendered["matches"], rendered["results"]);
        assert!(rendered["results"][0].get("data").is_none());
    }

    #[tokio::test]
    async fn outbound_reqwest_error_summary_is_classified_and_redacted() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!(
            "http://127.0.0.1:{}/private-token-must-not-leak",
            listener.local_addr().unwrap().port()
        );
        drop(listener);
        let error = Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(&url)
            .send()
            .await
            .unwrap_err();

        let summary = MeshService::redacted_outbound_error(&error);
        assert!(
            summary.starts_with("send remote request: connect"),
            "{summary}"
        );
        assert!(summary.contains("io_kind=ConnectionRefused"), "{summary}");
        assert!(summary.contains("errno="), "{summary}");
        assert!(!summary.contains("127.0.0.1"), "{summary}");
        assert!(
            !summary.contains("private-token-must-not-leak"),
            "{summary}"
        );
    }

    #[test]
    fn gptadmin_proxy_url_rejects_non_root_path() {
        assert!(validated_loopback_proxy_url("http://127.0.0.1:3126/tunnel").is_err());
        assert!(validated_loopback_proxy_url("http://127.0.0.1:3126/").is_ok());
    }
}
