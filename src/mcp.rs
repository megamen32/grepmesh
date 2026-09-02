use crate::{
    backend::{
        dedup_hits, DirectoryEntry, LocalBackend, Location, PerHostStatus, ReadResponse, SearchHit,
        SearchMode, SearchResponse, StatusResponse,
    },
    topology::Topology,
};
use anyhow::{anyhow, Context, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    path::PathBuf,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio::time::timeout;

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum HostsInput {
    One(String),
    Many(Vec<String>),
}

impl HostsInput {
    fn into_vec(self) -> Vec<String> {
        match self {
            HostsInput::One(v) => vec![v],
            HostsInput::Many(v) => v,
        }
    }
}

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

#[derive(Debug, Clone)]
pub struct NormalizedRequest {
    pub request_id: String,
    pub origin_host: String,
    pub hop_count: u8,
    pub hosts: Vec<String>,
}

pub fn normalize_request(
    local_host_id: &str,
    known_hosts: impl IntoIterator<Item = String>,
    hosts: Option<HostsInput>,
    request_id: Option<String>,
    origin_host: Option<String>,
    hop_count: Option<u8>,
) -> Result<NormalizedRequest> {
    let hop_count = hop_count.unwrap_or(0);
    if hop_count > 1 {
        return Err(anyhow!("hop_count above one is rejected"));
    }

    let mut known: BTreeSet<String> = known_hosts.into_iter().collect();
    known.insert(local_host_id.to_string());
    let requested = hosts.unwrap_or(HostsInput::One("*".into())).into_vec();
    let request_id = request_id.unwrap_or_else(|| format!("{}-{}", local_host_id, fresh_id()));
    let origin_host = origin_host.unwrap_or_else(|| local_host_id.to_string());

    let hosts = if hop_count == 1 {
        if requested != vec!["local".to_string()] {
            return Err(anyhow!("peer calls are local-only"));
        }
        vec![local_host_id.to_string()]
    } else if requested.len() == 1 && requested[0] == "local" {
        vec![local_host_id.to_string()]
    } else if requested.len() == 1 && requested[0] == "*" {
        known.into_iter().collect()
    } else {
        let mut dedup = BTreeSet::new();
        let mut out = Vec::new();
        for host in requested {
            let target = if host == "local" {
                local_host_id.to_string()
            } else {
                host
            };
            if !known.contains(&target) {
                return Err(anyhow!("unknown host {}", target));
            }
            if dedup.insert(target.clone()) {
                out.push(target);
            }
        }
        out
    };

    Ok(NormalizedRequest {
        request_id,
        origin_host,
        hop_count,
        hosts,
    })
}

fn fresh_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{:x}{:x}", d.as_secs(), d.subsec_nanos())
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
            seen_requests: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
        }
    }

    pub fn with_peer_auth_token(mut self, token: Option<String>) -> Self {
        self.peer_auth_token = token.filter(|value| !value.trim().is_empty());
        self
    }

    pub fn replace_topology(&self, mut topology: Topology) {
        if let Ok(mut current) = self.topology.write() {
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

    pub async fn call_read_text(&self, args: ReadTextArgs) -> Result<ToolResult> {
        let ReadTextArgs {
            host,
            path,
            start_line,
            end_line,
            request_id,
            origin_host,
            hop_count,
        } = args;
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
            let hop_count = hop_count_for(&service.local.host_id, &target, normalized.hop_count)?;
            futures.push((target.clone(), async move {
                let result: Result<(Vec<SearchHit>, Vec<PerHostStatus>, bool, bool)> = if target
                    == service.local.host_id
                {
                    let outcome = service
                        .local
                        .search_text_bounded(
                            &query,
                            limit,
                            context_lines,
                            mode.clone(),
                            path_globs.clone(),
                            roots.clone(),
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
        match self
            .call_remote_direct(&peer.routable_url, tool, arguments.clone())
            .await
        {
            Ok(value) => Ok(value),
            Err(direct_error) if is_direct_connect_error(&direct_error) => {
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

    async fn call_remote_direct(&self, url: &str, tool: &str, arguments: Value) -> Result<Value> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        });
        let max_response_bytes = self.local.limits.max_response_bytes;
        let value = timeout(
            Duration::from_millis(self.local.limits.peer_timeout_ms),
            async {
                let mut request = self
                    .client
                    .post(url)
                    .header("Accept", "application/json, text/event-stream")
                    .header("MCP-Protocol-Version", "2026-07-28")
                    .header("Mcp-Method", "tools/call")
                    .header("Mcp-Name", tool);
                if let Some(token) = self.peer_auth_token.as_deref() {
                    request = request.bearer_auth(token);
                }
                let resp = request
                    .json(&payload)
                    .send()
                    .await
                    .map_err(|error| anyhow!(Self::redacted_outbound_error(&error)))?;
                if !resp.status().is_success() {
                    return Err(anyhow!("remote HTTP status {}", resp.status()));
                }
                let mut stream = resp.bytes_stream();
                let mut body = Vec::new();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.context("read remote response")?;
                    if max_response_bytes != 0
                        && body.len().saturating_add(chunk.len()) > max_response_bytes
                    {
                        return Err(anyhow!(
                            "remote response exceeds max_response_bytes ({max_response_bytes})"
                        ));
                    }
                    body.extend_from_slice(&chunk);
                }
                serde_json::from_slice::<Value>(&body).context("parse remote response")
            },
        )
        .await??;
        if let Some(err) = value.get("error") {
            return Err(anyhow!("remote error: {}", err));
        }
        Ok(value.get("result").cloned().unwrap_or(value))
    }

    async fn call_remote_via_gptadmin_proxy(
        &self,
        proxy_url: &str,
        peer_url: &str,
        tool: &str,
        arguments: Value,
    ) -> Result<Value> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let proxy = validated_loopback_proxy_url(proxy_url)?;
        let peer = reqwest::Url::parse(peer_url).context("parse peer URL for GPTAdmin fallback")?;
        if peer.scheme() != "http" {
            return Err(anyhow!(
                "GPTAdmin fallback supports only http peer URLs; got {}",
                peer.scheme()
            ));
        }
        let host = peer
            .host_str()
            .ok_or_else(|| anyhow!("peer URL has no host"))?;
        let port = peer
            .port_or_known_default()
            .ok_or_else(|| anyhow!("peer URL has no port"))?;
        let target = format!("{host}:{port}");
        let proxy_host = proxy
            .host_str()
            .ok_or_else(|| anyhow!("GPTAdmin proxy URL has no host"))?;
        let proxy_port = proxy.port_or_known_default().unwrap_or(80);
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        })
        .to_string();
        let path = match peer.query() {
            Some(query) => format!("{}?{query}", peer.path()),
            None => peer.path().to_string(),
        };
        timeout(
            Duration::from_millis(self.local.limits.peer_timeout_ms),
            async {
                let mut stream = TcpStream::connect((proxy_host, proxy_port))
                    .await
                    .context("connect to GPTAdmin loopback proxy")?;
                let connect = format!(
                    "CONNECT {target} HTTP/1.1\r\nHost: {target}\r\nConnection: keep-alive\r\n\r\n"
                );
                stream
                    .write_all(connect.as_bytes())
                    .await
                    .context("write proxy CONNECT")?;
                let connect_response = read_http_headers(&mut stream).await?;
                if !connect_response.starts_with("HTTP/1.1 200")
                    && !connect_response.starts_with("HTTP/1.0 200")
                {
                    return Err(anyhow!("GPTAdmin proxy CONNECT was rejected"));
                }
                let mut request = format!(
                    "POST {path} HTTP/1.1\r\nHost: {target}\r\nAccept: application/json, text/event-stream\r\nMCP-Protocol-Version: 2026-07-28\r\nMcp-Method: tools/call\r\nMcp-Name: {tool}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
                    payload.len()
                );
                if let Some(token) = self.peer_auth_token.as_deref() {
                    request.push_str(&format!("Authorization: Bearer {token}\r\n"));
                }
                request.push_str("\r\n");
                stream
                    .write_all(request.as_bytes())
                    .await
                    .context("write tunneled request")?;
                stream
                    .write_all(payload.as_bytes())
                    .await
                    .context("write tunneled payload")?;
                let headers = read_http_headers(&mut stream).await?;
                if !headers.starts_with("HTTP/1.1 2") && !headers.starts_with("HTTP/1.0 2") {
                    return Err(anyhow!("remote HTTP status via GPTAdmin fallback"));
                }
                let max_response_bytes = self.local.limits.max_response_bytes;
                let mut body = Vec::new();
                let mut chunk = [0; 8 * 1024];
                loop {
                    let read = stream
                        .read(&mut chunk)
                        .await
                        .context("read tunneled response")?;
                    if read == 0 {
                        break;
                    }
                    if max_response_bytes != 0
                        && body.len().saturating_add(read) > max_response_bytes
                    {
                        return Err(anyhow!(
                            "remote response exceeds max_response_bytes ({max_response_bytes})"
                        ));
                    }
                    body.extend_from_slice(&chunk[..read]);
                }
                let value =
                    serde_json::from_slice::<Value>(&body).context("parse tunneled remote response")?;
                if let Some(err) = value.get("error") {
                    return Err(anyhow!("remote error: {}", err));
                }
                Ok(value.get("result").cloned().unwrap_or(value))
            },
        )
        .await
        .context("GPTAdmin fallback timeout")?
    }

    fn redacted_outbound_error(error: &reqwest::Error) -> String {
        let phase = if error.is_timeout() {
            "timeout"
        } else if error.is_connect() {
            "connect"
        } else if error.is_body() {
            "body"
        } else if error.is_request() {
            "request"
        } else {
            "transport"
        };
        let mut source = error.source();
        while let Some(cause) = source {
            if let Some(io_error) = cause.downcast_ref::<std::io::Error>() {
                let mut summary = format!(
                    "send remote request: {phase}; io_kind={:?}",
                    io_error.kind()
                );
                if let Some(errno) = io_error.raw_os_error() {
                    summary.push_str(&format!("; errno={errno}"));
                }
                return summary;
            }
            source = cause.source();
        }
        format!("send remote request: {phase}")
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

fn is_direct_connect_error(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .starts_with("send remote request: connect")
}

fn validated_loopback_proxy_url(value: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(value.trim()).context("parse GPTAdmin proxy URL")?;
    let loopback = matches!(
        url.host_str(),
        Some("127.0.0.1") | Some("::1") | Some("[::1]")
    );
    if url.scheme() != "http"
        || !loopback
        || url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(anyhow!(
            "GPTAdmin fallback proxy must be a credential-free http loopback URL with no path"
        ));
    }
    Ok(url)
}

async fn read_http_headers(stream: &mut tokio::net::TcpStream) -> Result<String> {
    use tokio::io::AsyncReadExt;
    let mut headers = Vec::new();
    while headers.len() < 32 * 1024 {
        let mut byte = [0u8; 1];
        stream
            .read_exact(&mut byte)
            .await
            .context("read HTTP headers")?;
        headers.push(byte[0]);
        if headers.ends_with(b"\r\n\r\n") {
            return String::from_utf8(headers).context("decode HTTP headers");
        }
    }
    Err(anyhow!("HTTP headers exceed 32768 bytes"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::HostSearchState;

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
