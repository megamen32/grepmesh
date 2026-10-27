use crate::config::{default_exclude_globs, LimitsConfig};
use crate::index::IndexManager;
use anyhow::{anyhow, Context, Result};
use globset::Glob;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufRead, BufReader as StdBufReader, Read},
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command as TokioCommand,
    time::{timeout, Instant},
};

const MAX_BOUNDED_TEXT_READ_BYTES: usize = 1024 * 1024;

fn rg_command() -> Option<TokioCommand> {
    let sibling = std::env::current_exe().ok().and_then(|exe| {
        let name = if cfg!(windows) { "rg.exe" } else { "rg" };
        exe.parent().map(|parent| parent.join(name))
    });
    match sibling.filter(|path| path.is_file()) {
        Some(path) => Some(TokioCommand::new(path)),
        None => {
            let name = if cfg!(windows) { "rg.exe" } else { "rg" };
            executable_on_path(name).map(TokioCommand::new)
        }
    }
}

fn executable_on_path(name: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path).find_map(|directory| {
            let candidate = directory.join(name);
            candidate.is_file().then_some(candidate)
        })
    })
}

fn grep_command() -> TokioCommand {
    TokioCommand::new("grep")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchLine {
    pub line_number: usize,
    pub text: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchHit {
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub modified_ms: Option<u64>,
    #[serde(default)]
    pub created_ms: Option<u64>,
    pub host_id: String,
    pub path: String,
    pub line_number: usize,
    pub context: Vec<MatchLine>,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub column: usize,
}

#[derive(Debug)]
pub struct SearchOutcome {
    pub hits: Vec<SearchHit>,
    pub truncated: bool,
    pub partial: bool,
    pub partial_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateFileHash {
    pub host_id: String,
    pub path: String,
    pub size: u64,
    pub sha256: String,
    pub allocated_bytes: Option<u64>,
    pub physical_file_id: Option<String>,
    pub hard_link_count: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateScan {
    pub files: Vec<DuplicateFileHash>,
    pub scanned_files: usize,
    pub bytes_hashed: u64,
    pub skipped_oversized: usize,
    pub skipped_unsafe_paths: usize,
    pub partial: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SearchMode {
    #[default]
    Literal,
    Regex,
    CaseInsensitiveLiteral,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexState {
    Ready,
    Building,
    Degraded,
    Corrupt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadChunk {
    pub start_line: usize,
    pub end_line: usize,
    pub lines: Vec<MatchLine>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Location {
    pub host: String,
    pub name: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectoryEntry {
    pub name: String,
    pub path: String,
    pub kind: String,
    pub size: Option<u64>,
    pub modified_ms: Option<u64>,
    #[serde(default)]
    pub created_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostStatus {
    #[serde(default)]
    pub roots: Vec<String>,
    #[serde(default)]
    pub index_current_path: Option<String>,
    #[serde(default)]
    pub index_db_bytes: Option<u64>,
    pub host_id: String,
    pub root: String,
    pub backend: String,
    pub file_count: usize,
    #[serde(default)]
    pub index_state: Option<IndexState>,
    #[serde(default)]
    pub index_generation: u64,
    #[serde(default)]
    pub indexed_files: usize,
    #[serde(default)]
    pub index_last_error: Option<String>,
    #[serde(default)]
    pub index_directory_activity: Vec<crate::index::IndexDirectoryActivity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostSearchState {
    Ok,
    Partial,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerHostStatus {
    pub host_id: String,
    pub ok: bool,
    #[serde(default)]
    pub state: Option<HostSearchState>,
    pub error: Option<String>,
}

impl PerHostStatus {
    pub fn complete(host_id: impl Into<String>) -> Self {
        Self {
            host_id: host_id.into(),
            ok: true,
            state: Some(HostSearchState::Ok),
            error: None,
        }
    }

    pub fn partial(host_id: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            host_id: host_id.into(),
            ok: false,
            state: Some(HostSearchState::Partial),
            error: Some(error.into()),
        }
    }

    pub fn failed(host_id: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            host_id: host_id.into(),
            ok: false,
            state: Some(HostSearchState::Failed),
            error: Some(error.into()),
        }
    }

    pub fn normalize_legacy(mut self, response_partial: bool) -> Self {
        if self.state.is_none() {
            self.state = Some(if self.ok {
                HostSearchState::Ok
            } else if response_partial {
                HostSearchState::Partial
            } else {
                HostSearchState::Failed
            });
        }
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse<T> {
    pub request_id: String,
    pub origin_host: String,
    pub hop_count: u8,
    pub host_id: String,
    pub partial: bool,
    pub truncated: bool,
    pub results: Vec<T>,
    pub host_status: Vec<PerHostStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadResponse {
    pub request_id: String,
    pub origin_host: String,
    pub hop_count: u8,
    pub host_id: String,
    pub target_host_id: String,
    pub partial: bool,
    pub truncated: bool,
    pub path: String,
    pub start_line: usize,
    pub end_line: usize,
    pub chunks: Vec<ReadChunk>,
    pub host_status: Vec<PerHostStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub request_id: String,
    pub origin_host: String,
    pub hop_count: u8,
    pub host_id: String,
    pub partial: bool,
    pub host_status: Vec<PerHostStatus>,
    pub local: HostStatus,
    #[serde(default)]
    pub nodes: Vec<HostStatus>,
    pub topology: crate::topology::TopologyStatus,
}

#[derive(Clone)]
pub struct LocalBackend {
    pub host_id: String,
    pub root: PathBuf,
    pub root_paths: BTreeMap<String, Vec<PathBuf>>,
    pub limits: LimitsConfig,
    pub exclude_globs: Vec<String>,
    pub index: IndexManager,
    pub remote_index: Option<crate::remote_index::RemoteIndexStore>,
}

impl LocalBackend {
    pub fn new(host_id: impl Into<String>, root: impl Into<PathBuf>, limits: LimitsConfig) -> Self {
        let root = root.into();
        let mut root_paths = BTreeMap::new();
        root_paths.insert("local".to_string(), vec![root.clone()]);
        let excludes = default_exclude_globs();
        let index = IndexManager::start(
            root_paths.clone(),
            excludes.clone(),
            limits.max_file_bytes,
            limits.full_rebuild_min_interval_ms,
            limits.index_activity.clone(),
            None,
            crate::config::SttConfig::default(),
            crate::config::OcrConfig::default(),
        );
        Self {
            host_id: host_id.into(),
            root,
            root_paths,
            limits,
            exclude_globs: excludes,
            index,
            remote_index: None,
        }
    }

    pub fn from_config(
        host_id: impl Into<String>,
        root: impl Into<PathBuf>,
        limits: LimitsConfig,
        roots: BTreeMap<String, Vec<PathBuf>>,
        exclude_globs: Vec<String>,
        index_path: Option<PathBuf>,
    ) -> Self {
        Self::from_config_with_ingestion(
            host_id,
            root,
            limits,
            roots,
            exclude_globs,
            index_path,
            crate::config::SttConfig::default(),
            crate::config::OcrConfig::default(),
        )
    }

    pub fn from_config_with_ingestion(
        host_id: impl Into<String>,
        root: impl Into<PathBuf>,
        limits: LimitsConfig,
        roots: BTreeMap<String, Vec<PathBuf>>,
        exclude_globs: Vec<String>,
        index_path: Option<PathBuf>,
        stt: crate::config::SttConfig,
        ocr: crate::config::OcrConfig,
    ) -> Self {
        let root = root.into();
        let mut root_paths = roots
            .into_iter()
            .filter(|(_, paths)| !paths.is_empty())
            .collect::<BTreeMap<_, _>>();
        root_paths.insert("local".to_string(), vec![root.clone()]);
        let mut excludes = default_exclude_globs();
        excludes.extend(exclude_globs);
        excludes.extend(limits.index_activity.exclude_globs.clone());
        excludes.sort();
        excludes.dedup();
        let index = match index_path {
            Some(index_path) => IndexManager::start(
                root_paths.clone(),
                excludes.clone(),
                limits.max_file_bytes,
                limits.full_rebuild_min_interval_ms,
                limits.index_activity.clone(),
                Some(index_path),
                stt,
                ocr,
            ),
            None => IndexManager::disabled(),
        };
        Self {
            host_id: host_id.into(),
            root,
            root_paths,
            limits,
            exclude_globs: excludes,
            index,
            remote_index: None,
        }
    }

    pub fn with_remote_index_store(mut self, store: crate::remote_index::RemoteIndexStore) -> Self {
        self.remote_index = Some(store);
        self
    }

    pub fn with_excludes(mut self, excludes: Vec<String>) -> Self {
        self.exclude_globs.extend(excludes);
        self.exclude_globs.sort();
        self.exclude_globs.dedup();
        self.index = IndexManager::start(
            self.root_paths.clone(),
            self.exclude_globs.clone(),
            self.limits.max_file_bytes,
            self.limits.full_rebuild_min_interval_ms,
            self.limits.index_activity.clone(),
            None,
            crate::config::SttConfig::default(),
            crate::config::OcrConfig::default(),
        );
        self
    }

    pub fn with_named_roots(mut self, roots: BTreeMap<String, Vec<PathBuf>>) -> Self {
        let mut configured = BTreeMap::new();
        for (name, paths) in roots {
            let paths = paths
                .into_iter()
                .filter(|path| path.is_absolute())
                .collect::<Vec<_>>();
            if !paths.is_empty() {
                configured.insert(name, paths);
            }
        }
        configured.insert("local".to_string(), vec![self.root.clone()]);
        self.root_paths = configured;
        self.index = IndexManager::start(
            self.root_paths.clone(),
            self.exclude_globs.clone(),
            self.limits.max_file_bytes,
            self.limits.full_rebuild_min_interval_ms,
            self.limits.index_activity.clone(),
            None,
            crate::config::SttConfig::default(),
            crate::config::OcrConfig::default(),
        );
        self
    }

    pub fn status(&self) -> Result<HostStatus> {
        let index = self.index.status();
        Ok(HostStatus {
            roots: self
                .root_paths
                .values()
                .flatten()
                .map(|p| p.display().to_string())
                .collect(),
            index_current_path: index.current_path,
            index_db_bytes: self.index.database_bytes(),
            host_id: self.host_id.clone(),
            root: self.root.display().to_string(),
            backend: if self.index.is_enabled() {
                "indexed+rg-fallback"
            } else {
                "rg"
            }
            .to_string(),
            file_count: index.indexed_files,
            index_state: Some(index.state),
            index_generation: index.generation,
            indexed_files: index.indexed_files,
            index_last_error: index.last_error,
            index_directory_activity: index.directory_activity,
        })
    }

    pub fn list_locations(&self) -> Vec<Location> {
        self.root_paths
            .iter()
            .flat_map(|(name, paths)| {
                paths.iter().map(|path| Location {
                    host: self.host_id.clone(),
                    name: name.clone(),
                    path: path.display().to_string(),
                })
            })
            .collect()
    }

    pub fn list_directory(&self, path: &Path) -> Result<Vec<DirectoryEntry>> {
        let roots = self
            .root_paths
            .values()
            .flatten()
            .filter_map(|root| fs::canonicalize(root).ok())
            .collect::<Vec<_>>();
        let path = fs::canonicalize(path)
            .with_context(|| format!("resolve directory {}", path.display()))?;
        if !roots.iter().any(|root| path.starts_with(root)) {
            return Err(anyhow!(
                "path {} is outside configured roots",
                path.display()
            ));
        }
        if excluded_path(&path, &roots, &self.exclude_globs) {
            return Err(anyhow!("path {} is excluded", path.display()));
        }
        if !path.is_dir() {
            return Err(anyhow!("path {} is not a directory", path.display()));
        }

        let mut entries = Vec::new();
        for entry in
            fs::read_dir(&path).with_context(|| format!("list directory {}", path.display()))?
        {
            let Ok(entry) = entry else { continue };
            let Ok(entry_path) = fs::canonicalize(entry.path()) else {
                continue;
            };
            if !roots.iter().any(|root| entry_path.starts_with(root))
                || excluded_path(&entry_path, &roots, &self.exclude_globs)
            {
                continue;
            }
            let Ok(metadata) = fs::metadata(&entry_path) else {
                continue;
            };
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let kind = if metadata.is_dir() {
                "directory"
            } else if metadata.is_file() {
                "file"
            } else {
                "other"
            };
            let modified_ms = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .and_then(|duration| u64::try_from(duration.as_millis()).ok());
            entries.push(DirectoryEntry {
                name,
                path: entry_path.display().to_string(),
                kind: kind.to_string(),
                size: metadata.is_file().then_some(metadata.len()),
                modified_ms,
                created_ms: metadata
                    .created()
                    .ok()
                    .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                    .and_then(|duration| u64::try_from(duration.as_millis()).ok()),
            });
        }
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(entries)
    }

    fn selected_roots(&self, names: &[String]) -> Result<Vec<PathBuf>> {
        let mut roots = self.default_search_roots();
        if !names.is_empty() {
            roots.clear();
            for name in names {
                let paths = if let Some(paths) = self.root_paths.get(name) {
                    paths.clone()
                } else {
                    let requested = Path::new(name);
                    let Ok(requested) = fs::canonicalize(requested) else {
                        return Ok(self.default_search_roots());
                    };
                    let allowed = self
                        .root_paths
                        .values()
                        .flatten()
                        .filter_map(|path| fs::canonicalize(path).ok())
                        .any(|path| requested.starts_with(path));
                    if !allowed {
                        return Ok(self.default_search_roots());
                    }
                    vec![requested]
                };
                roots.extend(paths);
            }
        }
        roots.sort();
        roots.dedup();
        if roots.is_empty() {
            roots.push(self.root.clone());
        }
        Ok(roots)
    }

    fn default_search_roots(&self) -> Vec<PathBuf> {
        let mut roots = self
            .root_paths
            .iter()
            .filter(|(name, _)| name.as_str() != "local")
            .flat_map(|(_, paths)| paths.iter().cloned())
            .collect::<Vec<_>>();
        roots.sort();
        roots.dedup();
        if roots.is_empty() {
            roots.push(self.root.clone());
        }
        roots
    }

    pub async fn search_text(
        &self,
        query: &str,
        limit: usize,
        context_lines: usize,
        mode: SearchMode,
        path_globs: Vec<String>,
        root_names: Vec<String>,
    ) -> Result<Vec<SearchHit>> {
        Ok(self
            .search_text_bounded(query, limit, context_lines, mode, path_globs, root_names)
            .await?
            .hits)
    }

    pub async fn search_text_bounded(
        &self,
        query: &str,
        limit: usize,
        context_lines: usize,
        mode: SearchMode,
        path_globs: Vec<String>,
        root_names: Vec<String>,
    ) -> Result<SearchOutcome> {
        self.search_text_with_overall_timeout(
            query,
            limit,
            context_lines,
            mode,
            path_globs,
            root_names,
            Duration::from_millis(self.limits.overall_timeout_ms),
        )
        .await
    }

    /// Keep the interactive default separate from an already-admitted finite
    /// background job's deadline. No new caller-controlled timeout is exposed.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn search_text_with_overall_timeout(
        &self,
        query: &str,
        limit: usize,
        context_lines: usize,
        mode: SearchMode,
        path_globs: Vec<String>,
        root_names: Vec<String>,
        overall_timeout: Duration,
    ) -> Result<SearchOutcome> {
        let host_id = self.host_id.clone();
        // A source root authorized by this host's remote index lives on the
        // source machine. Scanning unrelated local roots first can use the
        // entire peer deadline before the remote SQLite result is read.
        let remote_index_only = query.chars().count() >= 3
            && matches!(
                mode,
                SearchMode::Literal | SearchMode::CaseInsensitiveLiteral
            )
            && self
                .remote_index
                .as_ref()
                .is_some_and(|index| index.owns_requested_roots(&root_names));
        let roots = if remote_index_only {
            Vec::new()
        } else {
            self.selected_roots(&root_names)?
        };
        let query = query.to_string();
        let exclude_globs = self.exclude_globs.clone();
        let max_file_bytes = self.limits.max_file_bytes;
        let max_response_bytes = self.limits.max_response_bytes;
        let deadline = Instant::now() + overall_timeout;
        let total_roots = roots.len();
        let mut hits = Vec::new();
        let mut truncated = false;
        let mut partial = false;
        let mut partial_error = None;
        let mut successful_roots = 0usize;
        let mut failed_roots = 0usize;
        let mut first_root_error = None;
        for root in roots {
            let mut index_miss = false;
            let remaining = limit.saturating_sub(hits.len());
            if remaining == 0 {
                truncated = true;
                break;
            }
            let remaining_time = deadline.saturating_duration_since(Instant::now());
            if remaining_time.is_zero() {
                failed_roots += 1;
                first_root_error.get_or_insert_with(|| "local rg search timed out".to_string());
                partial = true;
                break;
            }
            if matches!(
                mode,
                SearchMode::Literal | SearchMode::CaseInsensitiveLiteral
            ) {
                let index = self.index.clone();
                let reader_query = query.clone();
                let reader_root = root.clone();
                let reader_globs = path_globs.clone();
                let reader_excludes = exclude_globs.clone();
                let case_sensitive = matches!(mode, SearchMode::Literal);
                // Indexed reads run in spawn_blocking and already have the
                // bounded request deadline. A separate two-second cap makes
                // large but valid FTS lookups surface as SQLITE_INTERRUPT.
                let reader_deadline = deadline.into_std();
                // SQLite and cached document parsing must never occupy the async HTTP worker.
                let indexed = tokio::task::spawn_blocking(move || {
                    index.search_text_hits_filtered_until(
                        &reader_query,
                        &reader_root,
                        remaining,
                        context_lines,
                        case_sensitive,
                        &reader_globs,
                        &reader_excludes,
                        reader_deadline,
                    )
                })
                .await;
                let index_page = match indexed {
                    Ok(Ok(page)) => page,
                    failure => {
                        failed_roots += 1;
                        first_root_error.get_or_insert_with(|| match failure {
                            Ok(Err(message)) => format!("indexed search unavailable: {message}"),
                            _ => "indexed reader failed".to_string(),
                        });
                        partial = true;
                        // Cached OCR/PDF cannot be honestly replaced by raw binary file scanning.
                        continue;
                    }
                };
                if let Some(index_page) = index_page.filter(|page| {
                    index_miss = page.hits.is_empty() && !page.truncated;
                    !index_miss
                }) {
                    truncated |= index_page.truncated;
                    let index_hits = index_page.hits;
                    successful_roots += 1;
                    hits.extend(index_hits.into_iter().map(|hit| {
                        SearchHit {
                            host_id: host_id.clone(),
                            path: hit.path.display().to_string(),
                            line_number: hit.line_number,
                            text: hit
                                .context
                                .iter()
                                .find(|(line, _)| *line == hit.line_number)
                                .map(|(_, text)| text.clone())
                                .unwrap_or_default(),
                            context: hit
                                .context
                                .into_iter()
                                .map(|(line_number, text)| MatchLine { line_number, text })
                                .collect(),
                            column: 0,
                            ..Default::default()
                        }
                    }));
                    continue;
                }
            }
            // The persistent index is eventually consistent: an empty page must
            // not suppress a real file omitted by an older reconciliation. Do
            // not feed the same empty index candidates back into the live scan.
            let candidate_paths = if !index_miss
                && path_globs.is_empty()
                && matches!(
                    mode,
                    SearchMode::Literal | SearchMode::CaseInsensitiveLiteral
                ) {
                let index = self.index.clone();
                let candidate_query = query.clone();
                let candidate_root = root.clone();
                tokio::task::spawn_blocking(move || {
                    index.candidate_paths(&candidate_query, &candidate_root)
                })
                .await
                .map_err(|_| anyhow!("indexed candidate reader failed"))?
            } else {
                None
            };
            let outcome = search_text_impl(
                &host_id,
                &root,
                &query,
                remaining,
                context_lines,
                &mode,
                &path_globs,
                &exclude_globs,
                max_file_bytes,
                max_response_bytes,
                // An empty retained index is not proof of an empty live tree.
                // Give its verification the remaining request budget; a new
                // two-second cap prematurely abandoned large configured roots.
                deadline,
                candidate_paths,
            )
            .await;
            let outcome = match outcome {
                Ok(outcome) => {
                    successful_roots += 1;
                    outcome
                }
                Err(err) => {
                    failed_roots += 1;
                    first_root_error.get_or_insert_with(|| err.to_string());
                    partial = true;
                    continue;
                }
            };
            hits.extend(outcome.hits);
            partial |= outcome.partial;
            if partial_error.is_none() {
                partial_error = outcome.partial_error;
            }
            if outcome.truncated {
                truncated = true;
                break;
            }
        }
        if successful_roots == 0 && failed_roots > 0 && self.remote_index.is_none() {
            return Err(anyhow!(
                "all selected search roots failed: {}",
                first_root_error.unwrap_or_else(|| "unknown root error".to_string())
            ));
        }
        if failed_roots > 0 && partial_error.is_none() {
            partial_error = Some(format!(
                "{failed_roots} of {total_roots} selected search roots failed"
            ));
        }
        self.enrich_search_metadata(&mut hits);
        if let Some(remote_index) = self.remote_index.clone() {
            if matches!(
                mode,
                SearchMode::Literal | SearchMode::CaseInsensitiveLiteral
            ) {
                let remote_query = query.clone();
                let remote_mode = mode.clone();
                let remote_limit = limit.saturating_sub(hits.len());
                if remote_limit > 0 {
                    match tokio::task::spawn_blocking(move || {
                        remote_index.search_filtered(
                            &remote_query,
                            remote_limit,
                            &remote_mode,
                            &path_globs,
                        )
                    })
                    .await
                    {
                        Ok(Ok(remote_hits)) => hits.extend(remote_hits),
                        Ok(Err(error)) => {
                            partial = true;
                            partial_error.get_or_insert_with(|| {
                                format!("remote index unavailable: {error}")
                            });
                        }
                        Err(_) => {
                            partial = true;
                            partial_error
                                .get_or_insert_with(|| "remote index reader failed".to_string());
                        }
                    }
                }
            }
        }
        hits.sort_by(|a, b| {
            a.host_id
                .cmp(&b.host_id)
                .then(a.path.cmp(&b.path))
                .then(a.line_number.cmp(&b.line_number))
        });
        hits.truncate(limit);
        Ok(SearchOutcome {
            hits,
            truncated,
            partial,
            partial_error,
        })
    }

    pub async fn find_paths(
        &self,
        query: &str,
        limit: usize,
        root_names: Vec<String>,
    ) -> Result<Vec<SearchHit>> {
        Ok(self
            .find_paths_bounded(query, limit, root_names)
            .await?
            .hits)
    }

    pub async fn find_paths_bounded(
        &self,
        query: &str,
        limit: usize,
        root_names: Vec<String>,
    ) -> Result<SearchOutcome> {
        let host_id = self.host_id.clone();
        let roots = self.selected_roots(&root_names)?;
        let query = query.to_string();
        let exclude_globs = self.exclude_globs.clone();
        let max_response_bytes = self.limits.max_response_bytes;
        let deadline = Instant::now() + Duration::from_millis(self.limits.overall_timeout_ms);
        let total_roots = roots.len();
        let mut hits = Vec::new();
        let mut truncated = false;
        let mut partial = false;
        let mut partial_error = None;
        let mut successful_roots = 0usize;
        let mut failed_roots = 0usize;
        let mut first_root_error = None;
        for root in roots {
            let remaining = limit.saturating_sub(hits.len());
            if remaining == 0 {
                truncated = true;
                break;
            }
            if deadline.saturating_duration_since(Instant::now()).is_zero() {
                failed_roots += 1;
                first_root_error
                    .get_or_insert_with(|| "local rg path search timed out".to_string());
                partial = true;
                break;
            }
            let outcome = find_paths_impl(
                &host_id,
                &root,
                &query,
                remaining,
                &exclude_globs,
                max_response_bytes,
                deadline,
            )
            .await;
            let outcome = match outcome {
                Ok(outcome) => {
                    successful_roots += 1;
                    outcome
                }
                Err(err) => {
                    failed_roots += 1;
                    first_root_error.get_or_insert_with(|| err.to_string());
                    partial = true;
                    continue;
                }
            };
            hits.extend(outcome.hits);
            partial |= outcome.partial;
            if partial_error.is_none() {
                partial_error = outcome.partial_error;
            }
            if outcome.truncated {
                truncated = true;
                break;
            }
        }
        if successful_roots == 0 && failed_roots > 0 {
            return Err(anyhow!(
                "all selected search roots failed: {}",
                first_root_error.unwrap_or_else(|| "unknown root error".to_string())
            ));
        }
        if failed_roots > 0 && partial_error.is_none() {
            partial_error = Some(format!(
                "{failed_roots} of {total_roots} selected search roots failed"
            ));
        }
        hits.sort_by(|a, b| a.path.cmp(&b.path));
        hits.truncate(limit);
        self.enrich_search_metadata(&mut hits);
        Ok(SearchOutcome {
            hits,
            truncated,
            partial,
            partial_error,
        })
    }

    pub async fn find_duplicates_bounded(
        &self,
        max_files: usize,
        root_names: Vec<String>,
        requested_paths: Vec<PathBuf>,
    ) -> Result<DuplicateScan> {
        anyhow::ensure!(
            !root_names.is_empty(),
            "find_duplicates requires explicit configured root names"
        );
        let max_files = max_files.clamp(2, 250);
        anyhow::ensure!(requested_paths.len() <= 16, "at most 16 duplicate-scan paths are allowed");
        let configured_roots = self.selected_roots(&root_names)?;
        let allowed_roots = configured_roots
            .iter()
            .into_iter()
            .filter_map(|root| fs::canonicalize(root).ok())
            .collect::<Vec<_>>();
        let mut truncated = false;
        let mut partial = false;
        let mut hits = Vec::new();
        if requested_paths.is_empty() {
            let result = self
                .find_paths_bounded("", max_files, root_names)
                .await?;
            hits = result.hits;
            truncated = result.truncated;
            partial = result.partial;
        } else {
            let mut canonical_paths = BTreeSet::new();
            for requested in requested_paths {
                let path = fs::canonicalize(&requested).with_context(|| {
                    format!("canonicalize requested duplicate root {}", requested.display())
                })?;
                anyhow::ensure!(
                    allowed_roots.iter().any(|root| path.starts_with(root)),
                    "duplicate scan path is outside the selected configured roots"
                );
                anyhow::ensure!(path.is_dir(), "duplicate scan path must be a directory");
                canonical_paths.insert(path);
            }
            let deadline = Instant::now() + Duration::from_millis(self.limits.overall_timeout_ms);
            for root in canonical_paths {
                let remaining = max_files.saturating_sub(hits.len());
                if remaining == 0 {
                    truncated = true;
                    break;
                }
                let result = find_paths_impl(
                    &self.host_id,
                    &root,
                    "",
                    remaining,
                    &self.exclude_globs,
                    self.limits.max_response_bytes,
                    deadline,
                )
                .await?;
                hits.extend(result.hits);
                partial |= result.partial;
                if result.truncated {
                    truncated = true;
                    break;
                }
            }
        }
        let mut files = Vec::new();
        let mut bytes_hashed = 0u64;
        let mut skipped_oversized = 0usize;
        let mut skipped_unsafe_paths = 0usize;
        let mut seen_paths = BTreeSet::new();
        for hit in &hits {
            let path = PathBuf::from(&hit.path);
            let canonical_path = match fs::canonicalize(&path) {
                Ok(path) if allowed_roots.iter().any(|root| path.starts_with(root)) => path,
                _ => {
                    skipped_unsafe_paths += 1;
                    continue;
                }
            };
            if !seen_paths.insert(canonical_path.clone()) {
                continue;
            }
            let metadata = match fs::metadata(&canonical_path) {
                Ok(metadata) if metadata.is_file() => metadata,
                _ => continue,
            };
            let size = metadata.len();
            if size > 64 * 1024 * 1024 || bytes_hashed.saturating_add(size) > 512 * 1024 * 1024 {
                skipped_oversized += 1;
                continue;
            }
            let mut file = fs::File::open(&canonical_path)
                .with_context(|| format!("open candidate {}", path.display()))?;
            let before = file.metadata()?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                anyhow::ensure!(
                    before.dev() == metadata.dev() && before.ino() == metadata.ino(),
                    "candidate changed while opening: {}",
                    path.display()
                );
            }
            let mut hasher = Sha256::new();
            let mut buffer = [0u8; 64 * 1024];
            loop {
                let count = file.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                hasher.update(&buffer[..count]);
            }
            let after = file.metadata()?;
            anyhow::ensure!(
                before.len() == after.len()
                    && before.modified().ok() == after.modified().ok()
                    && before.len() == size,
                "candidate changed while hashing: {}",
                path.display()
            );
            bytes_hashed += size;
            #[cfg(unix)]
            let (allocated_bytes, physical_file_id, hard_link_count) = {
                use std::os::unix::fs::MetadataExt;
                (
                    Some(after.blocks().saturating_mul(512)),
                    Some(format!("{}:{}", after.dev(), after.ino())),
                    Some(after.nlink()),
                )
            };
            #[cfg(not(unix))]
            let (allocated_bytes, physical_file_id, hard_link_count) = (None, None, None);
            files.push(DuplicateFileHash {
                host_id: self.host_id.clone(),
                path: path.to_string_lossy().to_string(),
                size,
                sha256: format!("{:x}", hasher.finalize()),
                allocated_bytes,
                physical_file_id,
                hard_link_count,
            });
        }
        Ok(DuplicateScan {
            scanned_files: hits.len(),
            bytes_hashed,
            skipped_oversized,
            skipped_unsafe_paths,
            partial: partial || skipped_oversized > 0 || skipped_unsafe_paths > 0,
            truncated,
            files,
        })
    }

    /// Only stat returned local paths, once per file even with many matching lines.
    fn enrich_search_metadata(&self, hits: &mut [SearchHit]) {
        let mut metadata = BTreeMap::new();
        for hit in hits.iter_mut().filter(|hit| hit.host_id == self.host_id) {
            let meta = metadata
                .entry(hit.path.clone())
                .or_insert_with(|| fs::metadata(&hit.path).ok());
            if let Some(meta) = meta {
                hit.size = meta.is_file().then_some(meta.len());
                let millis = |time: std::io::Result<SystemTime>| {
                    time.ok()
                        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
                };
                hit.modified_ms = millis(meta.modified());
                hit.created_ms = millis(meta.created());
            }
        }
    }

    pub fn read_text(
        &self,
        path: &Path,
        start_line: Option<usize>,
        end_line: Option<usize>,
    ) -> Result<Vec<ReadChunk>> {
        // Search defaults may omit the primary root when named roots exist.
        // Reading a selected result is authorized against every configured root.
        let roots = std::iter::once(self.root.clone())
            .chain(self.root_paths.values().flatten().cloned())
            .collect::<Vec<_>>();
        let abs = normalize_absolute_path_any(&roots, path)?;
        let size = fs::metadata(&abs)
            .with_context(|| format!("metadata {}", abs.display()))?
            .len();
        if size > self.limits.max_file_bytes {
            return Err(anyhow!(
                "file {} exceeds max_file_bytes {}",
                abs.display(),
                self.limits.max_file_bytes
            ));
        }
        let start = start_line.unwrap_or(1).max(1);
        let mut selected_lines = Vec::new();
        let requested_end = end_line.map(|end| end.max(start));
        let max_read_bytes = if requested_end.is_some() {
            MAX_BOUNDED_TEXT_READ_BYTES
        } else {
            usize::try_from(size).unwrap_or(usize::MAX)
        };
        let mut reader = StdBufReader::new(
            fs::File::open(&abs).with_context(|| format!("read {}", abs.display()))?,
        )
        .take((max_read_bytes as u64).saturating_add(1));
        let mut line_number = 0usize;
        let mut bytes_read = 0usize;
        let mut line = String::new();
        loop {
            if requested_end.is_some_and(|end| line_number >= end) {
                break;
            }
            line.clear();
            if reader
                .read_line(&mut line)
                .with_context(|| format!("read {}", abs.display()))?
                == 0
            {
                break;
            }
            bytes_read = bytes_read.saturating_add(line.len());
            if bytes_read > max_read_bytes {
                return Err(anyhow!(
                    "requested text range in {} exceeds the {} byte read limit",
                    abs.display(),
                    max_read_bytes
                ));
            }
            line_number = line_number.saturating_add(1);
            if line_number >= start {
                let text = if let Some(without_lf) = line.strip_suffix('\n') {
                    without_lf.strip_suffix('\r').unwrap_or(without_lf)
                } else {
                    line.as_str()
                };
                selected_lines.push(MatchLine {
                    line_number,
                    text: text.to_string(),
                });
            }
        }
        let end = if line_number < start {
            start
        } else {
            requested_end.unwrap_or(line_number).min(line_number)
        };
        Ok(vec![ReadChunk {
            start_line: start,
            end_line: end,
            lines: selected_lines,
        }])
    }
}

#[allow(clippy::too_many_arguments)]
async fn search_text_impl(
    host_id: &str,
    root: &Path,
    query: &str,
    limit: usize,
    context_lines: usize,
    mode: &SearchMode,
    path_globs: &[String],
    exclude_globs: &[String],
    max_file_bytes: u64,
    max_response_bytes: usize,
    deadline: Instant,
    candidate_paths: Option<Vec<PathBuf>>,
) -> Result<SearchOutcome> {
    ensure_search_root_listable(root, "start rg search")?;
    let using_rg = rg_command().is_some();
    let mut command = rg_command().unwrap_or_else(grep_command);
    command.arg("-n");
    if using_rg {
        // Match the indexer's device boundary. Explicitly configured mounted
        // roots are searched separately, rather than entering foreign mounts.
        // Repository ignore files are not GrepMesh exclusions. Match the
        // index's configured corpus while retaining explicit exclude globs.
        command.arg("--hidden").arg("--no-ignore").arg("--one-file-system");
    } else {
        command
            .arg("-R")
            .arg("-H")
            .arg("--binary-files=without-match");
    }
    match mode {
        SearchMode::Literal => {
            command.arg("-F");
        }
        SearchMode::Regex => {}
        SearchMode::CaseInsensitiveLiteral => {
            command.arg("-F").arg("-i");
        }
    }
    if using_rg {
        for glob in path_globs {
            command.arg("--glob").arg(glob);
        }
        for glob in exclude_globs {
            command.arg("--glob").arg(format!("!{glob}"));
        }
        command
            .arg("--max-filesize")
            .arg(max_file_bytes.to_string());
    } else {
        if !path_globs.is_empty() {
            return Err(anyhow!(
                "path_globs require rg; install ripgrep or retry without path_globs"
            ));
        }
        for glob in exclude_globs {
            if let Some(name) = glob
                .strip_prefix("**/")
                .and_then(|value| value.strip_suffix("/**"))
            {
                command.arg(format!("--exclude-dir={name}"));
            } else if let Some(name) = glob.strip_prefix("**/") {
                command.arg(format!("--exclude={name}"));
            }
        }
    }
    if candidate_paths
        .as_ref()
        .is_some_and(|paths| !paths.is_empty())
    {
        command.arg("--with-filename");
    }
    command.arg("--");
    command.arg(query);
    if let Some(paths) = candidate_paths {
        if paths.is_empty() {
            return Ok(SearchOutcome {
                hits: Vec::new(),
                truncated: false,
                partial: false,
                partial_error: None,
            });
        }
        for path in paths {
            command.arg(path);
        }
    } else {
        command.arg(".");
    }
    let mut child = command
        .current_dir(root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|err| {
            anyhow!(
                "start {} search in {}: {err}",
                if using_rg { "rg" } else { "grep" },
                root.display()
            )
        })?;
    let Some(mut stdout) = child.stdout.take() else {
        terminate_child(&mut child).await;
        return Err(anyhow!("rg search did not provide stdout"));
    };
    let Some(stderr) = child.stderr.take() else {
        terminate_child(&mut child).await;
        return Err(anyhow!("rg search did not provide stderr"));
    };
    let stderr_task = tokio::spawn(async move {
        let mut stderr = BufReader::new(stderr);
        let mut output = Vec::new();
        let mut line = Vec::new();
        let mut overflowed = false;
        let mut saw_diagnostic = false;
        let mut all_diagnostics_benign = true;
        loop {
            line.clear();
            let read = stderr.read_until(b'\n', &mut line).await?;
            if read == 0 {
                break;
            }
            if line.iter().any(|byte| !byte.is_ascii_whitespace()) {
                saw_diagnostic = true;
                all_diagnostics_benign &= permission_only_traversal_diagnostic(&line);
            }
            let remaining = (64 * 1024usize).saturating_sub(output.len());
            let kept = read.min(remaining);
            output.extend_from_slice(&line[..kept]);
            overflowed |= kept != read;
        }
        Ok::<_, std::io::Error>((output, overflowed, saw_diagnostic && all_diagnostics_benign))
    });
    let mut hits = Vec::new();
    let mut pending = Vec::new();
    let mut stdout_bytes = 0usize;
    // A zero response limit is the existing configuration convention for an
    // unbounded response. Otherwise, cap the total bytes consumed from this
    // rg invocation and stop as soon as the global match/byte bound is met.
    let byte_limit = (max_response_bytes != 0).then_some(max_response_bytes);
    let mut stopped_early = limit == 0;
    let mut buffer = [0u8; 8192];

    while !stopped_early {
        let read_size = match byte_limit {
            Some(byte_limit) => {
                let remaining_bytes = byte_limit.saturating_sub(stdout_bytes);
                if remaining_bytes == 0 {
                    stopped_early = true;
                    break;
                }
                remaining_bytes.min(buffer.len())
            }
            None => buffer.len(),
        };
        let read = match timeout(
            deadline.saturating_duration_since(Instant::now()),
            stdout.read(&mut buffer[..read_size]),
        )
        .await
        {
            Ok(Ok(read)) => read,
            Ok(Err(err)) => {
                terminate_child(&mut child).await;
                return Err(err).context("read rg search output");
            }
            Err(_) => {
                terminate_child(&mut child).await;
                return Err(anyhow!("rg search timed out"));
            }
        };
        if read == 0 {
            break;
        }
        stdout_bytes = stdout_bytes.saturating_add(read);
        pending.extend_from_slice(&buffer[..read]);

        while let Some(line_end) = pending.iter().position(|byte| *byte == b'\n') {
            let line = pending.drain(..=line_end).collect::<Vec<_>>();
            let hit = match search_hit_from_rg_line(
                host_id,
                root,
                query,
                context_lines,
                mode,
                &line,
                deadline,
            )
            .await
            {
                Ok(hit) => hit,
                Err(err) => {
                    terminate_child(&mut child).await;
                    return Err(err);
                }
            };
            if let Some(hit) = hit {
                hits.push(hit);
            }
            if hits.len() >= limit {
                stopped_early = true;
                break;
            }
        }
    }

    if !stopped_early && !pending.is_empty() {
        let hit = match search_hit_from_rg_line(
            host_id,
            root,
            query,
            context_lines,
            mode,
            &pending,
            deadline,
        )
        .await
        {
            Ok(hit) => hit,
            Err(err) => {
                terminate_child(&mut child).await;
                return Err(err);
            }
        };
        if let Some(hit) = hit {
            hits.push(hit);
        }
    }

    drop(stdout);
    let mut partial = false;
    let mut partial_error = None;
    if stopped_early {
        terminate_child(&mut child).await;
    } else {
        let status = match timeout(
            deadline.saturating_duration_since(Instant::now()),
            child.wait(),
        )
        .await
        {
            Ok(Ok(status)) => status,
            Ok(Err(err)) => return Err(err).context("wait for rg search"),
            Err(_) => {
                terminate_child(&mut child).await;
                return Err(anyhow!("rg search timed out"));
            }
        };
        let (stderr, stderr_overflowed, all_diagnostics_benign) = stderr_task
            .await
            .context("collect rg search diagnostics")??;
        if stderr_overflowed && !all_diagnostics_benign {
            return Err(anyhow!("rg search diagnostics exceeded 65536 bytes"));
        }
        if status.code() == Some(2) {
            if !(all_diagnostics_benign || permission_only_traversal_diagnostic(&stderr)) {
                let diagnostic = non_readable_traversal_diagnostic(&stderr).ok_or_else(|| {
                    anyhow!(
                        "rg search failed with {}: {}",
                        status,
                        String::from_utf8_lossy(&stderr).trim()
                    )
                })?;
                partial = true;
                partial_error = Some(diagnostic);
            }
        } else if !status.success() && status.code() != Some(1) {
            return Err(anyhow!(
                "rg search failed with {}: {}",
                status,
                String::from_utf8_lossy(&stderr).trim()
            ));
        }
    }
    Ok(SearchOutcome {
        hits,
        truncated: stopped_early,
        partial,
        partial_error,
    })
}

fn non_readable_traversal_diagnostic(stderr: &[u8]) -> Option<String> {
    let diagnostic = String::from_utf8_lossy(stderr).trim().to_string();
    let mut lines = diagnostic
        .lines()
        .filter(|line| !line.trim().is_empty())
        .peekable();
    lines.peek()?;
    lines
        .all(|line| {
            let line = line.to_ascii_lowercase();
            line.contains("permission denied")
                || line.contains("operation not permitted")
                || line.contains("not readable")
                || line.contains("input/output error")
                || line.contains("resource busy")
                || line.contains("resource deadlock avoided")
                || line.contains("no such file or directory")
                || line.contains("stale file handle")
        })
        .then_some("some configured paths were not readable".to_string())
}

fn permission_only_traversal_diagnostic(stderr: &[u8]) -> bool {
    let diagnostic = String::from_utf8_lossy(stderr);
    let mut lines = diagnostic
        .lines()
        .filter(|line| !line.trim().is_empty())
        .peekable();
    if lines.peek().is_none() {
        return false;
    }
    lines.all(|line| {
        let line = line.to_ascii_lowercase();
        line.contains("permission denied")
            || line.contains("operation not permitted")
            || line.contains("(os error 32)")
            || line.contains("(os error 1920)")
    })
}

fn ensure_search_root_listable(root: &Path, operation: &str) -> Result<()> {
    fs::read_dir(root).with_context(|| format!("{operation} in {}", root.display()))?;
    fs::canonicalize(root.join("."))
        .with_context(|| format!("{operation} in {}", root.display()))?;
    Ok(())
}

async fn terminate_child(child: &mut tokio::process::Child) {
    let _ = child.start_kill();
    let _ = child.wait().await;
}

async fn search_hit_from_rg_line(
    host_id: &str,
    root: &Path,
    query: &str,
    context_lines: usize,
    mode: &SearchMode,
    raw_line: &[u8],
    deadline: Instant,
) -> Result<Option<SearchHit>> {
    let line = String::from_utf8_lossy(raw_line);
    let line = line.strip_suffix('\n').unwrap_or(&line);
    let line = line.strip_suffix('\r').unwrap_or(line);
    let Some((rel_path, line_no, text)) = parse_rg_line(line) else {
        return Ok(None);
    };
    let abs = root.join(strip_current_dir_prefix(rel_path));
    let remaining = deadline.saturating_duration_since(Instant::now());
    let context = timeout(remaining, read_context(&abs, line_no, context_lines))
        .await
        .map_err(|_| anyhow!("rg search timed out"))??;
    Ok(Some(SearchHit {
        host_id: host_id.to_string(),
        path: abs.display().to_string(),
        line_number: line_no,
        context,
        text: text.to_string(),
        column: match mode {
            SearchMode::Regex => 1,
            SearchMode::Literal => text.find(query).map(|index| index + 1).unwrap_or(1),
            SearchMode::CaseInsensitiveLiteral => text
                .to_ascii_lowercase()
                .find(&query.to_ascii_lowercase())
                .map(|index| index + 1)
                .unwrap_or(1),
        },
        ..Default::default()
    }))
}

async fn find_paths_impl(
    host_id: &str,
    root: &Path,
    query: &str,
    limit: usize,
    exclude_globs: &[String],
    max_response_bytes: usize,
    deadline: Instant,
) -> Result<SearchOutcome> {
    ensure_search_root_listable(root, "run rg --files")?;
    let Some(mut command) = rg_command() else {
        return Err(anyhow!(
            "path search requires rg; install ripgrep (text search falls back to grep)"
        ));
    };
    command
        .arg("--files")
        .arg("--hidden")
        .arg("--no-ignore")
        .arg("--one-file-system");
    for glob in exclude_globs {
        command.arg("--glob").arg(format!("!{glob}"));
    }
    let mut child = command
        .arg(".")
        .current_dir(root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| "run rg --files")?;
    let Some(mut stdout) = child.stdout.take() else {
        terminate_child(&mut child).await;
        return Err(anyhow!("rg --files did not provide stdout"));
    };
    let Some(mut stderr) = child.stderr.take() else {
        terminate_child(&mut child).await;
        return Err(anyhow!("rg --files did not provide stderr"));
    };
    let stderr_task = tokio::spawn(async move {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).await.map(|_| bytes)
    });
    let mut hits = Vec::new();
    let mut pending = Vec::new();
    let mut stdout_bytes = 0usize;
    let byte_limit = (max_response_bytes != 0).then_some(max_response_bytes);
    let mut stopped_early = limit == 0;
    let mut buffer = [0u8; 8192];

    while !stopped_early {
        let read_size = match byte_limit {
            Some(byte_limit) => {
                let remaining_bytes = byte_limit.saturating_sub(stdout_bytes);
                if remaining_bytes == 0 {
                    stopped_early = true;
                    break;
                }
                remaining_bytes.min(buffer.len())
            }
            None => buffer.len(),
        };
        let read = match timeout(
            deadline.saturating_duration_since(Instant::now()),
            stdout.read(&mut buffer[..read_size]),
        )
        .await
        {
            Ok(Ok(read)) => read,
            Ok(Err(err)) => {
                terminate_child(&mut child).await;
                return Err(err).context("read rg path output");
            }
            Err(_) => {
                terminate_child(&mut child).await;
                return Err(anyhow!("rg path search timed out"));
            }
        };
        if read == 0 {
            break;
        }
        stdout_bytes = stdout_bytes.saturating_add(read);
        pending.extend_from_slice(&buffer[..read]);

        while let Some(line_end) = pending.iter().position(|byte| *byte == b'\n') {
            let line = pending.drain(..=line_end).collect::<Vec<_>>();
            if let Some(hit) = path_hit_from_rg_line(host_id, root, query, &line) {
                hits.push(hit);
            }
            if hits.len() >= limit {
                stopped_early = true;
                break;
            }
        }
    }

    if !stopped_early && !pending.is_empty() {
        if let Some(hit) = path_hit_from_rg_line(host_id, root, query, &pending) {
            hits.push(hit);
        }
    }

    drop(stdout);
    if stopped_early {
        terminate_child(&mut child).await;
    } else {
        let status = match timeout(
            deadline.saturating_duration_since(Instant::now()),
            child.wait(),
        )
        .await
        {
            Ok(Ok(status)) => status,
            Ok(Err(err)) => return Err(err).context("wait for rg --files"),
            Err(_) => {
                terminate_child(&mut child).await;
                return Err(anyhow!("rg path search timed out"));
            }
        };
        let stderr = stderr_task
            .await
            .context("collect rg --files diagnostics")??;
        if status.code() == Some(2) {
            if !permission_only_traversal_diagnostic(&stderr) {
                let diagnostic = non_readable_traversal_diagnostic(&stderr).ok_or_else(|| {
                    anyhow!(
                        "rg --files failed with {}: {}",
                        status,
                        String::from_utf8_lossy(&stderr).trim()
                    )
                })?;
                return Ok(SearchOutcome {
                    hits,
                    truncated: false,
                    partial: true,
                    partial_error: Some(diagnostic),
                });
            }
        } else if !status.success() && status.code() != Some(1) {
            return Err(anyhow!("rg --files failed with {}", status));
        }
    }
    Ok(SearchOutcome {
        hits,
        truncated: stopped_early,
        partial: false,
        partial_error: None,
    })
}

fn path_hit_from_rg_line(
    host_id: &str,
    root: &Path,
    query: &str,
    raw_line: &[u8],
) -> Option<SearchHit> {
    let line = String::from_utf8_lossy(raw_line);
    let line = line.strip_suffix('\n').unwrap_or(&line);
    let line = line.strip_suffix('\r').unwrap_or(line);
    if !path_query_matches(query, line) {
        return None;
    }
    let reported_path = Path::new(strip_current_dir_prefix(line));
    let abs = if reported_path.is_absolute() {
        reported_path.to_path_buf()
    } else {
        root.join(reported_path)
    };
    Some(SearchHit {
        host_id: host_id.to_string(),
        path: abs.display().to_string(),
        line_number: 0,
        context: Vec::new(),
        text: String::new(),
        column: 0,
        ..Default::default()
    })
}

fn path_query_matches(pattern: &str, path: &str) -> bool {
    if pattern.contains('*') || pattern.contains('?') {
        return Glob::new(pattern)
            .map(|glob| glob.compile_matcher().is_match(path))
            .unwrap_or(false);
    }
    path.contains(pattern)
}

fn parse_rg_line(line: &str) -> Option<(&str, usize, &str)> {
    let (path, rest) = line.split_once(':')?;
    let (line_no, text) = rest.split_once(':')?;
    Some((path, line_no.parse().ok()?, text))
}

fn strip_current_dir_prefix(path: &str) -> &str {
    path.strip_prefix("./").unwrap_or(path)
}

async fn read_context(
    path: &Path,
    line_number: usize,
    context_lines: usize,
) -> Result<Vec<MatchLine>> {
    let text = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("read {}", path.display()))?;
    let lines: Vec<_> = text.lines().collect();
    let start = line_number.saturating_sub(context_lines).max(1);
    let end = std::cmp::min(lines.len(), line_number + context_lines);
    let mut out = Vec::new();
    for idx in start..=end {
        out.push(MatchLine {
            line_number: idx,
            text: lines[idx - 1].to_string(),
        });
    }
    Ok(out)
}

fn normalize_absolute_path_any(roots: &[PathBuf], path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(anyhow!("path must be absolute: {}", path.display()));
    }
    let resolved = fs::canonicalize(path).with_context(|| format!("resolve {}", path.display()))?;
    if !roots
        .iter()
        .filter_map(|root| fs::canonicalize(root).ok())
        .any(|root| resolved.starts_with(root))
    {
        return Err(anyhow!(
            "path {} is outside configured roots",
            path.display(),
        ));
    }
    Ok(resolved)
}

fn excluded_path(path: &Path, roots: &[PathBuf], exclude_globs: &[String]) -> bool {
    roots.iter().any(|root| {
        path.strip_prefix(root)
            .ok()
            .is_some_and(|relative| excluded_relative_path(relative, exclude_globs))
    })
}

fn excluded_relative_path(path: &Path, exclude_globs: &[String]) -> bool {
    let path = path.to_string_lossy().replace('\\', "/");
    exclude_globs.iter().any(|pattern| {
        Glob::new(pattern)
            .map(|glob| glob.compile_matcher().is_match(&path))
            .unwrap_or(false)
            || pattern
                .strip_suffix("/**")
                .map(|prefix| prefix.trim_start_matches("**/").trim_start_matches("./"))
                .is_some_and(|prefix| path == prefix || path.starts_with(&format!("{prefix}/")))
    })
}

pub fn dedup_hits<T, F>(items: Vec<T>, key: F) -> Vec<T>
where
    F: Fn(&T) -> (String, String, usize),
{
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for item in items {
        let k = key(&item);
        if seen.insert(k) {
            out.push(item);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn find_duplicates_hashes_only_an_explicit_bounded_root_without_mutating_files() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("docs");
        std::fs::create_dir_all(&root).unwrap();
        let first = root.join("first.md");
        let second = root.join("second.md");
        let unique = root.join("unique.md");
        std::fs::write(&first, b"same content").unwrap();
        std::fs::write(&second, b"same content").unwrap();
        std::fs::write(&unique, b"other content").unwrap();
        let backend = super::LocalBackend {
            host_id: "test".into(),
            root: root.clone(),
            root_paths: std::collections::BTreeMap::from([("docs".into(), vec![root.clone()])]),
            limits: Default::default(),
            exclude_globs: Vec::new(),
            index: crate::index::IndexManager::disabled(),
            remote_index: None,
        };
        let scan = backend
            .find_duplicates_bounded(10, vec!["docs".into()], vec![root])
            .await
            .unwrap();
        assert_eq!(scan.scanned_files, 3);
        assert_eq!(scan.files.len(), 3);
        assert_eq!(scan.bytes_hashed, 37);
        let duplicate_hashes = scan
            .files
            .iter()
            .filter(|file| file.size == 12)
            .map(|file| file.sha256.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(duplicate_hashes.len(), 1);
        assert!(first.exists() && second.exists() && unique.exists());
    }

    #[tokio::test]
    async fn lossless_body_copy_remote_root_alias_search_does_not_scan_unrelated_local_roots() {
        let temp = tempfile::tempdir().unwrap();
        let local_root = temp.path().join("local");
        std::fs::create_dir(&local_root).unwrap();
        let source_root = temp.path().join("source-docs");
        let source_path = source_root.join("pilot.md");
        let store =
            crate::remote_index::RemoteIndexStore::open(crate::config::RemoteIndexStoreConfig {
                index_host: "server-44".into(),
                path: temp.path().join("remote.sqlite3"),
                allowed_source_hosts: vec!["server-100".into()],
                allowed_roots: std::collections::BTreeMap::from([(
                    "server-100".into(),
                    std::collections::BTreeMap::from([("docs".into(), source_root.clone())]),
                )]),
            })
            .unwrap();
        store
            .upsert(crate::remote_index::RemoteDocument {
                source_host: "server-100".into(),
                root_name: "docs".into(),
                path: source_path.clone(),
                size: 40,
                mtime_ns: 1,
                body: "remote scoped pilot marker".into(),
            })
            .unwrap();
        let local_match = local_root.join("unrelated.md");
        std::fs::write(&local_match, "remote scoped pilot marker").unwrap();
        let backend = super::LocalBackend {
            host_id: "server-44".into(),
            root: local_root.clone(),
            root_paths: std::collections::BTreeMap::from([("local".into(), vec![local_root])]),
            limits: Default::default(),
            exclude_globs: Vec::new(),
            index: crate::index::IndexManager::disabled(),
            remote_index: Some(store),
        };

        let result = backend
            .search_text_bounded(
                "remote scoped pilot",
                5,
                0,
                super::SearchMode::Literal,
                Vec::new(),
                vec!["docs".into()],
            )
            .await
            .unwrap();
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].host_id, "server-100");
        assert_eq!(result.hits[0].path, source_path.display().to_string());

        let excluded = backend
            .search_text_bounded(
                "remote scoped pilot",
                5,
                0,
                super::SearchMode::Literal,
                vec!["!**/pilot.md".into()],
                vec!["docs".into()],
            )
            .await
            .unwrap();
        assert!(excluded.hits.is_empty());
        assert!(local_match.is_file());
    }

    #[tokio::test]
    async fn live_scans_use_configured_exclusions_not_repository_ignore_files() {
        // Focused integration, expected <1s/max5s. Real rg over an unindexed
        // tree: the index does not interpret repository ignore files either.
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join(".ignore"), "needle.txt\nblocked.txt\n").unwrap();
        let needle = temp.path().join("needle.txt");
        std::fs::write(&needle, "live needle\n").unwrap();
        std::fs::write(temp.path().join("blocked.txt"), "live needle\n").unwrap();
        let backend = super::LocalBackend {
            host_id: "test".into(),
            root: temp.path().into(),
            root_paths: std::collections::BTreeMap::from([(
                "local".into(), vec![temp.path().into()],
            )]),
            limits: Default::default(),
            exclude_globs: vec!["**/blocked.txt".into()],
            index: crate::index::IndexManager::disabled(),
            remote_index: None,
        };
        for mode in [super::SearchMode::Literal, super::SearchMode::Regex] {
            let result = backend.search_text_bounded(
                "live needle", 5, 0, mode, vec![], vec!["local".into()],
            ).await.unwrap();
            assert!(!result.partial);
            assert_eq!(result.hits.len(), 1);
            assert_eq!(result.hits[0].path, needle.display().to_string());
        }
        let paths = backend.find_paths_bounded(
            "needle.txt", 5, vec!["local".into()],
        ).await.unwrap();
        assert!(!paths.partial);
        assert_eq!(paths.hits.len(), 1);
        assert_eq!(paths.hits[0].path, needle.display().to_string());
        let blocked = backend
            .find_paths_bounded("blocked.txt", 5, vec!["local".into()])
            .await
            .unwrap();
        assert!(!blocked.partial);
        assert!(blocked.hits.is_empty());
    }

    #[tokio::test]
    async fn empty_cached_index_does_not_hide_existing_live_text() {
        // Focused integration, expected <1s/max5s. Real SQLite+rg, no watcher
        // timing: model a retained index whose completed baseline missed a file.
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("scope");
        std::fs::create_dir(&root).unwrap();
        let hostname = root.join("hostname");
        std::fs::write(&hostname, "server-100\n").unwrap();
        let backend = super::LocalBackend {
            host_id: "test".into(),
            root: root.clone(),
            root_paths: std::collections::BTreeMap::from([("local".into(), vec![root.clone()])]),
            limits: Default::default(),
            exclude_globs: Vec::new(),
            index: crate::index::IndexManager::cached_test_index(
                &root,
                temp.path().join("index.sqlite"),
                &[],
            ),
            remote_index: None,
        };
        for globs in [vec![], vec!["**/hostname".into()]] {
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                backend.search_text_bounded(
                    "server",
                    5,
                    0,
                    super::SearchMode::CaseInsensitiveLiteral,
                    globs,
                    vec!["local".into()],
                ),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(!result.partial);
            assert_eq!(result.hits.len(), 1);
            assert_eq!(result.hits[0].path, hostname.display().to_string());
            assert_eq!(result.hits[0].text, "server-100");
        }
    }

    #[tokio::test]
    async fn cached_pdf_text_does_not_fall_back_to_binary_scan() {
        // Focused integration, expected <1s/max5s. Real SQLite cached extraction
        // over a real non-text PDF fixture; success cannot come from raw rg.
        let temp = tempfile::tempdir().unwrap();
        let pdf = temp.path().join("scan.pdf");
        std::fs::write(&pdf, b"%PDF\0raw bytes without extracted text").unwrap();
        let backend = super::LocalBackend {
            host_id: "test".into(),
            root: temp.path().into(),
            root_paths: std::collections::BTreeMap::from([(
                "local".into(),
                vec![temp.path().into()],
            )]),
            limits: Default::default(),
            exclude_globs: Vec::new(),
            index: crate::index::IndexManager::cached_test_index(
                temp.path(),
                temp.path().join("index.sqlite"),
                &[(&pdf, "needle cached PDF extraction")],
            ),
            remote_index: None,
        };
        let result = backend
            .search_text_bounded(
                "needle",
                5,
                0,
                super::SearchMode::Literal,
                vec!["**/*.pdf".into()],
                vec!["local".into()],
            )
            .await
            .unwrap();
        assert!(!result.partial);
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].text, "needle cached PDF extraction");
    }

    #[tokio::test]
    async fn preview_reads_primary_search_result_with_named_roots_and_rejects_escape() {
        let temp = tempfile::tempdir().unwrap();
        let primary = temp.path().join("primary");
        let extra = temp.path().join("extra");
        std::fs::create_dir_all(&primary).unwrap();
        std::fs::create_dir_all(&extra).unwrap();
        let path = primary.join("needle.txt");
        std::fs::write(&path, b"needle preview\n").unwrap();
        std::fs::write(extra.join("extra.txt"), b"extra preview\n").unwrap();
        let outside = temp.path().join("outside.txt");
        std::fs::write(&outside, b"outside\n").unwrap();
        let backend = super::LocalBackend {
            host_id: "test".into(),
            root: primary.clone(),
            root_paths: std::collections::BTreeMap::from([
                ("local".into(), vec![primary.clone()]),
                ("extra".into(), vec![extra.clone()]),
            ]),
            limits: Default::default(),
            exclude_globs: Vec::new(),
            index: crate::index::IndexManager::disabled(),
            remote_index: None,
        };
        let partial = primary.join("partial.txt");
        std::fs::write(&partial, b"first line\n\xffunreadable trailing bytes").unwrap();
        let preview = backend
            .read_text(&partial, Some(1), Some(1))
            .expect("a bounded line read must not decode bytes beyond its window");
        assert_eq!(preview[0].lines[0].text, "first line");
        assert!(backend.read_text(&partial, None, None).is_err());
        let giant_line = primary.join("giant-line.txt");
        std::fs::write(
            &giant_line,
            vec![b'x'; super::MAX_BOUNDED_TEXT_READ_BYTES + 1],
        )
        .unwrap();
        let bounded_error = backend
            .read_text(&giant_line, Some(1), Some(1))
            .expect_err("bounded preview reads must stop at the hard byte limit");
        assert!(bounded_error
            .to_string()
            .contains("1048576 byte read limit"));
        assert_eq!(backend.default_search_roots(), vec![extra.clone()]);
        let found = backend
            .find_paths_bounded("needle", 5, vec!["local".into()])
            .await
            .unwrap();
        assert_eq!(found.hits.len(), 1);
        assert!(!backend
            .read_text(std::path::Path::new(&found.hits[0].path), None, None)
            .unwrap()
            .is_empty());
        assert!(backend
            .read_text(&extra.join("extra.txt"), None, None)
            .is_ok());
        assert!(backend.read_text(&outside, None, None).is_err());
        assert!(backend
            .read_text(&primary.join("../outside.txt"), None, None)
            .is_err());
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&outside, primary.join("escape.txt")).unwrap();
            assert!(backend
                .read_text(&primary.join("escape.txt"), None, None)
                .is_err());
        }
    }
    #[tokio::test]
    async fn local_search_results_include_real_metadata_and_preserve_remote_fields() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("needle.txt");
        std::fs::write(&path, b"needle\n").unwrap();
        let backend = super::LocalBackend {
            host_id: "test".into(),
            root: temp.path().into(),
            root_paths: std::collections::BTreeMap::from([(
                "local".into(),
                vec![temp.path().into()],
            )]),
            limits: Default::default(),
            exclude_globs: Vec::new(),
            index: crate::index::IndexManager::disabled(),
            remote_index: None,
        };
        let expected = backend.list_directory(temp.path()).unwrap().remove(0);
        let paths = backend
            .find_paths_bounded("needle", 5, Vec::new())
            .await
            .unwrap();
        let text = backend
            .search_text_bounded(
                "needle",
                5,
                0,
                super::SearchMode::Literal,
                Vec::new(),
                Vec::new(),
            )
            .await
            .unwrap();
        for result in [paths, text] {
            assert_eq!(result.hits.len(), 1);
            let hit = &result.hits[0];
            assert_eq!(hit.size, Some(7));
            assert_eq!(hit.modified_ms, expected.modified_ms);
            assert_eq!(hit.created_ms, expected.created_ms);
            let decoded: super::SearchHit =
                serde_json::from_value(serde_json::to_value(hit).unwrap()).unwrap();
            assert_eq!(decoded.size, hit.size);
        }
        let mut remote = vec![super::SearchHit {
            host_id: "other".into(),
            path: path.display().to_string(),
            size: Some(999),
            modified_ms: Some(10),
            created_ms: Some(3),
            ..Default::default()
        }];
        backend.enrich_search_metadata(&mut remote);
        assert_eq!(remote[0].size, Some(999));
        assert_eq!(remote[0].created_ms, Some(3));
        let old: super::SearchHit = serde_json::from_value(
            serde_json::json!({"host_id":"old", "path":"/old", "line_number":0,"context":[]}),
        )
        .unwrap();
        assert_eq!(old.size, None);
        assert_eq!(old.created_ms, None);
    }
    #[test]
    fn browse_creation_time_matches_real_filesystem_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("file.txt");
        std::fs::write(&path, b"hello").unwrap();
        let backend = super::LocalBackend {
            host_id: "test".into(),
            root: temp.path().into(),
            root_paths: std::collections::BTreeMap::from([(
                "local".into(),
                vec![temp.path().into()],
            )]),
            limits: Default::default(),
            exclude_globs: Vec::new(),
            index: crate::index::IndexManager::disabled(),
            remote_index: None,
        };
        let entries = backend.list_directory(temp.path()).unwrap();
        let expected = std::fs::metadata(&path)
            .unwrap()
            .created()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .and_then(|duration| u64::try_from(duration.as_millis()).ok());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].created_ms, expected);
        assert_eq!(entries[0].size, Some(5));
    }
    #[test]
    fn older_directory_metadata_has_unknown_created_date() {
        let entry: super::DirectoryEntry = serde_json::from_value(serde_json::json!({
            "name":"old.txt", "path":"/old.txt", "kind":"file", "size":3, "modified_ms":42
        }))
        .unwrap();
        assert_eq!(entry.created_ms, None);
        assert_eq!(entry.modified_ms, Some(42));
    }
    use super::{non_readable_traversal_diagnostic, permission_only_traversal_diagnostic};

    #[test]
    fn non_readable_traversal_errors_have_one_safe_diagnostic_for_text_and_paths() {
        for stderr in [
            b"rg: ./Library/Mobile Documents: Operation not permitted (os error 1)\n".as_slice(),
            b"rg: ./iCloud: Permission denied (os error 13)\n".as_slice(),
            b"rg: ./volume: Input/output error (os error 5)\n".as_slice(),
            b"rg: ./Library/Mobile Documents: Resource deadlock avoided\n".as_slice(),
        ] {
            assert_eq!(
                non_readable_traversal_diagnostic(stderr).as_deref(),
                Some("some configured paths were not readable")
            );
        }
    }

    #[test]
    fn malformed_queries_are_not_misclassified_as_traversal_errors() {
        assert!(non_readable_traversal_diagnostic(b"regex parse error:\n    (\n").is_none());
    }

    #[test]
    fn only_permission_diagnostics_are_safe_to_ignore() {
        assert!(permission_only_traversal_diagnostic(
            b"rg: ./private: Permission denied (os error 13)\n"
        ));
        assert!(permission_only_traversal_diagnostic(
            b"rg: .\\ntuser.dat.LOG1: file is in use by another process (os error 32)\n"
        ));
        assert!(permission_only_traversal_diagnostic(
            b"rg: .\\OneDrive\\photo.jpg: cloud placeholder unavailable (os error 1920)\n"
        ));
        assert!(permission_only_traversal_diagnostic(
            b"rg: .\\ntuser.dat.LOG1: file is busy (os error 32)\nrg: .\\OneDrive\\photo.jpg: offline placeholder (os error 1920)\n"
        ));
        assert!(!permission_only_traversal_diagnostic(
            b"rg: ./volume: Input/output error (os error 5)\n"
        ));
    }
}
