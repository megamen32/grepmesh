use crate::topology::PeerConfig;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

pub fn default_exclude_globs() -> Vec<String> {
    [
        "**/.git/**",
        "**/.svn/**",
        "**/.hg/**",
        "**/node_modules/**",
        "**/.pnpm-store/**",
        "**/bower_components/**",
        "**/venv/**",
        "**/.venv/**",
        "**/__pycache__/**",
        "**/.tox/**",
        "**/.nox/**",
        "**/.pytest_cache/**",
        "**/.mypy_cache/**",
        "**/.ruff_cache/**",
        "**/target/**",
        "**/dist/**",
        "**/build/**",
        "**/out/**",
        "**/.next/**",
        "**/.nuxt/**",
        "**/coverage/**",
        "**/.cache/**",
        "**/cache/**",
        "**/caches/**",
        "**/Caches/**",
        "**/.tmp/**",
        "**/tmp/**",
        "**/temp/**",
        "**/logs/**",
        "**/log/**",
        "**/*.log",
        "**/.cargo/registry/**",
        "**/.cargo/git/**",
        "**/.rustup/**",
        "**/go/pkg/mod/**",
        "**/.local/share/Trash/**",
        "**/diag-live/**",
        "**/.grepmesh-jobs/**",
        "proc/**",
        "sys/**",
        "dev/**",
        "run/**",
        "**/proc/**",
        "**/sys/**",
        "**/dev/**",
        "**/run/**",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LimitsConfig {
    #[serde(default = "default_max_results")]
    pub max_results: usize,
    #[serde(default = "default_context_lines")]
    pub context_lines: usize,
    #[serde(default = "default_max_response_bytes")]
    pub max_response_bytes: usize,
    #[serde(default = "default_peer_timeout_ms")]
    pub peer_timeout_ms: u64,
    #[serde(default = "default_overall_timeout_ms")]
    pub overall_timeout_ms: u64,
    #[serde(default = "default_max_file_bytes")]
    pub max_file_bytes: u64,
    /// Independent ceiling for an asynchronous search job. This must exceed
    /// the foreground wait budget; it is not the synchronous fan-out timeout.
    #[serde(default = "default_search_job_timeout_ms")]
    pub search_job_timeout_ms: u64,
    #[serde(default = "default_search_job_ttl_ms")]
    pub search_job_ttl_ms: u64,
    #[serde(default = "default_search_job_max_bytes")]
    pub search_job_max_bytes: u64,
    #[serde(default = "default_search_job_store_max_bytes")]
    pub search_job_store_max_bytes: u64,
    /// Minimum delay between automatic full index reconciliations. Watcher
    /// events during the delay are coalesced into one later pass.
    #[serde(default = "default_full_rebuild_min_interval_ms")]
    pub full_rebuild_min_interval_ms: u64,
    #[serde(default)]
    pub index_activity: IndexActivityConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IndexActivityConfig {
    #[serde(default = "default_activity_window_ms")]
    pub window_ms: u64,
    #[serde(default = "default_hot_event_threshold")]
    pub hot_event_threshold: u64,
    #[serde(default = "default_hot_changed_bytes_threshold")]
    pub hot_changed_bytes_threshold: u64,
    #[serde(default = "default_hot_debounce_ms")]
    pub hot_debounce_ms: u64,
    #[serde(default = "default_hot_cooldown_ms")]
    pub hot_cooldown_ms: u64,
    #[serde(default)]
    pub metadata_only_globs: Vec<String>,
    #[serde(default)]
    pub exclude_globs: Vec<String>,
}

fn default_activity_window_ms() -> u64 {
    60_000
}
fn default_hot_event_threshold() -> u64 {
    300
}
fn default_hot_changed_bytes_threshold() -> u64 {
    256 * 1024 * 1024
}
fn default_hot_debounce_ms() -> u64 {
    30_000
}
fn default_hot_cooldown_ms() -> u64 {
    60 * 60 * 1_000
}

impl Default for IndexActivityConfig {
    fn default() -> Self {
        Self {
            window_ms: default_activity_window_ms(),
            hot_event_threshold: default_hot_event_threshold(),
            hot_changed_bytes_threshold: default_hot_changed_bytes_threshold(),
            hot_debounce_ms: default_hot_debounce_ms(),
            hot_cooldown_ms: default_hot_cooldown_ms(),
            metadata_only_globs: Vec::new(),
            exclude_globs: Vec::new(),
        }
    }
}

fn default_max_results() -> usize {
    64
}
fn default_context_lines() -> usize {
    2
}
fn default_max_response_bytes() -> usize {
    128 * 1024
}
fn default_peer_timeout_ms() -> u64 {
    2_000
}
fn default_overall_timeout_ms() -> u64 {
    5_000
}
fn default_max_file_bytes() -> u64 {
    16 * 1024 * 1024
}
fn default_search_job_timeout_ms() -> u64 {
    10 * 60 * 1_000
}
fn default_search_job_ttl_ms() -> u64 {
    30 * 60 * 1_000
}
fn default_search_job_max_bytes() -> u64 {
    8 * 1024 * 1024
}
fn default_search_job_store_max_bytes() -> u64 {
    64 * 1024 * 1024
}
fn default_full_rebuild_min_interval_ms() -> u64 {
    60 * 60 * 1_000
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_results: default_max_results(),
            context_lines: default_context_lines(),
            max_response_bytes: default_max_response_bytes(),
            peer_timeout_ms: default_peer_timeout_ms(),
            overall_timeout_ms: default_overall_timeout_ms(),
            max_file_bytes: default_max_file_bytes(),
            search_job_timeout_ms: default_search_job_timeout_ms(),
            search_job_ttl_ms: default_search_job_ttl_ms(),
            search_job_max_bytes: default_search_job_max_bytes(),
            search_job_store_max_bytes: default_search_job_store_max_bytes(),
            full_rebuild_min_interval_ms: default_full_rebuild_min_interval_ms(),
            index_activity: IndexActivityConfig::default(),
        }
    }
}

const DEFAULT_BACKUP_CATALOG_STALE_AFTER_MS: u64 = 24 * 60 * 60 * 1_000;

/// Optional, fixture-only configuration for the local backup catalog.
///
/// It names no object-store endpoint or credential. `fixture_path` remains
/// process configuration and is not part of the browser-facing catalog model.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct BackupCatalogConfig {
    #[serde(default)]
    pub provider_alias: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub fixture_path: Option<PathBuf>,
    #[serde(default = "default_backup_catalog_stale_after_ms")]
    pub stale_after_ms: u64,
}

fn default_backup_catalog_stale_after_ms() -> u64 {
    DEFAULT_BACKUP_CATALOG_STALE_AFTER_MS
}

impl BackupCatalogConfig {
    pub fn effective_stale_after_ms(&self) -> u64 {
        if self.stale_after_ms == 0 {
            DEFAULT_BACKUP_CATALOG_STALE_AFTER_MS
        } else {
            self.stale_after_ms
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct OcrConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_ocr_det_model")]
    pub det_model: String,
    #[serde(default = "default_ocr_rec_model")]
    pub rec_model: String,
    #[serde(default = "default_ocr_dict")]
    pub dict: String,
    #[serde(default = "default_true")]
    pub pdf_fallback: bool,
    #[serde(default = "default_ocr_min_pdf_text_chars")]
    pub min_pdf_text_chars: usize,
    #[serde(default = "default_ocr_pdf_dpi")]
    pub pdf_dpi: u32,
    #[serde(default = "default_ocr_max_pdf_pages")]
    pub max_pdf_pages: usize,
    #[serde(default = "default_ocr_max_image_bytes")]
    pub max_image_bytes: u64,
}

impl Default for OcrConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            det_model: default_ocr_det_model(),
            rec_model: default_ocr_rec_model(),
            dict: default_ocr_dict(),
            pdf_fallback: true,
            min_pdf_text_chars: default_ocr_min_pdf_text_chars(),
            pdf_dpi: default_ocr_pdf_dpi(),
            max_pdf_pages: default_ocr_max_pdf_pages(),
            max_image_bytes: default_ocr_max_image_bytes(),
        }
    }
}

fn default_ocr_det_model() -> String {
    "pp-ocrv6_tiny_det.onnx".to_string()
}
fn default_ocr_rec_model() -> String {
    "eslav_pp-ocrv5_mobile_rec.onnx".to_string()
}
fn default_ocr_dict() -> String {
    "ppocrv5_eslav_dict.txt".to_string()
}
fn default_ocr_min_pdf_text_chars() -> usize {
    64
}
fn default_ocr_pdf_dpi() -> u32 {
    180
}
fn default_ocr_max_pdf_pages() -> usize {
    200
}
fn default_ocr_max_image_bytes() -> u64 {
    256 * 1024 * 1024
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SttConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_stt_backend")]
    pub backend: String,
    #[serde(default = "default_stt_model")]
    pub model: String,
    #[serde(default)]
    pub model_dir: Option<PathBuf>,
    #[serde(default)]
    pub remote_base_url: Option<String>,
    #[serde(default = "default_stt_api_key_env")]
    pub api_key_env: String,
    #[serde(default = "default_true")]
    pub auto_download: bool,
    #[serde(default = "default_max_media_bytes")]
    pub max_media_bytes: u64,
}

impl Default for SttConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: default_stt_backend(),
            model: default_stt_model(),
            model_dir: None,
            remote_base_url: None,
            api_key_env: default_stt_api_key_env(),
            auto_download: true,
            max_media_bytes: default_max_media_bytes(),
        }
    }
}

fn default_stt_backend() -> String {
    "auto".to_string()
}
fn default_stt_model() -> String {
    "auto".to_string()
}
fn default_stt_api_key_env() -> String {
    "GREPMESH_STT_API_KEY".to_string()
}
fn default_true() -> bool {
    true
}
fn default_max_media_bytes() -> u64 {
    4 * 1024 * 1024 * 1024
}

/// Opt-in UserIO message cache adapter. Renders cached chat messages from a
/// local Universal UserIO SQLite store into plain-text conversation files
/// under `cache_dir`, and (per-stage, also opt-in) materializes chat
/// attachments there so the regular extraction pipeline indexes documents
/// (anydoc) and audio/video (whisper STT). Disabled by default: message
/// bodies are private and indexing them must be an explicit choice.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct UserioConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_userio_sqlite_path")]
    pub sqlite_path: PathBuf,
    #[serde(default = "default_userio_cache_dir")]
    pub cache_dir: PathBuf,
    #[serde(default = "default_userio_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Restrict rendering to these UserIO user ids; empty means all users.
    #[serde(default)]
    pub user_ids: Vec<String>,
    /// Cap on cache files written per sync pass. The initial population of a
    /// large store lands in bounded batches so the index watcher's hot
    /// directory protection never sees one giant write burst.
    #[serde(default = "default_userio_max_writes_per_sync")]
    pub max_writes_per_sync: usize,
    /// Restrict rendering to these message sources (gmail, telegram,
    /// whatsapp, sms, vk, matrix, chatgpt:*); empty means all sources.
    #[serde(default)]
    pub include_sources: Vec<String>,
    #[serde(default)]
    pub attachments: UserioAttachmentsConfig,
    #[serde(default = "default_userio_api_base")]
    pub api_base: String,
    #[serde(default = "default_userio_token_env")]
    pub token_env: String,
    /// Attachment downloads that fail are retried at most this many times
    /// per process before being skipped for the remainder of the run.
    #[serde(default = "default_userio_download_attempts")]
    pub max_download_attempts: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct UserioAttachmentsConfig {
    /// Materialize document attachments (PDF/office/text) so anydoc
    /// extraction indexes their text. In-DB transcripts are always inlined
    /// into the conversation render regardless of these flags.
    #[serde(default)]
    pub docs: bool,
    /// Materialize audio/video attachments so the STT stage transcribes
    /// them. Attachments that already carry a transcript in the UserIO
    /// store are only inlined, never downloaded, unless
    /// `materialize_transcribed` is set.
    #[serde(default)]
    pub media: bool,
    #[serde(default)]
    pub materialize_transcribed: bool,
    #[serde(default = "default_userio_max_attachment_bytes")]
    pub max_bytes: u64,
}

impl Default for UserioConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sqlite_path: default_userio_sqlite_path(),
            cache_dir: default_userio_cache_dir(),
            poll_interval_ms: default_userio_poll_interval_ms(),
            user_ids: Vec::new(),
            max_writes_per_sync: default_userio_max_writes_per_sync(),
            include_sources: Vec::new(),
            attachments: UserioAttachmentsConfig::default(),
            api_base: default_userio_api_base(),
            token_env: default_userio_token_env(),
            max_download_attempts: default_userio_download_attempts(),
        }
    }
}

impl Default for UserioAttachmentsConfig {
    fn default() -> Self {
        Self {
            docs: false,
            media: false,
            materialize_transcribed: false,
            max_bytes: default_userio_max_attachment_bytes(),
        }
    }
}

fn default_userio_sqlite_path() -> PathBuf {
    PathBuf::from("/var/lib/universal-userio/userio.sqlite3")
}
fn default_userio_cache_dir() -> PathBuf {
    PathBuf::from("/var/lib/grepmesh-mcp/userio-cache")
}
fn default_userio_poll_interval_ms() -> u64 {
    60_000
}
fn default_userio_max_writes_per_sync() -> usize {
    40
}
fn default_userio_api_base() -> String {
    "http://127.0.0.1:18093".to_string()
}
fn default_userio_token_env() -> String {
    "GREPMESH_USERIO_TOKEN".to_string()
}
fn default_userio_download_attempts() -> u32 {
    3
}
fn default_userio_max_attachment_bytes() -> u64 {
    256 * 1024 * 1024
}

impl UserioConfig {
    /// A UserIO adapter that never contacts the UserIO API. Used when only
    /// inline (already transcribed) attachment text is wanted.
    pub fn downloads_enabled(&self) -> bool {
        self.attachments.docs || self.attachments.media
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub host_id: String,
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,
    #[serde(default)]
    pub local_bind: Option<SocketAddr>,
    pub root: PathBuf,
    #[serde(default)]
    pub roots: BTreeMap<String, Vec<PathBuf>>,
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub exclude_globs: Vec<String>,
    #[serde(default)]
    pub topology_cache_path: Option<PathBuf>,
    /// Local persistent full-text index. Enabled by default; every GrepMesh node
    /// indexes its own configured roots and mesh fan-out provides one logical
    /// cross-host index without copying documents to a central server.
    #[serde(default)]
    pub index_path: Option<PathBuf>,
    #[serde(default)]
    pub gptadmin_topology_url: Option<String>,
    #[serde(default)]
    pub gptadmin_token_env: Option<String>,
    #[serde(default)]
    pub peer_auth_token_env: Option<String>,
    #[serde(default)]
    pub backup_catalog: Option<BackupCatalogConfig>,
    #[serde(default)]
    pub stt: SttConfig,
    #[serde(default)]
    pub ocr: OcrConfig,
    #[serde(default)]
    pub userio: UserioConfig,
    #[serde(default = "default_topology_ttl_ms")]
    pub topology_ttl_ms: u64,
    #[serde(skip)]
    pub config_path: Option<PathBuf>,
}

fn default_bind() -> SocketAddr {
    "127.0.0.1:9419".parse().expect("valid default bind")
}

fn default_index_path_for(host_id: &str, root: &Path) -> Option<PathBuf> {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in root.as_os_str().to_string_lossy().bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    std::env::var_os("HOME").map(PathBuf::from).map(|home| {
        let safe_host = host_id
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                    ch
                } else {
                    '_'
                }
            })
            .collect::<String>();
        home.join(".cache/grepmesh")
            .join(format!("{safe_host}-{hash:016x}.sqlite3"))
    })
}

fn default_topology_ttl_ms() -> u64 {
    30_000
}

impl AppConfig {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let bytes = fs::read(path.as_ref())
            .with_context(|| format!("read config {}", path.as_ref().display()))?;
        let mut cfg: AppConfig = serde_json::from_slice(&bytes).context("parse config JSON")?;
        cfg.config_path = Some(path.as_ref().to_path_buf());
        if cfg.limits.max_results == 0 {
            cfg.limits.max_results = default_max_results();
        }
        if cfg.limits.max_file_bytes == 0 {
            cfg.limits.max_file_bytes = default_max_file_bytes();
        }
        if cfg.index_path.is_none() {
            cfg.index_path = default_index_path_for(&cfg.host_id, &cfg.root);
        }
        Ok(cfg)
    }

    pub fn peer_timeout(&self) -> Duration {
        Duration::from_millis(self.limits.peer_timeout_ms)
    }

    pub fn overall_timeout(&self) -> Duration {
        Duration::from_millis(self.limits.overall_timeout_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::{default_exclude_globs, AppConfig};

    #[test]
    fn defaults_exclude_runtime_pseudo_filesystems() {
        let excludes = default_exclude_globs();
        for pattern in [
            "proc/**",
            "sys/**",
            "dev/**",
            "run/**",
            "**/.cache/**",
            "**/logs/**",
            "**/*.log",
            "**/build/**",
            "**/target/**",
            "**/.tmp/**",
        ] {
            assert!(excludes.contains(&pattern.to_string()), "missing {pattern}");
        }
    }

    #[test]
    fn bind_defaults_to_loopback() {
        let config: AppConfig = serde_json::from_value(serde_json::json!({
            "host_id": "local-test",
            "root": "/tmp"
        }))
        .unwrap();

        assert_eq!(config.bind.to_string(), "127.0.0.1:9419");
        assert!(config.peers.is_empty());
        assert!(config.peer_auth_token_env.is_none());
        assert!(config.index_path.is_none());
    }
}
