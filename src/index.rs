use crate::{
    backend::IndexState,
    config::{IndexActivityConfig, OcrConfig, SttConfig},
    ocr::OcrEngine,
    stt::SttEngine,
};
use globset::{Glob, GlobSet, GlobSetBuilder};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{mpsc, Arc, Mutex, RwLock},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

type IndexMap = BTreeMap<String, BTreeSet<PathBuf>>;
type DirectoryScan = (usize, IndexMap, Vec<IndexedDocument>, Vec<PathBuf>);

#[derive(Clone, Debug)]
struct IndexedDocument {
    path: PathBuf,
    body: String,
    size: u64,
    mtime_ns: u128,
}

#[derive(Clone, Debug)]
pub struct IndexTextHit {
    pub path: PathBuf,
    pub line_number: usize,
    pub text: String,
    pub context: Vec<(usize, String)>,
}

struct RebuildState<'a> {
    snapshot: &'a Arc<RwLock<IndexSnapshot>>,
    candidates: &'a Arc<RwLock<BTreeMap<String, BTreeSet<PathBuf>>>>,
    ready_roots: &'a Arc<RwLock<BTreeSet<PathBuf>>>,
    persistent: Option<&'a PersistentIndex>,
}

struct ScanContext<'a> {
    root: &'a Path,
    root_device: Option<u64>,
    excludes: &'a GlobSet,
    max_file_bytes: u64,
    build_candidates: bool,
    stt: Option<&'a SttEngine>,
    ocr: Option<&'a OcrEngine>,
    persistent: Option<&'a PersistentIndex>,
    metadata_only: Option<&'a GlobSet>,
}

#[cfg(unix)]
fn device_id(metadata: &fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.dev())
}

#[cfg(not(unix))]
fn device_id(_: &fs::Metadata) -> Option<u64> {
    None
}

#[derive(Clone, Debug)]
pub struct PersistentIndex {
    path: PathBuf,
}

impl PersistentIndex {
    pub fn open(path: PathBuf) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| format!("{}: {error}", parent.display()))?;
        }
        let store = Self { path };
        store.connection()?;
        store.backfill_cache_from_fts()?;
        Ok(store)
    }

    fn connection(&self) -> Result<Connection, String> {
        let connection = Connection::open(&self.path).map_err(|error| error.to_string())?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;\
                 CREATE VIRTUAL TABLE IF NOT EXISTS grepmesh_documents \
                 USING fts5(path UNINDEXED, body, tokenize='trigram');                 CREATE TABLE IF NOT EXISTS grepmesh_extraction_cache (                   path TEXT PRIMARY KEY,                   size INTEGER NOT NULL,                   mtime_ns TEXT NOT NULL,                   body TEXT NOT NULL                 );                 CREATE TABLE IF NOT EXISTS grepmesh_metadata (                   key TEXT PRIMARY KEY,                   value TEXT NOT NULL                 );",
            )
            .map_err(|error| error.to_string())?;
        Ok(connection)
    }

    fn document_count(&self) -> Result<usize, String> {
        self.connection()?
            .query_row("SELECT count(*) FROM grepmesh_documents", [], |row| {
                row.get(0)
            })
            .map_err(|error| error.to_string())
    }

    fn last_full_rebuild_ms(&self) -> Result<Option<u64>, String> {
        let connection = self.connection()?;
        match connection.query_row(
            "SELECT value FROM grepmesh_metadata WHERE key='last_full_rebuild_ms'",
            [],
            |row| row.get::<_, String>(0),
        ) {
            Ok(value) => value
                .parse::<u64>()
                .map(Some)
                .map_err(|error| error.to_string()),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error.to_string()),
        }
    }

    fn mark_full_rebuild_ms(&self, now_ms: u64) -> Result<(), String> {
        self.connection()?
            .execute(
                "INSERT INTO grepmesh_metadata(key, value) VALUES ('last_full_rebuild_ms', ?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![now_ms.to_string()],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn replace_indexed_document(&self, document: &IndexedDocument) -> Result<(), String> {
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let path = document.path.display().to_string();
        // Extraction cache hits still reach this writer during a full scan.
        // Skip writes only when both persisted representations agree; checking
        // the FTS row also preserves repair of missing, stale or duplicate rows.
        let unchanged: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM grepmesh_extraction_cache \
                 WHERE path=?1 AND size=?2 AND mtime_ns=?3 AND body=?4) \
                 AND (SELECT count(*)=1 AND min(body)=?4 \
                 FROM grepmesh_documents WHERE path=?1)",
                params![
                    path,
                    document.size as i64,
                    document.mtime_ns.to_string(),
                    document.body
                ],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        if unchanged {
            return Ok(());
        }
        transaction
            .execute(
                "DELETE FROM grepmesh_documents WHERE path = ?1",
                params![path],
            )
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO grepmesh_documents(path, body) VALUES (?1, ?2)",
                params![path, document.body],
            )
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO grepmesh_extraction_cache(path, size, mtime_ns, body) VALUES (?1, ?2, ?3, ?4)                  ON CONFLICT(path) DO UPDATE SET size=excluded.size, mtime_ns=excluded.mtime_ns, body=excluded.body",
                params![path, document.size as i64, document.mtime_ns.to_string(), document.body],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn replace_document(&self, path: &Path, body: &str) -> Result<(), String> {
        let metadata = fs::metadata(path).ok();
        let document = IndexedDocument {
            path: path.to_path_buf(),
            body: body.to_string(),
            size: metadata.as_ref().map(|m| m.len()).unwrap_or(0),
            mtime_ns: metadata.as_ref().map(metadata_mtime_ns).unwrap_or(0),
        };
        self.replace_indexed_document(&document)
    }

    fn cached_body(
        &self,
        path: &Path,
        size: u64,
        mtime_ns: u128,
    ) -> Result<Option<String>, String> {
        let connection = self.connection()?;
        let mut statement = connection
            .prepare("SELECT body FROM grepmesh_extraction_cache WHERE path=?1 AND size=?2 AND mtime_ns=?3")
            .map_err(|error| error.to_string())?;
        let mut rows = statement
            .query(params![
                path.display().to_string(),
                size as i64,
                mtime_ns.to_string()
            ])
            .map_err(|error| error.to_string())?;
        match rows.next().map_err(|error| error.to_string())? {
            Some(row) => row
                .get::<_, String>(0)
                .map(Some)
                .map_err(|error| error.to_string()),
            None => Ok(None),
        }
    }

    fn prune_except(&self, seen: &BTreeSet<PathBuf>) -> Result<(), String> {
        let mut connection = self.connection()?;
        let paths = {
            let mut statement = connection
                .prepare("SELECT path FROM grepmesh_documents")
                .map_err(|error| error.to_string())?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|error| error.to_string())?;
            rows.filter_map(Result::ok)
                .map(PathBuf::from)
                .collect::<Vec<_>>()
        };
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        for path in paths.into_iter().filter(|path| !seen.contains(path)) {
            let path = path.display().to_string();
            transaction
                .execute(
                    "DELETE FROM grepmesh_documents WHERE path=?1",
                    params![path.clone()],
                )
                .map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "DELETE FROM grepmesh_extraction_cache WHERE path=?1",
                    params![path],
                )
                .map_err(|error| error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(())
    }

    fn remove_document(&self, path: &Path) -> Result<(), String> {
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let path = path.display().to_string();
        transaction
            .execute(
                "DELETE FROM grepmesh_documents WHERE path=?1",
                params![path.clone()],
            )
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "DELETE FROM grepmesh_extraction_cache WHERE path=?1",
                params![path],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())
    }

    fn backfill_cache_from_fts(&self) -> Result<(), String> {
        let mut connection = self.connection()?;
        let existing = {
            let mut statement = connection
                .prepare("SELECT path, body FROM grepmesh_documents WHERE path NOT IN (SELECT path FROM grepmesh_extraction_cache)")
                .map_err(|error| error.to_string())?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|error| error.to_string())?;
            rows.filter_map(Result::ok).collect::<Vec<_>>()
        };
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        for (path, body) in existing {
            let metadata = match fs::metadata(&path) {
                Ok(metadata) if metadata.is_file() => metadata,
                _ => continue,
            };
            transaction.execute(
                "INSERT OR IGNORE INTO grepmesh_extraction_cache(path, size, mtime_ns, body) VALUES (?1, ?2, ?3, ?4)",
                params![path, metadata.len() as i64, metadata_mtime_ns(&metadata).to_string(), body],
            ).map_err(|error| error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn clear(&self) -> Result<(), String> {
        let connection = self.connection()?;
        connection
            .execute_batch("DELETE FROM grepmesh_documents; DELETE FROM grepmesh_extraction_cache; DELETE FROM grepmesh_metadata;")
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn candidates(&self, query: &str) -> Result<Vec<PathBuf>, String> {
        let connection = self.connection()?;
        let fts_query = fts_literal_query(query);
        let mut statement = connection
            .prepare("SELECT path FROM grepmesh_documents WHERE body MATCH ?1")
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map(params![fts_query], |row| row.get::<_, String>(0))
            .map_err(|error| error.to_string())?;
        rows.map(|row| row.map(PathBuf::from).map_err(|error| error.to_string()))
            .collect()
    }

    fn matching_documents(&self, query: &str) -> Result<Vec<(PathBuf, String)>, String> {
        let connection = self.connection()?;
        let fts_query = fts_literal_query(query);
        let mut documents = BTreeMap::<PathBuf, String>::new();
        {
            let mut statement = connection
                .prepare(
                    "SELECT path, body FROM grepmesh_documents WHERE body MATCH ?1 ORDER BY rank",
                )
                .map_err(|error| error.to_string())?;
            let rows = statement
                .query_map(params![fts_query], |row| {
                    Ok((
                        PathBuf::from(row.get::<_, String>(0)?),
                        row.get::<_, String>(1)?,
                    ))
                })
                .map_err(|error| error.to_string())?;
            for row in rows {
                let (path, body) = row.map_err(|error| error.to_string())?;
                documents.insert(path, body);
            }
        }
        let path_pattern = format!("%{}%", query.replace('%', r"\%").replace('_', r"\_"));
        {
            let mut statement = connection
                .prepare("SELECT path, body FROM grepmesh_documents WHERE path LIKE ?1 ESCAPE '\\'")
                .map_err(|error| error.to_string())?;
            let rows = statement
                .query_map(params![path_pattern], |row| {
                    Ok((
                        PathBuf::from(row.get::<_, String>(0)?),
                        row.get::<_, String>(1)?,
                    ))
                })
                .map_err(|error| error.to_string())?;
            for row in rows {
                let (path, body) = row.map_err(|error| error.to_string())?;
                documents.entry(path).or_insert(body);
            }
        }
        Ok(documents.into_iter().collect())
    }
}

#[derive(Clone, Debug)]
pub struct IndexSnapshot {
    pub current_path: Option<String>,
    pub state: IndexState,
    pub generation: u64,
    pub indexed_files: usize,
    pub last_error: Option<String>,
    pub directory_activity: Vec<IndexDirectoryActivity>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexDirectoryActivity {
    pub path: String,
    pub events: u64,
    pub changed_bytes: u64,
    pub hot: bool,
    pub metadata_only: bool,
    pub excluded: bool,
    pub debounce_ms: u64,
    pub last_event_ms: u64,
}

#[derive(Default)]
struct DirectoryActivityState {
    window_started_ms: u64,
    events: u64,
    changed_bytes: u64,
    hot_until_ms: u64,
    last_indexed_ms: u64,
    last_event_ms: u64,
    metadata_only: bool,
    excluded: bool,
    debounce_ms: u64,
}

impl Default for IndexSnapshot {
    fn default() -> Self {
        Self {
            state: IndexState::Building,
            generation: 0,
            indexed_files: 0,
            last_error: None,
            current_path: None,
            directory_activity: Vec::new(),
        }
    }
}

#[derive(Clone)]
pub struct IndexManager {
    snapshot: Arc<RwLock<IndexSnapshot>>,
    candidates: Arc<RwLock<BTreeMap<String, BTreeSet<PathBuf>>>>,
    ready_roots: Arc<RwLock<BTreeSet<PathBuf>>>,
    persistent: Option<PersistentIndex>,
    enabled: bool,
}

impl IndexManager {
    pub fn disabled() -> Self {
        Self {
            snapshot: Arc::new(RwLock::new(IndexSnapshot {
                state: IndexState::Ready,
                ..Default::default()
            })),
            candidates: Arc::new(RwLock::new(BTreeMap::new())),
            ready_roots: Arc::new(RwLock::new(BTreeSet::new())),
            persistent: None,
            enabled: false,
        }
    }

    pub fn start(
        roots: BTreeMap<String, Vec<PathBuf>>,
        excludes: Vec<String>,
        max_file_bytes: u64,
        full_rebuild_min_interval_ms: u64,
        activity_config: IndexActivityConfig,
        persistent_path: Option<PathBuf>,
        stt_config: SttConfig,
        ocr_config: OcrConfig,
    ) -> Self {
        let snapshot = Arc::new(RwLock::new(IndexSnapshot {
            state: IndexState::Building,
            ..Default::default()
        }));
        let state = Arc::clone(&snapshot);
        let candidates = Arc::new(RwLock::new(BTreeMap::new()));
        let candidate_state = Arc::clone(&candidates);
        let ready_roots = Arc::new(RwLock::new(BTreeSet::new()));
        let ready_root_state = Arc::clone(&ready_roots);
        let persistent =
            persistent_path.and_then(|path| match PersistentIndex::open(path.clone()) {
                Ok(index) => Some(index),
                Err(error) => {
                    if let Ok(mut current) = state.write() {
                        current.state = IndexState::Degraded;
                        current.last_error =
                            Some(format!("open persistent index {}: {error}", path.display()));
                    }
                    None
                }
            });
        let persistent_state = persistent.clone();
        let stt = SttEngine::new(stt_config);
        let ocr = OcrEngine::new(ocr_config);
        thread::spawn(move || {
            let index_storage_path = persistent_state.as_ref().map(|index| index.path.clone());
            let (events, rx) = mpsc::sync_channel(1);
            let pending_event_paths = Arc::new(Mutex::new(BTreeMap::<PathBuf, u64>::new()));
            let watcher_pending_paths = Arc::clone(&pending_event_paths);
            let watched_index_storage_path = index_storage_path.clone();
            let watch_roots = ordered_roots(&roots);
            thread::spawn(move || {
                let mut watcher: RecommendedWatcher = match notify::recommended_watcher(
                    move |event: notify::Result<notify::Event>| {
                        if let Ok(event) = event {
                            let paths =
                                event
                                    .paths
                                    .into_iter()
                                    .filter(|path| {
                                        !watched_index_storage_path.as_ref().is_some_and(
                                            |index_path| is_index_storage_path(path, index_path),
                                        )
                                    })
                                    .collect::<Vec<_>>();
                            if !paths.is_empty() {
                                if let Ok(mut pending) = watcher_pending_paths.lock() {
                                    for path in paths {
                                        *pending.entry(path).or_default() += 1;
                                    }
                                }
                                let _ = events.try_send(());
                            }
                        }
                    },
                ) {
                    Ok(watcher) => watcher,
                    Err(error) => {
                        tracing::error!(error = %error, "create GrepMesh watcher failed");
                        return;
                    }
                };
                for root in watch_roots {
                    if let Err(error) = watcher.watch(&root, RecursiveMode::Recursive) {
                        tracing::warn!(root = %root.display(), error = %error, "watch GrepMesh root failed");
                    }
                }
                loop {
                    thread::park();
                }
            });
            let configured_roots = ordered_roots(&roots).into_iter().collect::<BTreeSet<_>>();
            let now_ms = unix_time_ms();
            let mut last_full_rebuild_ms = persistent_state
                .as_ref()
                .and_then(|index| index.last_full_rebuild_ms().ok().flatten());
            // A populated legacy database predates the admission marker. Keep
            // serving it and schedule its first reconciliation after the host
            // interval instead of forcing another expensive startup walk.
            if last_full_rebuild_ms.is_none()
                && persistent_state
                    .as_ref()
                    .and_then(|index| index.document_count().ok())
                    .is_some_and(|count| count > 0)
            {
                if let Some(index) = persistent_state.as_ref() {
                    if index.mark_full_rebuild_ms(now_ms).is_ok() {
                        last_full_rebuild_ms = Some(now_ms);
                    }
                }
            }
            let rebuild_due = |last: Option<u64>| {
                let now = unix_time_ms();
                last.is_none_or(|last| {
                    now < last || now.saturating_sub(last) >= full_rebuild_min_interval_ms
                })
            };
            let run_rebuild = |last: &mut Option<u64>, generation: &mut u64| {
                let succeeded = rebuild_index(
                    &roots,
                    &excludes,
                    max_file_bytes,
                    stt.as_ref(),
                    ocr.as_ref(),
                    &activity_config,
                    RebuildState {
                        snapshot: &state,
                        candidates: &candidate_state,
                        ready_roots: &ready_root_state,
                        persistent: persistent_state.as_ref(),
                    },
                    generation,
                );
                if succeeded {
                    let completed_ms = unix_time_ms();
                    if let Some(index) = persistent_state.as_ref() {
                        let _ = index.mark_full_rebuild_ms(completed_ms);
                    }
                    *last = Some(completed_ms);
                }
            };
            let mut generation = 0;
            if rebuild_due(last_full_rebuild_ms) || persistent_state.is_none() {
                run_rebuild(&mut last_full_rebuild_ms, &mut generation);
            } else {
                if let Ok(mut ready) = ready_root_state.write() {
                    *ready = configured_roots;
                }
                if let Ok(mut current) = state.write() {
                    current.state = IndexState::Ready;
                    current.indexed_files = persistent_state
                        .as_ref()
                        .and_then(|index| index.document_count().ok())
                        .unwrap_or(0);
                }
            }
            let mut pending = false;
            let mut directory_activity = BTreeMap::<PathBuf, DirectoryActivityState>::new();
            loop {
                let activity_delay_until = directory_activity
                    .values()
                    .map(|stats| stats.hot_until_ms)
                    .max()
                    .unwrap_or(0);
                let remaining = if pending {
                    last_full_rebuild_ms
                        .map(|last| {
                            let due = last
                                .saturating_add(full_rebuild_min_interval_ms)
                                .max(activity_delay_until);
                            Duration::from_millis(due.saturating_sub(unix_time_ms()).min(30_000))
                        })
                        .unwrap_or(Duration::from_secs(30))
                } else {
                    Duration::from_secs(30)
                };
                if rx.recv_timeout(remaining).is_ok() {
                    // Filesystems commonly emit a burst of events for one
                    // logical update. Wait for a quiet interval before the
                    // expensive reconciliation and never advertise Building
                    // until a rebuild actually starts.
                    let debounce_deadline = Instant::now() + Duration::from_secs(2);
                    while let Some(remaining) =
                        debounce_deadline.checked_duration_since(Instant::now())
                    {
                        match rx.recv_timeout(remaining) {
                            Ok(()) => continue,
                            Err(_) => break,
                        }
                    }
                    let event_counts = pending_event_paths
                        .lock()
                        .map(|mut pending| std::mem::take(&mut *pending))
                        .unwrap_or_default();
                    let event_counts = event_counts
                        .into_iter()
                        .filter(|(path, _)| {
                            !index_storage_path
                                .as_ref()
                                .is_some_and(|index_path| is_index_storage_path(path, index_path))
                        })
                        .collect::<BTreeMap<_, _>>();
                    if event_counts.is_empty() {
                        continue;
                    }
                    let changed_paths = event_counts.keys().cloned().collect::<BTreeSet<_>>();
                    if let Some(index) = persistent_state.as_ref() {
                        reconcile_event_paths(
                            &changed_paths,
                            &event_counts,
                            &roots,
                            &excludes,
                            &activity_config,
                            &mut directory_activity,
                            &state,
                            max_file_bytes,
                            stt.as_ref(),
                            ocr.as_ref(),
                            index,
                        );
                    }
                    pending = true;
                }
                if pending
                    && (persistent_state.is_none()
                        || (rebuild_due(last_full_rebuild_ms)
                            && unix_time_ms() >= activity_delay_until))
                {
                    run_rebuild(&mut last_full_rebuild_ms, &mut generation);
                    pending = false;
                }
            }
        });
        Self {
            snapshot,
            candidates,
            ready_roots,
            persistent,
            enabled: true,
        }
    }

    pub fn status(&self) -> IndexSnapshot {
        let mut snapshot = self
            .snapshot
            .read()
            .map(|s| s.clone())
            .unwrap_or(IndexSnapshot {
                state: IndexState::Degraded,
                ..Default::default()
            });
        if snapshot.state != IndexState::Building {
            snapshot.current_path = None;
        }
        snapshot
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Only three metadata reads; polling never opens SQLite or walks roots.
    pub fn database_bytes(&self) -> Option<u64> {
        let index = self.persistent.as_ref()?;
        Some(
            ["", "-wal", "-shm"]
                .iter()
                .map(|suffix| {
                    let mut path = index.path.as_os_str().to_os_string();
                    path.push(suffix);
                    fs::metadata(PathBuf::from(path))
                        .map(|m| m.len())
                        .unwrap_or(0)
                })
                .sum(),
        )
    }

    pub fn search_text_hits(
        &self,
        query: &str,
        root: &Path,
        limit: usize,
        context_lines: usize,
        case_sensitive: bool,
    ) -> Option<Vec<IndexTextHit>> {
        if query.as_bytes().len() < 3
            || !self
                .ready_roots
                .read()
                .ok()
                .is_some_and(|roots| roots.iter().any(|ready| root.starts_with(ready)))
        {
            return None;
        }
        let persistent = self.persistent.as_ref()?;
        let documents = persistent.matching_documents(query).ok()?;
        let needle = if case_sensitive {
            query.to_string()
        } else {
            query.to_lowercase()
        };
        let mut hits = Vec::new();
        for (path, body) in documents {
            if !path.starts_with(root) {
                continue;
            }
            let mut content_match = false;
            let lines: Vec<&str> = body.lines().collect();
            for (index, line) in lines.iter().enumerate() {
                let haystack = if case_sensitive {
                    (*line).to_string()
                } else {
                    line.to_lowercase()
                };
                if !haystack.contains(&needle) {
                    continue;
                }
                content_match = true;
                let start = index.saturating_sub(context_lines);
                let end = (index + context_lines + 1).min(lines.len());
                hits.push(IndexTextHit {
                    path: path.clone(),
                    line_number: index + 1,
                    text: (*line).to_string(),
                    context: (start..end)
                        .map(|i| (i + 1, lines[i].to_string()))
                        .collect(),
                });
                if hits.len() >= limit {
                    return Some(hits);
                }
            }
            let path_text = path.to_string_lossy();
            let path_haystack = if case_sensitive {
                path_text.to_string()
            } else {
                path_text.to_lowercase()
            };
            if !content_match && path_haystack.contains(&needle) {
                hits.push(IndexTextHit {
                    path: path.clone(),
                    line_number: 0,
                    text: path_text.to_string(),
                    context: Vec::new(),
                });
                if hits.len() >= limit {
                    return Some(hits);
                }
            }
        }
        Some(hits)
    }

    pub fn candidate_paths(&self, query: &str, root: &Path) -> Option<Vec<PathBuf>> {
        if !self
            .ready_roots
            .read()
            .ok()
            .is_some_and(|roots| roots.iter().any(|ready| root.starts_with(ready)))
        {
            return None;
        }
        let grams = trigrams(&query.to_ascii_lowercase());
        if grams.is_empty() {
            return None;
        }
        if let Some(persistent) = &self.persistent {
            match persistent.candidates(query) {
                Ok(paths) => {
                    return Some(
                        paths
                            .into_iter()
                            .filter(|path| path.starts_with(root))
                            .collect(),
                    );
                }
                Err(_) => return None,
            }
        }
        let map = self.candidates.read().ok()?;
        let mut sets = grams.iter().map(|gram| map.get(gram));
        let first = sets.next()?.cloned()?;
        let result: Vec<_> = sets
            .try_fold(first, |acc, set| {
                set.map(|set| acc.intersection(set).cloned().collect())
            })?
            .into_iter()
            .filter(|path| path.starts_with(root))
            .collect();
        Some(result)
    }
}

fn is_index_storage_path(path: &Path, index_path: &Path) -> bool {
    let normalize = |candidate: &Path| {
        fs::canonicalize(candidate).unwrap_or_else(|_| {
            candidate
                .parent()
                .and_then(|parent| fs::canonicalize(parent).ok())
                .zip(candidate.file_name())
                .map(|(parent, name)| parent.join(name))
                .unwrap_or_else(|| candidate.to_path_buf())
        })
    };
    let normalized_path = normalize(path);
    let normalized_index = normalize(index_path);
    normalized_path == normalized_index
        || ["-wal", "-shm", "-journal"].iter().any(|suffix| {
            let mut sidecar = index_path.as_os_str().to_os_string();
            sidecar.push(suffix);
            let sidecar = PathBuf::from(sidecar);
            normalized_path == normalize(&sidecar)
        })
}

fn reconcile_event_paths(
    paths: &BTreeSet<PathBuf>,
    event_counts: &BTreeMap<PathBuf, u64>,
    roots: &BTreeMap<String, Vec<PathBuf>>,
    excludes: &[String],
    activity_config: &IndexActivityConfig,
    activity: &mut BTreeMap<PathBuf, DirectoryActivityState>,
    snapshot: &Arc<RwLock<IndexSnapshot>>,
    max_file_bytes: u64,
    stt: Option<&SttEngine>,
    ocr: Option<&OcrEngine>,
    persistent: &PersistentIndex,
) {
    let matcher = match compile_excludes(excludes) {
        Ok(matcher) => matcher,
        Err(error) => {
            tracing::warn!(error = %error, "incremental index update skipped");
            return;
        }
    };
    let activity_excludes = compile_excludes(&activity_config.exclude_globs).ok();
    let metadata_only = compile_excludes(&activity_config.metadata_only_globs).ok();
    let configured_roots = ordered_roots(roots)
        .into_iter()
        .map(|root| {
            let canonical = fs::canonicalize(&root).unwrap_or_else(|_| root.clone());
            (root, canonical)
        })
        .collect::<Vec<_>>();
    // Count and classify the whole batch before doing any extraction. Status
    // remains current even if one document converter is slow.
    for raw_path in paths {
        let occurrences = event_counts.get(raw_path).copied().unwrap_or(1);
        let Some((root, canonical_root)) = configured_roots
            .iter()
            .filter(|(_, canonical)| raw_path.starts_with(canonical))
            .max_by_key(|(_, canonical)| canonical.components().count())
        else {
            continue;
        };
        let path = raw_path
            .strip_prefix(canonical_root)
            .map(|relative| root.join(relative))
            .unwrap_or_else(|_| raw_path.clone());
        if path.exists() && !path.is_file() {
            continue;
        }
        let relative = path.strip_prefix(root).unwrap_or(&path);
        let bucket = if path.parent() == Some(root.as_path()) {
            root.clone()
        } else {
            relative
                .components()
                .next()
                .map(|component| root.join(component.as_os_str()))
                .unwrap_or_else(|| root.clone())
        };
        let now = unix_time_ms();
        let changed_bytes = fs::metadata(&path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let stats = activity.entry(bucket).or_default();
        if stats.window_started_ms == 0
            || now.saturating_sub(stats.window_started_ms) >= activity_config.window_ms
        {
            stats.window_started_ms = now;
            stats.events = 0;
            stats.changed_bytes = 0;
        }
        stats.events = stats.events.saturating_add(occurrences);
        stats.changed_bytes = stats
            .changed_bytes
            .saturating_add(changed_bytes.saturating_mul(occurrences));
        stats.last_event_ms = now;
        if stats.events >= activity_config.hot_event_threshold
            || stats.changed_bytes >= activity_config.hot_changed_bytes_threshold
        {
            stats.hot_until_ms = now.saturating_add(activity_config.hot_cooldown_ms);
        }
        let is_hot = now < stats.hot_until_ms;
        stats.excluded = activity_excludes
            .as_ref()
            .is_some_and(|matcher| excluded(&path, root, matcher));
        stats.metadata_only = is_hot
            || metadata_only
                .as_ref()
                .is_some_and(|matcher| excluded(&path, root, matcher));
        stats.debounce_ms = if is_hot {
            activity_config.hot_debounce_ms
        } else {
            2_000
        };
    }
    let activity_snapshot = directory_activity_snapshot(activity);
    if let Ok(mut current) = snapshot.write() {
        current.directory_activity = activity_snapshot.clone();
    }
    for path in paths {
        let Some((root, canonical_root)) = configured_roots
            .iter()
            .filter(|(_, canonical)| path.starts_with(canonical))
            .max_by_key(|(_, canonical)| canonical.components().count())
        else {
            continue;
        };
        let logical_path = path
            .strip_prefix(canonical_root)
            .map(|relative| root.join(relative))
            .unwrap_or_else(|_| path.clone());
        let path = logical_path.as_path();
        // Directory metadata events are emitted for every child write. They
        // carry no file content change and would falsely make the root hot.
        if path.exists() && !path.is_file() {
            continue;
        }
        let relative = path.strip_prefix(root).unwrap_or(path);
        let bucket = if path.parent() == Some(root.as_path()) {
            root.clone()
        } else {
            relative
                .components()
                .next()
                .map(|component| root.join(component.as_os_str()))
                .unwrap_or_else(|| root.clone())
        };
        let now = unix_time_ms();
        let stats = activity.entry(bucket.clone()).or_default();
        let is_hot = now < stats.hot_until_ms;
        let is_activity_excluded = activity_excludes
            .as_ref()
            .is_some_and(|matcher| excluded(path, root, matcher));
        let is_metadata_only = is_hot
            || metadata_only
                .as_ref()
                .is_some_and(|matcher| excluded(path, root, matcher));
        stats.metadata_only = is_metadata_only;
        stats.excluded = is_activity_excluded;
        stats.debounce_ms = if is_hot {
            activity_config.hot_debounce_ms
        } else {
            2_000
        };
        if is_activity_excluded {
            let _ = persistent.remove_document(path);
            continue;
        }
        if excluded(path, root, &matcher) {
            continue;
        }
        if is_hot && now.saturating_sub(stats.last_indexed_ms) < activity_config.hot_debounce_ms {
            continue;
        }
        if !path.is_file() {
            if !path.exists() {
                let _ = persistent.remove_document(path);
            }
            continue;
        }
        if is_metadata_only {
            if let Ok(metadata) = fs::metadata(path) {
                let document = IndexedDocument {
                    path: path.to_path_buf(),
                    body: format!(
                        "{}\npath: {}\nsize: {}\nmodified_ns: {}",
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or_default(),
                        path.display(),
                        metadata.len(),
                        metadata_mtime_ns(&metadata)
                    ),
                    size: metadata.len(),
                    mtime_ns: metadata_mtime_ns(&metadata),
                };
                let _ = persistent.replace_indexed_document(&document);
                stats.last_indexed_ms = now;
            }
            continue;
        }
        match scan_directory_unit(
            path,
            root,
            &matcher,
            max_file_bytes,
            false,
            stt,
            ocr,
            Some(persistent),
            metadata_only.as_ref(),
        ) {
            Ok((_, _, documents, _)) if documents.is_empty() => {
                let _ = persistent.remove_document(path);
            }
            Ok((_, _, documents, _)) => {
                for document in documents {
                    if let Err(error) = persistent.replace_indexed_document(&document) {
                        tracing::warn!(path = %path.display(), error = %error, "incremental index update failed");
                    }
                }
                stats.last_indexed_ms = now;
            }
            Err(error) => {
                tracing::warn!(path = %path.display(), error = %error, "incremental index update failed");
            }
        }
    }
}

fn directory_activity_snapshot(
    activity: &BTreeMap<PathBuf, DirectoryActivityState>,
) -> Vec<IndexDirectoryActivity> {
    let now = unix_time_ms();
    activity
        .iter()
        .map(|(path, stats)| IndexDirectoryActivity {
            path: path.display().to_string(),
            events: stats.events,
            changed_bytes: stats.changed_bytes,
            hot: now < stats.hot_until_ms,
            metadata_only: stats.metadata_only,
            excluded: stats.excluded,
            debounce_ms: stats.debounce_ms,
            last_event_ms: stats.last_event_ms,
        })
        .collect()
}

fn rebuild_index(
    roots: &BTreeMap<String, Vec<PathBuf>>,
    excludes: &[String],
    max_file_bytes: u64,
    stt: Option<&SttEngine>,
    ocr: Option<&OcrEngine>,
    activity_config: &IndexActivityConfig,
    rebuild: RebuildState<'_>,
    generation: &mut u64,
) -> bool {
    let matcher = match compile_excludes(excludes) {
        Ok(matcher) => matcher,
        Err(error) => {
            if let Ok(mut current) = rebuild.snapshot.write() {
                current.state = IndexState::Degraded;
                current.last_error = Some(error);
                current.generation = generation.saturating_add(1);
            }
            return false;
        }
    };
    let metadata_only = compile_excludes(&activity_config.metadata_only_globs).ok();
    let mut count = 0;
    let mut seen_paths = BTreeSet::new();
    if let Ok(mut current) = rebuild.snapshot.write() {
        current.state = IndexState::Building;
        current.indexed_files = 0;
        current.last_error = None;
    }
    if let Ok(mut map) = rebuild.candidates.write() {
        map.clear();
    }
    if let Ok(mut ready) = rebuild.ready_roots.write() {
        ready.clear();
    }
    for root in ordered_roots(roots) {
        let mut units: VecDeque<_> = match root_units(&root) {
            Ok(units) => units.into(),
            Err(error) => {
                if let Ok(mut current) = rebuild.snapshot.write() {
                    current.state = IndexState::Degraded;
                    current.last_error = Some(error);
                    current.generation = generation.saturating_add(1);
                }
                return false;
            }
        };
        while let Some(unit) = units.pop_front() {
            if let Ok(mut current) = rebuild.snapshot.write() {
                current.current_path = Some(unit.display().to_string());
            }
            let result = scan_directory_unit(
                &unit,
                &root,
                &matcher,
                max_file_bytes,
                rebuild.persistent.is_none(),
                stt,
                ocr,
                rebuild.persistent,
                metadata_only.as_ref(),
            );
            let (unit_count, next, documents, children) = match result {
                Ok(result) => result,
                Err(error) => {
                    if let Ok(mut current) = rebuild.snapshot.write() {
                        current.state = IndexState::Degraded;
                        current.last_error = Some(error);
                        current.generation = generation.saturating_add(1);
                    }
                    return false;
                }
            };
            if rebuild.persistent.is_none() {
                if let Ok(mut map) = rebuild.candidates.write() {
                    for (gram, paths) in next {
                        map.entry(gram).or_default().extend(paths);
                    }
                }
            }
            if let Some(persistent) = rebuild.persistent {
                for document in documents {
                    seen_paths.insert(document.path.clone());
                    if let Err(error) = persistent.replace_indexed_document(&document) {
                        if let Ok(mut current) = rebuild.snapshot.write() {
                            current.state = IndexState::Degraded;
                            current.last_error =
                                Some(format!("{}: {error}", document.path.display()));
                            current.generation = generation.saturating_add(1);
                        }
                        return false;
                    }
                }
            }
            units.extend(children);
            count += unit_count;
            *generation += 1;
            if let Ok(mut current) = rebuild.snapshot.write() {
                current.indexed_files = count;
                current.generation = *generation;
            }
        }
        if let Ok(mut ready) = rebuild.ready_roots.write() {
            ready.insert(root);
        }
    }
    if let Some(persistent) = rebuild.persistent {
        if let Err(error) = persistent.prune_except(&seen_paths) {
            if let Ok(mut current) = rebuild.snapshot.write() {
                current.state = IndexState::Degraded;
                current.last_error = Some(error);
                current.generation = generation.saturating_add(1);
            }
            return false;
        }
    }
    if let Ok(mut current) = rebuild.snapshot.write() {
        current.state = IndexState::Ready;
        current.last_error = None;
    }
    true
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

#[cfg(test)]
fn build_root_index(
    root: &Path,
    excludes: &[String],
    max_file_bytes: u64,
) -> Result<(usize, BTreeMap<String, BTreeSet<PathBuf>>), String> {
    let matcher = compile_excludes(excludes)?;
    let root_device = device_id(
        &fs::symlink_metadata(root).map_err(|error| format!("{}: {error}", root.display()))?,
    );
    let mut map = BTreeMap::new();
    let mut documents = Vec::new();
    let context = ScanContext {
        root,
        root_device,
        excludes: &matcher,
        max_file_bytes,
        build_candidates: true,
        stt: None,
        ocr: None,
        persistent: None,
        metadata_only: None,
    };
    let count = walk(&root.to_path_buf(), &context, &mut map, &mut documents)?;
    Ok((count, map))
}

fn scan_directory_unit(
    unit: &Path,
    root: &Path,
    excludes: &GlobSet,
    max_file_bytes: u64,
    build_candidates: bool,
    stt: Option<&SttEngine>,
    ocr: Option<&OcrEngine>,
    persistent: Option<&PersistentIndex>,
    metadata_only: Option<&GlobSet>,
) -> Result<DirectoryScan, String> {
    let root_device = device_id(
        &fs::symlink_metadata(root).map_err(|error| format!("{}: {error}", root.display()))?,
    );
    let mut map = BTreeMap::new();
    let metadata = match fs::symlink_metadata(unit) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::PermissionDenied => {
            return Ok((0, map, Vec::new(), Vec::new()))
        }
        Err(error) => return Err(format!("{}: {error}", unit.display())),
    };
    if device_id(&metadata).is_some_and(|device| Some(device) != root_device)
        || metadata.file_type().is_symlink()
        || excluded(unit, root, excludes)
    {
        return Ok((0, map, Vec::new(), Vec::new()));
    }
    if metadata.is_file() {
        let mut documents = Vec::new();
        let context = ScanContext {
            root,
            root_device,
            excludes,
            max_file_bytes,
            build_candidates,
            stt,
            ocr,
            persistent,
            metadata_only,
        };
        let count = walk(&unit.to_path_buf(), &context, &mut map, &mut documents)?;
        return Ok((count, map, documents, Vec::new()));
    }
    if !metadata.is_dir() {
        return Ok((0, map, Vec::new(), Vec::new()));
    }
    let children = match fs::read_dir(unit) {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .collect(),
        Err(error) if error.kind() == ErrorKind::PermissionDenied => Vec::new(),
        Err(error) => return Err(format!("{}: {error}", unit.display())),
    };
    Ok((0, map, Vec::new(), children))
}

fn root_units(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut units: Vec<_> = fs::read_dir(root)
        .map_err(|error| format!("{}: {error}", root.display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .collect();
    units.sort_by_key(|path| {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        (name != ".grepmesh-canary", path.clone())
    });
    Ok(units)
}

fn ordered_roots(roots: &BTreeMap<String, Vec<PathBuf>>) -> Vec<PathBuf> {
    let mut ordered = Vec::new();
    let mut seen = BTreeSet::new();
    for name in ["home", "opt", "etc", "local"] {
        if let Some(paths) = roots.get(name) {
            for path in paths {
                if seen.insert(path.clone()) {
                    ordered.push(path.clone());
                }
            }
        }
    }
    for paths in roots.values() {
        for path in paths {
            if seen.insert(path.clone()) {
                ordered.push(path.clone());
            }
        }
    }
    ordered
}

fn walk(
    path: &PathBuf,
    context: &ScanContext<'_>,
    map: &mut BTreeMap<String, BTreeSet<PathBuf>>,
    documents: &mut Vec<IndexedDocument>,
) -> Result<usize, String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::PermissionDenied => return Ok(0),
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    if device_id(&metadata).is_some_and(|device| Some(device) != context.root_device) {
        return Ok(0);
    }
    if metadata.file_type().is_symlink() {
        return Ok(0);
    }
    if excluded(path, context.root, context.excludes) {
        return Ok(0);
    }
    if metadata.is_file() {
        let size = metadata.len();
        let mtime_ns = metadata_mtime_ns(&metadata);
        if context
            .metadata_only
            .is_some_and(|matcher| excluded(path, context.root, matcher))
        {
            documents.push(IndexedDocument {
                path: path.clone(),
                body: format!(
                    "{}\npath: {}\nsize: {}\nmodified_ns: {}",
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or_default(),
                    path.display(),
                    size,
                    mtime_ns
                ),
                size,
                mtime_ns,
            });
            return Ok(1);
        }
        if let Some(persistent) = context.persistent {
            if let Ok(Some(body)) = persistent.cached_body(path, size, mtime_ns) {
                if context.build_candidates {
                    for gram in trigrams(&body.to_ascii_lowercase()) {
                        map.entry(gram).or_default().insert(path.clone());
                    }
                }
                documents.push(IndexedDocument {
                    path: path.clone(),
                    body,
                    size,
                    mtime_ns,
                });
                return Ok(1);
            }
        }
        let text = if let Some(stt) = context.stt.filter(|stt| stt.is_media(path)) {
            if stt.max_media_bytes() != 0 && metadata.len() > stt.max_media_bytes() {
                return Ok(0);
            }
            match stt.transcribe(path) {
                Ok(text) if !text.trim().is_empty() => text,
                Ok(_) => return Ok(0),
                Err(error) => {
                    tracing::warn!(path = %path.display(), error = %error, "STT skipped media file");
                    return Ok(0);
                }
            }
        } else if let Some(ocr) = context.ocr.filter(|ocr| ocr.is_image(path)) {
            if ocr.max_image_bytes() != 0 && metadata.len() > ocr.max_image_bytes() {
                return Ok(0);
            }
            match ocr.extract_image(path) {
                Ok(text) if !text.trim().is_empty() => text,
                Ok(_) => return Ok(0),
                Err(error) => {
                    tracing::warn!(path = %path.display(), error = %error, "OCR skipped image file");
                    return Ok(0);
                }
            }
        } else {
            let pdf_ocr = context.ocr.filter(|ocr| ocr.is_pdf(path));
            let read_limit = pdf_ocr
                .map(OcrEngine::max_image_bytes)
                .unwrap_or(context.max_file_bytes);
            if read_limit != 0 && metadata.len() > read_limit {
                return Ok(0);
            }
            let bytes = match fs::read(path) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == ErrorKind::PermissionDenied => return Ok(0),
                Err(error) => return Err(format!("{}: {error}", path.display())),
            };
            let extracted = extract_index_text(path, bytes);
            if let Some(ocr) = context
                .ocr
                .filter(|ocr| ocr.is_pdf(path) && ocr.should_ocr_pdf(extracted.as_deref()))
            {
                match ocr.extract_pdf(path) {
                    Ok(text) if !text.trim().is_empty() => text,
                    Ok(_) => match extracted {
                        Some(text) => text,
                        None => return Ok(0),
                    },
                    Err(error) => {
                        tracing::warn!(path = %path.display(), error = %error, "OCR PDF fallback failed");
                        match extracted {
                            Some(text) => text,
                            None => return Ok(0),
                        }
                    }
                }
            } else {
                let Some(text) = extracted else {
                    return Ok(0);
                };
                text
            }
        };
        if context.build_candidates {
            for gram in trigrams(&text.to_ascii_lowercase()) {
                map.entry(gram).or_default().insert(path.clone());
            }
        }
        documents.push(IndexedDocument {
            path: path.clone(),
            body: text,
            size,
            mtime_ns,
        });
        return Ok(1);
    }
    if !metadata.is_dir() {
        return Ok(0);
    }
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::PermissionDenied => return Ok(0),
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    let mut count = 0;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if error.kind() == ErrorKind::PermissionDenied => continue,
            Err(error) => return Err(error.to_string()),
        };
        count += walk(&entry.path(), context, map, documents)?;
    }
    Ok(count)
}

fn metadata_mtime_ns(metadata: &fs::Metadata) -> u128 {
    metadata
        .modified()
        .unwrap_or(SystemTime::UNIX_EPOCH)
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn extract_index_text(path: &Path, bytes: Vec<u8>) -> Option<String> {
    if !bytes.contains(&0) {
        if let Ok(text) = String::from_utf8(bytes) {
            return Some(text);
        }
    }
    if anydoc::Format::from_path(path).is_none() {
        return None;
    }
    match anydoc::to_markdown(path) {
        Ok(markdown) => Some(markdown),
        Err(error) => {
            tracing::debug!(path = %path.display(), error = %error, "AnyDoc skipped document");
            None
        }
    }
}

fn fts_literal_query(query: &str) -> String {
    format!("\"{}\"", query.replace('\"', "\"\""))
}

fn compile_excludes(excludes: &[String]) -> Result<GlobSet, String> {
    let mut builder = GlobSetBuilder::new();
    for pattern in excludes {
        builder.add(
            Glob::new(pattern).map_err(|error| format!("invalid exclude {pattern}: {error}"))?,
        );
    }
    builder
        .build()
        .map_err(|error| format!("compile excludes: {error}"))
}

fn trigrams(value: &str) -> BTreeSet<String> {
    let bytes = value.as_bytes();
    (0..bytes.len().saturating_sub(2))
        .map(|i| String::from_utf8_lossy(&bytes[i..i + 3]).into_owned())
        .collect()
}

fn excluded(path: &Path, root: &Path, excludes: &GlobSet) -> bool {
    let relative = path.strip_prefix(root).unwrap_or(path);
    excludes.is_match(relative) || excludes.is_match(relative.join(".grepmesh-directory-probe"))
}

#[cfg(test)]
mod tests {
    #[test]
    fn database_size_includes_sidecars_without_opening_database() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.sqlite");
        std::fs::write(&path, [0u8; 11]).unwrap();
        std::fs::write(dir.path().join("index.sqlite-wal"), [0u8; 7]).unwrap();
        std::fs::write(dir.path().join("index.sqlite-shm"), [0u8; 3]).unwrap();
        let mut manager = super::IndexManager::disabled();
        manager.persistent = Some(super::PersistentIndex { path });
        assert_eq!(manager.database_bytes(), Some(21));
        std::fs::remove_file(dir.path().join("index.sqlite-wal")).unwrap();
        assert_eq!(manager.database_bytes(), Some(14));
        manager.snapshot.write().unwrap().current_path = Some("/stale".into());
        assert_eq!(manager.status().current_path, None);
    }
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn permission_denied_subtree_does_not_degrade_the_entire_index() {
        let root = tempfile::tempdir().unwrap();
        let readable = root.path().join("readable.txt");
        let denied = root.path().join("denied");
        fs::write(&readable, "INDEX_ACCESS_TOKEN\n").unwrap();
        fs::create_dir(&denied).unwrap();
        fs::write(denied.join("secret.txt"), "should-not-break-index\n").unwrap();
        let mut permissions = fs::metadata(&denied).unwrap().permissions();
        permissions.set_mode(0o000);
        fs::set_permissions(&denied, permissions).unwrap();

        let mut roots = BTreeMap::new();
        roots.insert("home".to_string(), vec![root.path().to_path_buf()]);
        let result = build_root_index(root.path(), &[], 0);

        let mut restore = fs::metadata(&denied).unwrap().permissions();
        restore.set_mode(0o755);
        fs::set_permissions(&denied, restore).unwrap();
        let (count, candidates) = result.unwrap();
        assert!(count >= 1);
        assert!(candidates
            .get("ind")
            .is_some_and(|paths| paths.contains(&readable)));
    }

    #[test]
    fn special_files_do_not_break_the_entire_index() {
        let root = tempfile::tempdir().unwrap();
        let readable = root.path().join("readable.txt");
        let fifo = root.path().join("console");
        fs::write(&readable, "INDEX_SPECIAL_FILE_TOKEN\n").unwrap();
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(status.success());

        let mut roots = BTreeMap::new();
        roots.insert("home".to_string(), vec![root.path().to_path_buf()]);
        let (count, candidates) = build_root_index(root.path(), &[], 0).unwrap();

        assert_eq!(count, 1);
        assert!(candidates
            .get("ind")
            .is_some_and(|paths| paths.contains(&readable)));
    }

    #[test]
    fn roots_prioritize_home_before_other_named_roots() {
        let mut roots = BTreeMap::new();
        roots.insert("etc".to_string(), vec![PathBuf::from("/etc")]);
        roots.insert("home".to_string(), vec![PathBuf::from("/home/user")]);
        roots.insert("opt".to_string(), vec![PathBuf::from("/opt")]);
        assert_eq!(
            ordered_roots(&roots),
            vec![
                PathBuf::from("/home/user"),
                PathBuf::from("/opt"),
                PathBuf::from("/etc"),
            ]
        );
    }

    #[test]
    fn directory_exclusion_prunes_the_directory_itself() {
        let root = PathBuf::from("/workspace");
        let matcher = compile_excludes(&["**/.cache/**".to_string()]).unwrap();
        assert!(excluded(&root.join(".cache"), &root, &matcher));
    }

    #[test]
    fn anydoc_converts_rtf_for_indexing() {
        let dir = tempfile::tempdir().unwrap();
        let document = dir.path().join("note.rtf");
        let bytes = br"{\rtf1\ansi GrepMesh AnyDoc document canary}".to_vec();
        fs::write(&document, &bytes).unwrap();
        let markdown = extract_index_text(&document, bytes).expect("RTF should be converted");
        assert!(markdown.contains("GrepMesh AnyDoc document canary"));
    }

    #[test]
    fn persistent_index_searches_document_path_as_well_as_body() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.sqlite");
        let document = dir.path().join("quarterly-roadmap.docx");
        let index = PersistentIndex::open(db).unwrap();
        index
            .replace_document(&document, "body without the filename token")
            .unwrap();
        let matches = index.matching_documents("roadmap").unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].0, document);
    }

    #[test]
    fn extraction_cache_reuses_only_unchanged_file_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.sqlite");
        let document = dir.path().join("cached.txt");
        fs::write(&document, "cached extraction body").unwrap();
        let metadata = fs::metadata(&document).unwrap();
        let size = metadata.len();
        let mtime_ns = metadata_mtime_ns(&metadata);
        let index = PersistentIndex::open(db).unwrap();
        index
            .replace_document(&document, "cached extraction body")
            .unwrap();
        assert_eq!(
            index
                .cached_body(&document, size, mtime_ns)
                .unwrap()
                .as_deref(),
            Some("cached extraction body")
        );
        assert!(index
            .cached_body(&document, size + 1, mtime_ns)
            .unwrap()
            .is_none());
        assert!(index
            .cached_body(&document, size, mtime_ns + 1)
            .unwrap()
            .is_none());
    }

    #[test]
    fn persistent_rebuild_is_read_only_when_unchanged_and_tracks_changes() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("files");
        fs::create_dir(&root).unwrap();
        let document = root.join("document.txt");
        fs::write(&document, "original searchable token").unwrap();
        let index = PersistentIndex::open(dir.path().join("index.sqlite")).unwrap();
        let observer = index.connection().unwrap();
        let version = || {
            observer
                .query_row("PRAGMA data_version", [], |row| row.get::<_, i64>(0))
                .unwrap()
        };
        let roots = BTreeMap::from([("test".to_string(), vec![root])]);
        let snapshot = Arc::new(RwLock::new(IndexSnapshot::default()));
        let candidates = Arc::new(RwLock::new(BTreeMap::new()));
        let ready_roots = Arc::new(RwLock::new(BTreeSet::new()));
        let mut generation = 0;
        let mut rebuild = || {
            rebuild_index(
                &roots,
                &[],
                0,
                None,
                None,
                &IndexActivityConfig::default(),
                RebuildState {
                    snapshot: &snapshot,
                    candidates: &candidates,
                    ready_roots: &ready_roots,
                    persistent: Some(&index),
                },
                &mut generation,
            );
            assert!(snapshot.read().unwrap().last_error.is_none());
        };
        rebuild();
        assert_eq!(
            index.candidates("original").unwrap(),
            vec![document.clone()]
        );
        let before = version();
        rebuild();
        assert_eq!(
            version(),
            before,
            "cached rebuild must not commit SQLite writes"
        );
        fs::write(&document, "replacement searchable content").unwrap();
        rebuild();
        assert!(index.candidates("original").unwrap().is_empty());
        assert_eq!(
            index.candidates("replacement").unwrap(),
            vec![document.clone()]
        );
        fs::remove_file(&document).unwrap();
        rebuild();
        assert!(index.candidates("replacement").unwrap().is_empty());
        let count: i64 = observer
            .query_row(
                "SELECT count(*) FROM grepmesh_extraction_cache",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn unchanged_document_repairs_missing_stale_or_duplicate_fts_and_cache() {
        let dir = tempfile::tempdir().unwrap();
        let index = PersistentIndex::open(dir.path().join("index.sqlite")).unwrap();
        let document = dir.path().join("document.txt");
        fs::write(&document, "searchable repair token").unwrap();
        index
            .replace_document(&document, "searchable repair token")
            .unwrap();
        let connection = index.connection().unwrap();
        for corruption in [
            "DELETE FROM grepmesh_documents",
            "UPDATE grepmesh_documents SET body='stale content'",
            "INSERT INTO grepmesh_documents(path, body) SELECT path, body FROM grepmesh_documents",
            "DELETE FROM grepmesh_extraction_cache",
        ] {
            connection.execute(corruption, []).unwrap();
            index
                .replace_document(&document, "searchable repair token")
                .unwrap();
            assert_eq!(index.candidates("repair").unwrap(), vec![document.clone()]);
            assert!(index.candidates("stale").unwrap().is_empty());
            let metadata = fs::metadata(&document).unwrap();
            assert_eq!(
                index
                    .cached_body(&document, metadata.len(), metadata_mtime_ns(&metadata))
                    .unwrap()
                    .as_deref(),
                Some("searchable repair token")
            );
        }
    }

    #[test]
    fn persistent_index_replaces_documents_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.sqlite");
        let document = dir.path().join("producer.rs");
        let index = PersistentIndex::open(db.clone()).unwrap();
        index
            .replace_document(&document, "pub struct HealthProducer")
            .unwrap();
        assert_eq!(
            index.candidates("HealthProducer").unwrap(),
            vec![document.clone()]
        );
        index
            .replace_document(&document, "replacement body")
            .unwrap();
        assert!(index.candidates("HealthProducer").unwrap().is_empty());
        assert_eq!(
            PersistentIndex::open(db)
                .unwrap()
                .candidates("replacement")
                .unwrap(),
            vec![document]
        );
    }
}
