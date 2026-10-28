//! Source-to-index-host replication. Only extracted searchable text and source
//! metadata cross the existing mesh API; original files stay at their source.

use crate::{
    backend::{SearchHit, SearchMode},
    config::{RemoteIndexSourceConfig, RemoteIndexStoreConfig},
};
use anyhow::{anyhow, Context, Result};
use globset::{Glob, GlobSet, GlobSetBuilder};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const MAX_REMOTE_DOCUMENT_BYTES: usize = 1024 * 1024;
const SOURCE_DOCUMENTS_QUERY: &str = "SELECT cache.path,cache.size,cache.mtime_ns \
     FROM grepmesh_extraction_cache AS cache \
     JOIN grepmesh_document_rows AS rows ON rows.path=cache.path \
     JOIN grepmesh_documents AS documents ON documents.rowid=rows.fts_rowid \
     WHERE cache.path>=?1 AND cache.path<?2 ORDER BY cache.path";
const SOURCE_DOCUMENT_BODY_QUERY: &str =
    "SELECT CASE WHEN cache.body='' THEN documents.body ELSE cache.body END \
     FROM grepmesh_extraction_cache AS cache \
     JOIN grepmesh_document_rows AS rows ON rows.path=cache.path \
     JOIN grepmesh_documents AS documents ON documents.rowid=rows.fts_rowid \
     WHERE cache.path=?1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteDocument {
    pub source_host: String,
    pub root_name: String,
    pub path: PathBuf,
    pub size: u64,
    pub mtime_ns: u128,
    /// The source currently confirms that its service identity can read this
    /// exact regular file. Missing claims are denied for wire compatibility.
    #[serde(default)]
    pub source_read_authorized: bool,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteDelete {
    pub source_host: String,
    pub root_name: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteIndexStatus {
    pub index_host: String,
    pub epoch: String,
    pub document_count: u64,
    pub updated_ms: u64,
}

#[derive(Clone)]
pub struct RemoteIndexStore {
    config: Arc<RemoteIndexStoreConfig>,
}

impl RemoteIndexStore {
    pub fn open(config: RemoteIndexStoreConfig) -> Result<Self> {
        if !config.path.is_absolute() {
            return Err(anyhow!("remote index database path must be absolute"));
        }
        if config.allowed_source_hosts.is_empty() || config.allowed_roots.is_empty() {
            return Err(anyhow!(
                "remote index store requires allowed source hosts and roots"
            ));
        }
        if let Some(parent) = config.path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        let store = Self {
            config: Arc::new(config),
        };
        let mut db = store.connection()?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS grepmesh_remote_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS grepmesh_remote_rows(
               source_host TEXT NOT NULL,root_name TEXT NOT NULL,path TEXT NOT NULL,
               size INTEGER NOT NULL,mtime_ns TEXT NOT NULL,body_sha256 TEXT NOT NULL,
               fts_rowid INTEGER NOT NULL,updated_ms INTEGER NOT NULL,
               source_read_authorized INTEGER NOT NULL DEFAULT 0,
               PRIMARY KEY(source_host,root_name,path));
             CREATE INDEX IF NOT EXISTS grepmesh_remote_size ON grepmesh_remote_rows(size);
             CREATE VIRTUAL TABLE IF NOT EXISTS grepmesh_remote_documents USING fts5(
               source_host UNINDEXED,root_name UNINDEXED,path UNINDEXED,body,tokenize='trigram');
             INSERT OR IGNORE INTO grepmesh_remote_meta(key,value) VALUES('epoch',lower(hex(randomblob(16))));",
        )?;
        let has_source_read_auth = {
            let mut stmt = db.prepare("PRAGMA table_info(grepmesh_remote_rows)")?;
            let columns = stmt.query_map([], |row| row.get::<_, String>(1))?;
            columns
                .collect::<rusqlite::Result<Vec<_>>>()?
                .iter()
                .any(|name| name == "source_read_authorized")
        };
        if !has_source_read_auth {
            db.execute_batch("ALTER TABLE grepmesh_remote_rows ADD COLUMN source_read_authorized INTEGER NOT NULL DEFAULT 0;")?;
        }
        let auth_schema: Option<String> = db
            .query_row(
                "SELECT value FROM grepmesh_remote_meta WHERE key='source_read_authorization_v1'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if auth_schema.as_deref() != Some("1") {
            let tx = db.transaction()?;
            tx.execute("UPDATE grepmesh_remote_meta SET value=lower(hex(randomblob(16))) WHERE key='epoch'", [])?;
            tx.execute("INSERT INTO grepmesh_remote_meta(key,value) VALUES('source_read_authorization_v1','1') ON CONFLICT(key) DO UPDATE SET value=excluded.value", [])?;
            tx.commit()?;
        }
        Ok(store)
    }

    /// True when requested root paths or unique root names identify exactly
    /// the complete source-root set owned by this remote index. These paths
    /// are not present on the index host, so scanning unrelated local roots
    /// can consume the peer deadline before consulting this store.
    pub fn owns_requested_roots(&self, requested: &[String]) -> bool {
        if requested.is_empty() {
            return false;
        }
        let mut resolved = Vec::with_capacity(requested.len());
        for root in requested {
            let path = Path::new(root);
            if path.is_absolute() {
                resolved.push(path.to_path_buf());
                continue;
            }
            let matches = self
                .config
                .allowed_roots
                .values()
                .filter_map(|roots| roots.get(root))
                .collect::<Vec<_>>();
            if matches.len() != 1 {
                return false;
            }
            resolved.push(matches[0].clone());
        }
        resolved.sort();
        let mut configured = self
            .config
            .allowed_roots
            .values()
            .flat_map(|roots| roots.values().cloned())
            .collect::<Vec<_>>();
        configured.sort();
        resolved == configured
    }

    fn connection(&self) -> Result<Connection> {
        Connection::open(&self.config.path).context("open remote GrepMesh index")
    }

    fn validate_source_path(&self, source: &str, root_name: &str, path: &Path) -> Result<String> {
        if !self
            .config
            .allowed_source_hosts
            .iter()
            .any(|allowed| allowed == source)
        {
            return Err(anyhow!("source host is not authorized for this index"));
        }
        let root = self
            .config
            .allowed_roots
            .get(source)
            .and_then(|roots| roots.get(root_name))
            .ok_or_else(|| anyhow!("source root is not authorized for this index"))?;
        if !root.is_absolute() || !path.is_absolute() || !path.starts_with(root) || path == root {
            return Err(anyhow!("source path is outside its configured index root"));
        }
        Ok(path.to_string_lossy().into_owned())
    }

    pub fn upsert(&self, document: RemoteDocument) -> Result<()> {
        if !document.source_read_authorized {
            return Err(anyhow!(
                "source denied read authorization for this document"
            ));
        }
        let path =
            self.validate_source_path(&document.source_host, &document.root_name, &document.path)?;
        if document.body.len() > MAX_REMOTE_DOCUMENT_BYTES {
            return Err(anyhow!(
                "extracted document exceeds remote index size limit"
            ));
        }
        let digest = format!("{:x}", Sha256::digest(document.body.as_bytes()));
        let mut db = self.connection()?;
        let tx = db.transaction()?;
        let old_row: Option<i64> = tx.query_row(
            "SELECT fts_rowid FROM grepmesh_remote_rows WHERE source_host=?1 AND root_name=?2 AND path=?3",
            params![document.source_host, document.root_name, path], |row| row.get(0),
        ).optional()?;
        let same: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM grepmesh_remote_rows WHERE source_host=?1 AND root_name=?2 AND path=?3 AND size=?4 AND mtime_ns=?5 AND body_sha256=?6 AND source_read_authorized=1)",
            params![document.source_host, document.root_name, path, document.size as i64, document.mtime_ns.to_string(), digest], |row| row.get(0),
        )?;
        if same {
            return Ok(());
        }
        if let Some(rowid) = old_row {
            tx.execute(
                "DELETE FROM grepmesh_remote_documents WHERE rowid=?1",
                params![rowid],
            )?;
        }
        tx.execute("INSERT INTO grepmesh_remote_documents(source_host,root_name,path,body) VALUES(?1,?2,?3,?4)", params![document.source_host, document.root_name, path, document.body])?;
        tx.execute("INSERT INTO grepmesh_remote_rows(source_host,root_name,path,size,mtime_ns,body_sha256,fts_rowid,updated_ms,source_read_authorized) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,1) ON CONFLICT(source_host,root_name,path) DO UPDATE SET size=excluded.size,mtime_ns=excluded.mtime_ns,body_sha256=excluded.body_sha256,fts_rowid=excluded.fts_rowid,updated_ms=excluded.updated_ms,source_read_authorized=1", params![document.source_host, document.root_name, path, document.size as i64, document.mtime_ns.to_string(), digest, tx.last_insert_rowid(), unix_time_ms() as i64])?;
        tx.commit()?;
        Ok(())
    }

    pub fn delete(&self, deletion: RemoteDelete) -> Result<()> {
        let path =
            self.validate_source_path(&deletion.source_host, &deletion.root_name, &deletion.path)?;
        let mut db = self.connection()?;
        let tx = db.transaction()?;
        if let Some(rowid) = tx.query_row("SELECT fts_rowid FROM grepmesh_remote_rows WHERE source_host=?1 AND root_name=?2 AND path=?3", params![deletion.source_host, deletion.root_name, path], |row| row.get::<_, i64>(0)).optional()? {
            tx.execute("DELETE FROM grepmesh_remote_documents WHERE rowid=?1", params![rowid])?;
        }
        tx.execute(
            "DELETE FROM grepmesh_remote_rows WHERE source_host=?1 AND root_name=?2 AND path=?3",
            params![deletion.source_host, deletion.root_name, path],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn status(&self) -> Result<RemoteIndexStatus> {
        let db = self.connection()?;
        let epoch: String = db.query_row(
            "SELECT value FROM grepmesh_remote_meta WHERE key='epoch'",
            [],
            |row| row.get(0),
        )?;
        let (count, updated): (u64, u64) = db.query_row(
            "SELECT count(*),coalesce(max(updated_ms),0) FROM grepmesh_remote_rows",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok(RemoteIndexStatus {
            index_host: self.config.index_host.clone(),
            epoch,
            document_count: count,
            updated_ms: updated,
        })
    }

    pub fn search(&self, query: &str, limit: usize, mode: &SearchMode) -> Result<Vec<SearchHit>> {
        self.search_filtered(query, limit, mode, &[])
    }

    pub fn search_filtered(
        &self,
        _query: &str,
        _limit: usize,
        _mode: &SearchMode,
        _path_globs: &[String],
    ) -> Result<Vec<SearchHit>> {
        // `source_read_authorized` only proves that the GrepMesh service
        // account could open the source path during synchronization. MCP
        // currently authenticates a shared peer bearer and does not carry a
        // verified end-user principal or per-document ACL to this receiver.
        // Returning indexed bodies here would therefore widen the source
        // account's access to every peer caller. Fail closed until that
        // principal and its source ACL are propagated and checked end-to-end.
        Err(anyhow!(
            "remote indexed search denied: caller identity and per-document ACL are unavailable"
        ))
    }

    pub fn search_filtered_authorized(
        &self,
        query: &str,
        limit: usize,
        mode: &SearchMode,
        path_globs: &[String],
        read_roots: &[String],
    ) -> Result<Vec<SearchHit>> {
        if read_roots.is_empty()
            || read_roots
                .iter()
                .any(|root| !Path::new(root).is_absolute())
        {
            return Err(anyhow!("remote indexed search denied: verified read_roots required"));
        }
        if query.chars().count() < 3 || limit == 0 || matches!(mode, SearchMode::Regex) {
            return Ok(Vec::new());
        }
        let positive = path_globs
            .iter()
            .filter(|glob| !glob.starts_with('!'))
            .cloned()
            .collect::<Vec<_>>();
        let negative = path_globs
            .iter()
            .filter_map(|glob| glob.strip_prefix('!').map(str::to_owned))
            .collect::<Vec<_>>();
        let includes = compile_path_globs(&positive)?;
        let rejects = compile_path_globs(&negative)?;
        let db = self.connection()?;
        let fts_query = format!("\"{}\"", query.replace('"', "\"\""));
        let mut stmt = db.prepare("SELECT d.rowid,d.source_host,d.root_name,d.path,r.size,r.mtime_ns FROM grepmesh_remote_documents d JOIN grepmesh_remote_rows r ON r.fts_rowid=d.rowid WHERE grepmesh_remote_documents MATCH ?1 ORDER BY d.source_host,d.path")?;
        let mut rows = stmt.query(params![fts_query])?;
        let mut body_stmt = db.prepare("SELECT body FROM grepmesh_remote_documents WHERE rowid=?1")?;
        let needle = if matches!(mode, SearchMode::CaseInsensitiveLiteral) {
            query.to_lowercase()
        } else {
            query.to_owned()
        };
        let roots = read_roots.iter().map(Path::new).collect::<Vec<_>>();
        let mut hits = Vec::new();
        while let Some(row) = rows.next()? {
            let rowid: i64 = row.get(0)?;
            let host: String = row.get(1)?;
            let root_name: String = row.get(2)?;
            let path: String = row.get(3)?;
            let size: u64 = row.get(4)?;
            let modified_ns: String = row.get(5)?;
            let source_root = self
                .config
                .allowed_roots
                .get(&host)
                .and_then(|authorized| authorized.get(&root_name))
                .ok_or_else(|| anyhow!("remote index row has no authorized root"))?;
            let source_path = Path::new(&path);
            if !source_path.starts_with(source_root)
                || !roots.iter().any(|allowed| source_path.starts_with(allowed))
            {
                continue;
            }
            let relative = source_path.strip_prefix(source_root).unwrap_or(source_path);
            let matches = |set: &GlobSet| set.is_match(source_path) || set.is_match(relative);
            if (!positive.is_empty() && !matches(&includes)) || matches(&rejects) {
                continue;
            }
            // The ACL/path decision is complete before opening an FTS body.
            let body: String = body_stmt.query_row(params![rowid], |row| row.get(0))?;
            let modified_ms = (modified_ns.parse::<u128>().unwrap_or_default() / 1_000_000)
                .min(u64::MAX as u128) as u64;
            let mut lines = Vec::new();
            for (index, line) in body.lines().enumerate() {
                let haystack = if matches!(mode, SearchMode::CaseInsensitiveLiteral) {
                    line.to_lowercase()
                } else {
                    line.to_owned()
                };
                if let Some(column) = haystack.find(&needle) {
                    lines.push(crate::backend::MatchLine {
                        line_number: index + 1,
                        text: line.chars().take(1200).collect(),
                    });
                    hits.push(SearchHit {
                        host_id: host.clone(),
                        path: path.clone(),
                        line_number: index + 1,
                        text: line.chars().take(1200).collect(),
                        column: column + 1,
                        size: Some(size),
                        modified_ms: Some(modified_ms),
                        context: Vec::new(),
                        ..Default::default()
                    });
                    if hits.len() >= limit {
                        return Ok(hits);
                    }
                }
            }
            if let Some(last) = hits
                .last_mut()
                .filter(|hit| hit.host_id == host && hit.path == path)
            {
                last.context = lines;
            }
        }
        Ok(hits)
    }

    pub fn path_metadata(&self, source_host: &str, path: &Path) -> Result<Option<(u64, u64)>> {
        let db = self.connection()?;
        db.query_row("SELECT size,updated_ms FROM grepmesh_remote_rows WHERE source_host=?1 AND path=?2 LIMIT 1", params![source_host, path.to_string_lossy()], |row| Ok((row.get(0)?,row.get(1)?))).optional().map_err(Into::into)
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn compile_path_globs(patterns: &[String]) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern).map_err(|error| anyhow!("invalid path glob: {error}"))?);
    }
    builder
        .build()
        .map_err(|error| anyhow!("invalid path glob set: {error}"))
}

/// Polls only the explicitly selected source roots. The source index's path
/// primary key bounds each query; a separate tiny SQLite ledger records what
/// the remote accepted so outages and process restarts converge without replay
/// of unchanged extracted text.
pub fn spawn_source_sync(
    config: RemoteIndexSourceConfig,
    index_path: PathBuf,
    token: Option<String>,
) {
    tokio::spawn(async move {
        let state_path = config
            .state_path
            .clone()
            .unwrap_or_else(|| index_path.with_extension("remote-sync.sqlite3"));
        let mut ticker = tokio::time::interval(Duration::from_millis(
            config.sync_interval_ms.clamp(5_000, 300_000),
        ));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let cfg = config.clone();
            let state_path = state_path.clone();
            let index_path = index_path.clone();
            let token = token.clone();
            if let Err(error) = tokio::task::spawn_blocking(move || {
                let client = reqwest::blocking::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .build()?;
                sync_once(&cfg, &index_path, &state_path, &client, token.as_deref())
            })
            .await
            .unwrap_or_else(|e| Err(anyhow!(e)))
            {
                tracing::warn!(source_host=%config.source_host,index_host=%config.index_host,error=%error,"remote index sync deferred; source remains searchable locally");
            }
        }
    });
}

fn paths_missing_from_root(
    root: &Path,
    old_paths: impl IntoIterator<Item = String>,
    seen: &BTreeSet<String>,
) -> Vec<String> {
    old_paths
        .into_iter()
        .filter(|path| Path::new(path).starts_with(root) && !seen.contains(path))
        .collect()
}

fn sync_once(
    config: &RemoteIndexSourceConfig,
    index_path: &Path,
    state_path: &Path,
    client: &reqwest::blocking::Client,
    token: Option<&str>,
) -> Result<()> {
    let source = Connection::open_with_flags(
        index_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .context("open source extraction cache read-only")?;
    source.busy_timeout(Duration::from_secs(2))?;
    let state = Connection::open(state_path).context("open remote sync ledger")?;
    state.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS sync_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS sync_rows(root_name TEXT NOT NULL,path TEXT NOT NULL,size INTEGER NOT NULL,mtime_ns TEXT NOT NULL,body_sha256 TEXT NOT NULL,PRIMARY KEY(root_name,path));")?;
    let status_url = format!("{}/index/v1/status", config.endpoint.trim_end_matches('/'));
    let mut status_request = client.get(status_url);
    if let Some(token) = token {
        status_request = status_request.bearer_auth(token);
    }
    let remote: RemoteIndexStatus = status_request.send()?.error_for_status()?.json()?;
    if remote.index_host != config.index_host {
        return Err(anyhow!("remote index host identity mismatch"));
    }
    let current_epoch: Option<String> = state
        .query_row("SELECT value FROM sync_meta WHERE key='epoch'", [], |r| {
            r.get(0)
        })
        .optional()?;
    if current_epoch.as_deref() != Some(remote.epoch.as_str()) {
        state.execute("DELETE FROM sync_rows", [])?;
        state.execute("INSERT INTO sync_meta(key,value) VALUES('epoch',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value", params![remote.epoch])?;
    }
    for (root_name, roots) in &config.roots {
        for root in roots {
            let root = fs::canonicalize(root)
                .with_context(|| format!("resolve selected source root {}", root.display()))?;
            let lower = format!("{}/", root.display().to_string().trim_end_matches('/'));
            let upper = format!("{}\u{10ffff}", lower);
            let mut stmt = source.prepare(SOURCE_DOCUMENTS_QUERY)?;
            let mut rows = stmt.query(params![lower, upper])?;
            let mut seen = BTreeSet::new();
            while let Some(row) = rows.next()? {
                let path_text: String = row.get(0)?;
                let size: u64 = row.get(1)?;
                let mtime_ns: String = row.get(2)?;
                if !source_file_read_authorized(&root, Path::new(&path_text))? {
                    let deletion = RemoteDelete {
                        source_host: config.source_host.clone(),
                        root_name: root_name.clone(),
                        path: PathBuf::from(&path_text),
                    };
                    let url = format!("{}/index/v1/delete", config.endpoint.trim_end_matches('/'));
                    let mut request = client.post(url).json(&deletion);
                    if let Some(token) = token {
                        request = request.bearer_auth(token);
                    }
                    request.send()?.error_for_status()?;
                    state.execute(
                        "DELETE FROM sync_rows WHERE root_name=?1 AND path=?2",
                        params![root_name, path_text],
                    )?;
                    continue;
                }
                let body: String =
                    source.query_row(SOURCE_DOCUMENT_BODY_QUERY, [&path_text], |row| row.get(0))?;
                if body.len() > MAX_REMOTE_DOCUMENT_BYTES {
                    continue;
                }
                let digest = format!("{:x}", Sha256::digest(body.as_bytes()));
                seen.insert(path_text.clone());
                let unchanged: bool = state.query_row("SELECT EXISTS(SELECT 1 FROM sync_rows WHERE root_name=?1 AND path=?2 AND size=?3 AND mtime_ns=?4 AND body_sha256=?5)", params![root_name,path_text,size as i64,mtime_ns,digest], |r| r.get(0))?;
                if unchanged {
                    continue;
                }
                let document = RemoteDocument {
                    source_host: config.source_host.clone(),
                    root_name: root_name.clone(),
                    path: PathBuf::from(&path_text),
                    size,
                    mtime_ns: mtime_ns.parse().unwrap_or(0),
                    source_read_authorized: true,
                    body,
                };
                let url = format!("{}/index/v1/upsert", config.endpoint.trim_end_matches('/'));
                let mut request = client.post(url).json(&document);
                if let Some(token) = token {
                    request = request.bearer_auth(token);
                }
                request.send()?.error_for_status()?;
                state.execute("INSERT INTO sync_rows(root_name,path,size,mtime_ns,body_sha256) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(root_name,path) DO UPDATE SET size=excluded.size,mtime_ns=excluded.mtime_ns,body_sha256=excluded.body_sha256", params![root_name,path_text,size as i64,mtime_ns,digest])?;
            }
            let old: Vec<String> = {
                let mut s = state.prepare("SELECT path FROM sync_rows WHERE root_name=?1")?;
                let rows = s.query_map(params![root_name], |r| r.get(0))?;
                rows.collect::<rusqlite::Result<_>>()?
            };
            for path in paths_missing_from_root(&root, old, &seen) {
                let deletion = RemoteDelete {
                    source_host: config.source_host.clone(),
                    root_name: root_name.clone(),
                    path: PathBuf::from(&path),
                };
                let url = format!("{}/index/v1/delete", config.endpoint.trim_end_matches('/'));
                let mut request = client.post(url).json(&deletion);
                if let Some(token) = token {
                    request = request.bearer_auth(token);
                }
                request.send()?.error_for_status()?;
                state.execute(
                    "DELETE FROM sync_rows WHERE root_name=?1 AND path=?2",
                    params![root_name, path],
                )?;
            }
        }
    }
    Ok(())
}

/// Verify source-side authorization without reading or logging document text.
/// A document is eligible only while the service can open its regular-file
/// target and the resolved target remains inside the configured source root.
fn source_file_read_authorized(root: &Path, path: &Path) -> Result<bool> {
    let canonical = match fs::canonicalize(path) {
        Ok(path) => path,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            return Ok(false);
        }
        Err(_) => return Err(anyhow!("cannot validate source document authorization")),
    };
    if !canonical.starts_with(root) {
        return Ok(false);
    }
    let metadata = match fs::metadata(&canonical) {
        Ok(metadata) => metadata,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            return Ok(false);
        }
        Err(_) => return Err(anyhow!("cannot validate source document authorization")),
    };
    if !metadata.is_file() {
        return Ok(false);
    }
    match fs::File::open(canonical) {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            Ok(false)
        }
        Err(_) => Err(anyhow!("cannot validate source document authorization")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::State,
        http::{HeaderMap, StatusCode},
        response::{IntoResponse, Response},
        routing::{get, post},
        Json, Router,
    };
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    #[derive(Clone)]
    struct TestPeerState {
        store: RemoteIndexStore,
        token: String,
    }

    fn peer_request_is_authorized(headers: &HeaderMap, token: &str) -> bool {
        headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            == Some(format!("Bearer {token}").as_str())
    }

    async fn test_peer_status(State(state): State<TestPeerState>, headers: HeaderMap) -> Response {
        if !peer_request_is_authorized(&headers, &state.token) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        match state.store.status() {
            Ok(status) => Json(status).into_response(),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }

    async fn test_peer_upsert(
        State(state): State<TestPeerState>,
        headers: HeaderMap,
        Json(document): Json<RemoteDocument>,
    ) -> Response {
        if !peer_request_is_authorized(&headers, &state.token) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        match state.store.upsert(document) {
            Ok(()) => StatusCode::OK.into_response(),
            Err(_) => StatusCode::BAD_REQUEST.into_response(),
        }
    }

    async fn test_peer_delete(
        State(state): State<TestPeerState>,
        headers: HeaderMap,
        Json(deletion): Json<RemoteDelete>,
    ) -> Response {
        if !peer_request_is_authorized(&headers, &state.token) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        match state.store.delete(deletion) {
            Ok(()) => StatusCode::OK.into_response(),
            Err(_) => StatusCode::BAD_REQUEST.into_response(),
        }
    }

    fn config(root: &Path, db: &Path) -> RemoteIndexStoreConfig {
        RemoteIndexStoreConfig {
            index_host: "server-44".into(),
            path: db.to_path_buf(),
            allowed_source_hosts: vec!["server-100".into()],
            allowed_roots: BTreeMap::from([(
                "server-100".into(),
                BTreeMap::from([("docs".into(), root.to_path_buf())]),
            )]),
        }
    }

    #[test]
    fn remote_store_does_not_treat_service_read_grant_as_caller_acl() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("docs");
        fs::create_dir(&root).unwrap();
        let store =
            RemoteIndexStore::open(config(&root, &tmp.path().join("remote.sqlite3"))).unwrap();
        let path = root.join("note.md");
        let doc = RemoteDocument {
            source_host: "server-100".into(),
            root_name: "docs".into(),
            path: path.clone(),
            size: 12,
            mtime_ns: 1,
            source_read_authorized: true,
            body: "find remote needle in this document".into(),
        };
        store.upsert(doc.clone()).unwrap();
        store.upsert(doc).unwrap();
        assert_eq!(
            store.path_metadata("server-100", &path).unwrap().unwrap().0,
            12
        );
        let denied = store
            .search("remote needle", 8, &SearchMode::Literal)
            .unwrap_err();
        assert!(denied.to_string().contains("caller identity"));
        assert!(!denied.to_string().contains("find remote needle"));
        let allowed = store
            .search_filtered_authorized(
                "remote needle",
                8,
                &SearchMode::Literal,
                &[],
                &[root.display().to_string()],
            )
            .unwrap();
        assert_eq!(allowed.len(), 1);
        assert_eq!(allowed[0].path, path.display().to_string());
        assert!(allowed[0].text.contains("remote needle"));
        let denied_other_principal = store
            .search_filtered_authorized(
                "remote needle",
                8,
                &SearchMode::Literal,
                &[],
                &[tmp.path().join("other-user").display().to_string()],
            )
            .unwrap();
        assert!(denied_other_principal.is_empty());
        store
            .delete(RemoteDelete {
                source_host: "server-100".into(),
                root_name: "docs".into(),
                path: path.clone(),
            })
            .unwrap();
        store
            .delete(RemoteDelete {
                source_host: "server-100".into(),
                root_name: "docs".into(),
                path: path.clone(),
            })
            .unwrap();
        assert_eq!(store.path_metadata("server-100", &path).unwrap(), None);
    }

    #[test]
    fn remote_store_recognizes_only_the_complete_authorized_root_set() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("docs");
        fs::create_dir(&root).unwrap();
        let store =
            RemoteIndexStore::open(config(&root, &tmp.path().join("remote.sqlite3"))).unwrap();

        assert!(store.owns_requested_roots(&[root.display().to_string()]));
        assert!(store.owns_requested_roots(&["docs".into()]));
        assert!(!store.owns_requested_roots(&["docs".into(), "docs".into()]));
        assert!(!store.owns_requested_roots(&[tmp.path().display().to_string()]));
        assert!(!store.owns_requested_roots(&[tmp.path().join("other").display().to_string()]));
        assert!(!store.owns_requested_roots(&[]));

        let mut multiple_roots = config(&root, &tmp.path().join("multiple.sqlite3"));
        multiple_roots
            .allowed_roots
            .get_mut("server-100")
            .unwrap()
            .insert("other".into(), tmp.path().join("other"));
        let multiple_store = RemoteIndexStore::open(multiple_roots).unwrap();
        assert!(!multiple_store.owns_requested_roots(&[root.display().to_string()]));
        assert!(!multiple_store.owns_requested_roots(&["docs".into()]));
    }

    #[test]
    fn remote_store_rejects_other_hosts_and_paths_outside_selected_root() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("docs");
        fs::create_dir(&root).unwrap();
        let store =
            RemoteIndexStore::open(config(&root, &tmp.path().join("remote.sqlite3"))).unwrap();
        for (host, path) in [
            ("server-88", root.join("a.md")),
            ("server-100", tmp.path().join("outside.md")),
        ] {
            let result = store.upsert(RemoteDocument {
                source_host: host.into(),
                root_name: "docs".into(),
                path,
                size: 1,
                mtime_ns: 1,
                source_read_authorized: true,
                body: "candidate text".into(),
            });
            assert!(result.is_err());
        }
    }

    #[test]
    fn remote_store_denies_documents_without_source_read_authorization() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("docs");
        fs::create_dir(&root).unwrap();
        let store =
            RemoteIndexStore::open(config(&root, &tmp.path().join("remote.sqlite3"))).unwrap();
        let path = root.join("private.md");
        let payload = serde_json::json!({
            "source_host":"server-100", "root_name":"docs",
            "path":path, "size":20, "mtime_ns":1,
            "body":"never expose this fixture"
        });
        let denied: RemoteDocument = serde_json::from_value(payload).unwrap();
        assert!(!denied.source_read_authorized);
        let denied = store.upsert(denied);
        assert!(denied.is_err());
        let denied = store
            .search("expose this", 8, &SearchMode::Literal)
            .unwrap_err();
        assert!(denied.to_string().contains("caller identity"));
    }

    #[test]
    fn legacy_remote_rows_migrate_to_deny_until_the_source_reauthorizes_them() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("docs");
        fs::create_dir(&root).unwrap();
        let db_path = tmp.path().join("remote.sqlite3");
        let path = root.join("old.md");
        let db = Connection::open(&db_path).unwrap();
        db.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE grepmesh_remote_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
             INSERT INTO grepmesh_remote_meta(key,value) VALUES('epoch','legacy-epoch');
             CREATE TABLE grepmesh_remote_rows(
               source_host TEXT NOT NULL,root_name TEXT NOT NULL,path TEXT NOT NULL,
               size INTEGER NOT NULL,mtime_ns TEXT NOT NULL,body_sha256 TEXT NOT NULL,
               fts_rowid INTEGER NOT NULL,updated_ms INTEGER NOT NULL,
               PRIMARY KEY(source_host,root_name,path));
             CREATE VIRTUAL TABLE grepmesh_remote_documents USING fts5(
               source_host UNINDEXED,root_name UNINDEXED,path UNINDEXED,body,tokenize='trigram');",
        )
        .unwrap();
        db.execute(
            "INSERT INTO grepmesh_remote_documents(source_host,root_name,path,body) VALUES('server-100','docs',?1,'legacy authorization fixture')",
            params![path.to_string_lossy()],
        ).unwrap();
        let rowid = db.last_insert_rowid();
        db.execute(
            "INSERT INTO grepmesh_remote_rows(source_host,root_name,path,size,mtime_ns,body_sha256,fts_rowid,updated_ms) VALUES('server-100','docs',?1,20,'1','legacy',?2,1)",
            params![path.to_string_lossy(), rowid],
        ).unwrap();
        drop(db);

        let migrated = RemoteIndexStore::open(config(&root, &db_path)).unwrap();
        assert_ne!(migrated.status().unwrap().epoch, "legacy-epoch");
        assert!(migrated
            .search("authorization fixture", 8, &SearchMode::Literal)
            .unwrap_err()
            .to_string()
            .contains("caller identity"));
        migrated
            .upsert(RemoteDocument {
                source_host: "server-100".into(),
                root_name: "docs".into(),
                path: path.clone(),
                size: 20,
                mtime_ns: 1,
                source_read_authorized: true,
                body: "reauthorized fixture text".into(),
            })
            .unwrap();
        assert!(migrated
            .search("reauthorized fixture", 8, &SearchMode::Literal)
            .unwrap_err()
            .to_string()
            .contains("caller identity"));
    }

    #[test]
    fn source_read_authorization_requires_a_regular_target_inside_the_root() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("docs");
        fs::create_dir(&root).unwrap();
        let allowed = root.join("allowed.md");
        fs::write(&allowed, "fixture body is never read here").unwrap();
        let outside = tmp.path().join("outside.md");
        fs::write(&outside, "fixture body is never read here").unwrap();

        assert!(source_file_read_authorized(&root, &allowed).unwrap());
        assert!(!source_file_read_authorized(&root, &outside).unwrap());
        assert!(!source_file_read_authorized(&root, &root.join("missing.md")).unwrap());
    }

    #[tokio::test]
    async fn source_authorization_sync_sends_only_readable_documents_and_revokes_missing_source_paths(
    ) {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("docs");
        fs::create_dir(&root).unwrap();
        let allowed_path = root.join("allowed.md");
        fs::write(&allowed_path, "fixture source bytes").unwrap();
        let denied_path = root.join("missing.md");
        let source_db_path = tmp.path().join("source.sqlite3");
        let state_path = tmp.path().join("sync.sqlite3");
        let remote_db_path = tmp.path().join("remote.sqlite3");
        let remote = RemoteIndexStore::open(config(&root, &remote_db_path)).unwrap();

        let source = Connection::open(&source_db_path).unwrap();
        source
            .execute_batch(
                "CREATE VIRTUAL TABLE grepmesh_documents USING fts5(path UNINDEXED,body,tokenize='trigram');
                 CREATE TABLE grepmesh_extraction_cache(path TEXT PRIMARY KEY,size INTEGER,mtime_ns TEXT,body TEXT NOT NULL);
                 CREATE TABLE grepmesh_document_rows(path TEXT PRIMARY KEY,fts_rowid INTEGER UNIQUE);",
            )
            .unwrap();
        for (path, body) in [
            (&allowed_path, "authorized fixture marker"),
            (&denied_path, "secret denied fixture marker"),
        ] {
            source
                .execute(
                    "INSERT INTO grepmesh_documents(path,body) VALUES(?1,?2)",
                    params![path.to_string_lossy(), body],
                )
                .unwrap();
            let rowid = source.last_insert_rowid();
            source
                .execute(
                    "INSERT INTO grepmesh_document_rows(path,fts_rowid) VALUES(?1,?2)",
                    params![path.to_string_lossy(), rowid],
                )
                .unwrap();
            source
                .execute(
                    "INSERT INTO grepmesh_extraction_cache(path,size,mtime_ns,body) VALUES(?1,20,'7','')",
                    params![path.to_string_lossy()],
                )
                .unwrap();
        }
        drop(source);

        // Seed an already-synchronized document whose source has disappeared.
        remote
            .upsert(RemoteDocument {
                source_host: "server-100".into(),
                root_name: "docs".into(),
                path: denied_path.clone(),
                size: 20,
                mtime_ns: 7,
                source_read_authorized: true,
                body: "secret denied fixture marker".into(),
            })
            .unwrap();

        let state = TestPeerState {
            store: remote.clone(),
            token: "fixture-sync-token".into(),
        };
        let app = Router::new()
            .route("/index/v1/status", get(test_peer_status))
            .route("/index/v1/upsert", post(test_peer_upsert))
            .route("/index/v1/delete", post(test_peer_delete))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = stop_rx.await;
                })
                .await
                .unwrap();
        });
        let sync_config = RemoteIndexSourceConfig {
            source_host: "server-100".into(),
            index_host: "server-44".into(),
            endpoint,
            roots: BTreeMap::from([("docs".into(), vec![root])]),
            state_path: Some(state_path.clone()),
            sync_interval_ms: 30_000,
        };
        let index_path = source_db_path.clone();
        let token = "fixture-sync-token".to_string();
        tokio::task::spawn_blocking(move || {
            let client = reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap();
            sync_once(
                &sync_config,
                &index_path,
                &state_path,
                &client,
                Some(&token),
            )
        })
        .await
        .unwrap()
        .unwrap();
        let _ = stop_tx.send(());
        server.await.unwrap();

        assert_eq!(remote.status().unwrap().document_count, 1);
        assert!(remote
            .search("authorized fixture marker", 8, &SearchMode::Literal)
            .unwrap_err()
            .to_string()
            .contains("caller identity"));
        assert!(remote
            .search("secret denied fixture marker", 8, &SearchMode::Literal)
            .unwrap_err()
            .to_string()
            .contains("caller identity"));
    }

    #[test]
    fn lossless_body_copy_sync_deletes_only_missing_paths_from_the_current_root() {
        let tmp = tempdir().unwrap();
        let root_a = tmp.path().join("docs-a");
        let root_b = tmp.path().join("docs-b");
        fs::create_dir(&root_a).unwrap();
        fs::create_dir(&root_b).unwrap();
        let still_present = root_a.join("keep.md").to_string_lossy().into_owned();
        let missing = root_a.join("remove.md").to_string_lossy().into_owned();
        let sibling_root = root_b.join("keep-too.md").to_string_lossy().into_owned();
        let seen = BTreeSet::from([still_present.clone()]);

        let stale = paths_missing_from_root(
            &root_a,
            vec![still_present, missing.clone(), sibling_root.clone()],
            &seen,
        );

        assert_eq!(stale, vec![missing]);
        assert!(!stale.contains(&sibling_root));
    }

    #[test]
    fn lossless_body_copy_remote_sync_reads_cache_or_fts_without_losing_text() {
        let source = Connection::open_in_memory().unwrap();
        source
            .execute_batch(
                "CREATE VIRTUAL TABLE grepmesh_documents USING fts5(path UNINDEXED,body,tokenize='trigram');\
                 CREATE TABLE grepmesh_extraction_cache(path TEXT PRIMARY KEY,size INTEGER,mtime_ns TEXT,body TEXT NOT NULL);\
                 CREATE TABLE grepmesh_document_rows(path TEXT PRIMARY KEY,fts_rowid INTEGER UNIQUE);",
            )
            .unwrap();
        let path = "/source/docs/memory.md";
        source
            .execute(
                "INSERT INTO grepmesh_documents(path,body) VALUES(?1,?2)",
                params![path, "durable searchable memory"],
            )
            .unwrap();
        let rowid = source.last_insert_rowid();
        source
            .execute(
                "INSERT INTO grepmesh_document_rows(path,fts_rowid) VALUES(?1,?2)",
                params![path, rowid],
            )
            .unwrap();
        source
            .execute(
                "INSERT INTO grepmesh_extraction_cache(path,size,mtime_ns,body) VALUES(?1,24,'7','')",
                params![path],
            )
            .unwrap();
        let (path, size, mtime_ns): (String, u64, String) = source
            .query_row(
                SOURCE_DOCUMENTS_QUERY,
                params!["/source/docs/", "/source/docs/\u{10ffff}"],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            (path.as_str(), size, mtime_ns.as_str()),
            ("/source/docs/memory.md", 24, "7")
        );
        let read_body: String = source
            .query_row(SOURCE_DOCUMENT_BODY_QUERY, [&path], |row| row.get(0))
            .unwrap();
        assert_eq!(read_body, "durable searchable memory");

        source
            .execute(
                "UPDATE grepmesh_extraction_cache SET body='legacy cached text' WHERE path=?1",
                params![path],
            )
            .unwrap();
        let read_cached_body: String = source
            .query_row(SOURCE_DOCUMENT_BODY_QUERY, [&path], |row| row.get(0))
            .unwrap();
        assert_eq!(read_cached_body, "legacy cached text");
    }
}
