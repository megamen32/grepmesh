//! Source-to-index-host replication. Only extracted searchable text and source
//! metadata cross the existing mesh API; original files stay at their source.

use crate::{
    backend::{MatchLine, SearchHit, SearchMode},
    config::{RemoteIndexSourceConfig, RemoteIndexStoreConfig},
};
use anyhow::{anyhow, Context, Result};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteDocument {
    pub source_host: String,
    pub root_name: String,
    pub path: PathBuf,
    pub size: u64,
    pub mtime_ns: u128,
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
        let db = store.connection()?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS grepmesh_remote_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS grepmesh_remote_rows(
               source_host TEXT NOT NULL,root_name TEXT NOT NULL,path TEXT NOT NULL,
               size INTEGER NOT NULL,mtime_ns TEXT NOT NULL,body_sha256 TEXT NOT NULL,
               fts_rowid INTEGER NOT NULL,updated_ms INTEGER NOT NULL,
               PRIMARY KEY(source_host,root_name,path));
             CREATE INDEX IF NOT EXISTS grepmesh_remote_size ON grepmesh_remote_rows(size);
             CREATE VIRTUAL TABLE IF NOT EXISTS grepmesh_remote_documents USING fts5(
               source_host UNINDEXED,root_name UNINDEXED,path UNINDEXED,body,tokenize='trigram');
             INSERT OR IGNORE INTO grepmesh_remote_meta(key,value) VALUES('epoch',lower(hex(randomblob(16))));",
        )?;
        Ok(store)
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
            "SELECT EXISTS(SELECT 1 FROM grepmesh_remote_rows WHERE source_host=?1 AND root_name=?2 AND path=?3 AND size=?4 AND mtime_ns=?5 AND body_sha256=?6)",
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
        tx.execute("INSERT INTO grepmesh_remote_rows(source_host,root_name,path,size,mtime_ns,body_sha256,fts_rowid,updated_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(source_host,root_name,path) DO UPDATE SET size=excluded.size,mtime_ns=excluded.mtime_ns,body_sha256=excluded.body_sha256,fts_rowid=excluded.fts_rowid,updated_ms=excluded.updated_ms", params![document.source_host, document.root_name, path, document.size as i64, document.mtime_ns.to_string(), digest, tx.last_insert_rowid(), unix_time_ms() as i64])?;
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
        if query.chars().count() < 3 || limit == 0 || matches!(mode, SearchMode::Regex) {
            return Ok(Vec::new());
        }
        let db = self.connection()?;
        let fts_query = format!("\"{}\"", query.replace('"', "\"\""));
        let mut stmt = db.prepare("SELECT d.source_host,d.path,d.body,r.size,r.mtime_ns FROM grepmesh_remote_documents d JOIN grepmesh_remote_rows r ON r.fts_rowid=d.rowid WHERE grepmesh_remote_documents MATCH ?1 ORDER BY d.source_host,d.path LIMIT ?2")?;
        let mut rows = stmt.query(params![fts_query, limit as i64])?;
        let needle = if matches!(mode, SearchMode::CaseInsensitiveLiteral) {
            query.to_lowercase()
        } else {
            query.to_owned()
        };
        let mut hits = Vec::new();
        while let Some(row) = rows.next()? {
            let host: String = row.get(0)?;
            let path: String = row.get(1)?;
            let body: String = row.get(2)?;
            let size: u64 = row.get(3)?;
            let modified_ns: String = row.get(4)?;
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
                    lines.push(MatchLine {
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
            let mut stmt = source.prepare("SELECT path,size,mtime_ns,body FROM grepmesh_extraction_cache WHERE path>=?1 AND path<?2 ORDER BY path")?;
            let mut rows = stmt.query(params![lower, upper])?;
            let mut seen = BTreeSet::new();
            while let Some(row) = rows.next()? {
                let path_text: String = row.get(0)?;
                let size: u64 = row.get(1)?;
                let mtime_ns: String = row.get(2)?;
                let body: String = row.get(3)?;
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
            for path in old.into_iter().filter(|path| !seen.contains(path)) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

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
    fn remote_store_searches_original_path_and_delete_is_idempotent() {
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
            body: "find remote needle in this document".into(),
        };
        store.upsert(doc.clone()).unwrap();
        store.upsert(doc).unwrap();
        let hits = store
            .search("remote needle", 8, &SearchMode::Literal)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].host_id, "server-100");
        assert_eq!(hits[0].path, path.display().to_string());
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
                path,
            })
            .unwrap();
        assert!(store
            .search("remote needle", 8, &SearchMode::Literal)
            .unwrap()
            .is_empty());
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
                body: "candidate text".into(),
            });
            assert!(result.is_err());
        }
    }
}
