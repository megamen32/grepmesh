use crate::{
    backend::IndexState,
    config::{IndexActivityConfig, OcrConfig, SttConfig},
    ocr::OcrEngine,
    stt::SttEngine,
};
use globset::{Glob, GlobSet, GlobSetBuilder};
use notify::{
    event::{CreateKind, ModifyKind, RenameMode},
    EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
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
const MAX_PENDING_OCR: usize = 65_536;
const MAX_WATCH_RECOVERY_UNITS: usize = 65_536;

fn register_watch_root(
    root: &Path,
    excludes: &GlobSet,
    watch: &mut impl FnMut(&Path) -> notify::Result<()>,
) -> Vec<(PathBuf, notify::Error)> {
    register_watch_subtree(root, root, excludes, watch)
}

fn register_watch_subtree(
    start: &Path,
    root: &Path,
    excludes: &GlobSet,
    watch: &mut impl FnMut(&Path) -> notify::Result<()>,
) -> Vec<(PathBuf, notify::Error)> {
    let mut failures = Vec::new();
    let mut remaining = VecDeque::from([start.to_path_buf()]);
    let root_device = fs::symlink_metadata(root)
        .ok()
        .and_then(|meta| device_id(&meta));
    let mut attempted = 0;
    while let Some(unit) = remaining.pop_front() {
        let Ok(meta) = fs::symlink_metadata(&unit) else {
            continue;
        };
        if !meta.is_dir()
            || meta.file_type().is_symlink()
            || device_id(&meta) != root_device
            || excluded(&unit, root, excludes)
        {
            continue;
        }
        attempted += 1;
        if attempted > MAX_WATCH_RECOVERY_UNITS {
            failures.push((
                unit.clone(),
                notify::Error::generic("bounded watch recovery exhausted").add_path(unit),
            ));
            break;
        }
        if let Err(error) = watch(&unit) {
            // Register individual directories on the same watcher: recursive
            // notify6 traversal cannot apply our excludes and can exhaust the
            // user's watches inside node_modules or build output.
            let exhausted = matches!(error.kind, notify::ErrorKind::MaxFilesWatch);
            failures.push((unit.clone(), error));
            if exhausted {
                break;
            }
        }
        if let Ok(children) = fs::read_dir(&unit) {
            remaining.extend(children.filter_map(|entry| {
                let entry = entry.ok()?;
                entry.file_type().ok()?.is_dir().then(|| entry.path())
            }));
        }
    }
    failures
}

#[derive(Default)]
struct PendingIndexEvent {
    count: u64,
    new_subtree: bool,
}

fn new_subtree_event(kind: EventKind) -> bool {
    matches!(
        kind,
        EventKind::Create(CreateKind::Folder | CreateKind::Any)
            | EventKind::Modify(ModifyKind::Name(
                RenameMode::To | RenameMode::Both | RenameMode::Any
            ))
    )
}

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

#[derive(Debug)]
pub struct IndexedSearch {
    pub hits: Vec<IndexTextHit>,
    pub truncated: bool,
}

struct RebuildState<'a> {
    snapshot: &'a Arc<RwLock<IndexSnapshot>>,
    candidates: &'a Arc<RwLock<BTreeMap<String, BTreeSet<PathBuf>>>>,
    ready_roots: &'a Arc<RwLock<BTreeSet<PathBuf>>>,
    persistent: Option<&'a PersistentIndex>,
}

struct ScanContext<'a> {
    dispatch_mesh: bool,
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
        store.migrate_document_rows()?;
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
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS grepmesh_pending_ocr (path TEXT PRIMARY KEY);\
                 CREATE TABLE IF NOT EXISTS grepmesh_ocr_attempts (\
                   path TEXT PRIMARY KEY, size INTEGER NOT NULL, mtime_ns TEXT NOT NULL,\
                   failures INTEGER NOT NULL CHECK(failures BETWEEN 0 AND 4)\
                 );\
                 CREATE TABLE IF NOT EXISTS grepmesh_ocr_empty_results (\
                   path TEXT PRIMARY KEY, size INTEGER NOT NULL, mtime_ns TEXT NOT NULL,\
                   disposition TEXT NOT NULL CHECK(disposition IN ('empty_output','empty_input'))\
                 );\
                 CREATE TABLE IF NOT EXISTS grepmesh_document_rows (\
                   path TEXT PRIMARY KEY,\
                   fts_rowid INTEGER NOT NULL UNIQUE\
                 );\
                 CREATE TRIGGER IF NOT EXISTS grepmesh_cache_rows_after_insert \
                 AFTER INSERT ON grepmesh_extraction_cache BEGIN \
                   DELETE FROM grepmesh_document_rows WHERE path=NEW.path; \
                   DELETE FROM grepmesh_metadata WHERE key='document_rows_v1'; \
                 END;\
                 CREATE TRIGGER IF NOT EXISTS grepmesh_cache_rows_after_update \
                 AFTER UPDATE ON grepmesh_extraction_cache BEGIN \
                   DELETE FROM grepmesh_document_rows \
                   WHERE path=OLD.path OR path=NEW.path; \
                   DELETE FROM grepmesh_metadata WHERE key='document_rows_v1'; \
                 END;\
                 CREATE TRIGGER IF NOT EXISTS grepmesh_cache_rows_after_delete \
                 AFTER DELETE ON grepmesh_extraction_cache BEGIN \
                   DELETE FROM grepmesh_metadata WHERE key='document_rows_v1'; \
                 END;",
            )
            .map_err(|error| error.to_string())?;
        Ok(connection)
    }

    fn document_count(&self) -> Result<usize, String> {
        self.connection()?
            .query_row("SELECT count(*) FROM grepmesh_document_rows", [], |row| {
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
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO grepmesh_metadata(key, value) VALUES ('last_full_rebuild_ms', ?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![now_ms.to_string()],
            )
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "DELETE FROM grepmesh_metadata WHERE key='ocr_recovery_full_scan_required'",
                [],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())
    }

    pub(crate) fn ocr_recovery_full_scan_required(&self) -> Result<bool, String> {
        self.connection()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM grepmesh_metadata WHERE key='ocr_recovery_full_scan_required' AND value='1')", [], |row| row.get(0)
        ).map_err(|error| error.to_string())
    }

    pub(crate) fn load_pending_ocr(&self) -> Result<(Vec<PathBuf>, bool), String> {
        let connection = self.connection()?;
        let mut statement = connection
            .prepare("SELECT path FROM grepmesh_pending_ocr ORDER BY path LIMIT ?1")
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map(params![(MAX_PENDING_OCR + 1) as i64], |row| {
                row.get::<_, String>(0)
            })
            .map_err(|error| error.to_string())?;
        let mut paths = rows
            .map(|row| row.map(PathBuf::from))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        let overflow: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM grepmesh_metadata WHERE key='pending_ocr_overflow' AND value='1')", [], |row| row.get(0)
        ).map_err(|error| error.to_string())?;
        let overflow = overflow || paths.len() > MAX_PENDING_OCR;
        paths.truncate(MAX_PENDING_OCR);
        Ok((paths, overflow))
    }

    pub(crate) fn defer_ocr(&self, path: &Path) -> Result<(), String> {
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let path = path.display().to_string();
        let exists: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM grepmesh_pending_ocr WHERE path=?1)",
                params![path],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        if exists {
            return Ok(());
        }
        let count: usize = transaction
            .query_row("SELECT count(*) FROM grepmesh_pending_ocr", [], |row| {
                row.get(0)
            })
            .map_err(|error| error.to_string())?;
        if count >= MAX_PENDING_OCR {
            transaction.execute_batch("INSERT INTO grepmesh_metadata(key,value) VALUES ('pending_ocr_overflow','1'), ('ocr_recovery_full_scan_required','1') ON CONFLICT(key) DO UPDATE SET value='1';")
                .map_err(|error| error.to_string())?;
            transaction.commit().map_err(|error| error.to_string())?;
            return Err("durable OCR queue is full; reconciliation is required".into());
        }
        transaction
            .execute(
                "INSERT INTO grepmesh_pending_ocr(path) VALUES (?1)",
                params![path],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())
    }

    pub(crate) fn ack_pending_ocr(&self, path: &Path) -> Result<(), String> {
        self.connection()?
            .execute(
                "DELETE FROM grepmesh_pending_ocr WHERE path=?1",
                params![path.display().to_string()],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub(crate) fn set_ocr_overflow(&self, overflow: bool) -> Result<(), String> {
        let connection = self.connection()?;
        if overflow {
            connection.execute_batch("BEGIN; INSERT INTO grepmesh_metadata(key,value) VALUES ('pending_ocr_overflow','1'), ('ocr_recovery_full_scan_required','1') ON CONFLICT(key) DO UPDATE SET value='1'; COMMIT;")
        } else {
            connection.execute_batch("BEGIN; INSERT INTO grepmesh_metadata(key,value) VALUES ('ocr_recovery_full_scan_required','1') ON CONFLICT(key) DO UPDATE SET value='1'; DELETE FROM grepmesh_metadata WHERE key='pending_ocr_overflow'; COMMIT;")
        }.map_err(|error| error.to_string())
    }

    pub(crate) fn ocr_failure_count(
        &self,
        path: &Path,
        size: u64,
        mtime_ns: u128,
    ) -> Result<usize, String> {
        self.connection()?.query_row(
            "SELECT COALESCE((SELECT failures FROM grepmesh_ocr_attempts WHERE path=?1 AND size=?2 AND mtime_ns=?3),0)",
            params![path.display().to_string(), size as i64, mtime_ns.to_string()],
            |row| row.get(0),
        ).map_err(|error| error.to_string())
    }

    pub(crate) fn ocr_terminal_empty(
        &self,
        path: &Path,
        size: u64,
        mtime_ns: u128,
    ) -> Result<bool, String> {
        self.connection()?.query_row("SELECT EXISTS(SELECT 1 FROM grepmesh_ocr_empty_results WHERE path=?1 AND size=?2 AND mtime_ns=?3)",params![path.display().to_string(),size as i64,mtime_ns.to_string()],|row|row.get(0)).map_err(|error|error.to_string())
    }

    pub(crate) fn record_ocr_failures(
        &self,
        path: &Path,
        size: u64,
        mtime_ns: u128,
        attempts: usize,
    ) -> Result<usize, String> {
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        let key = path.display().to_string();
        let (exists, count): (bool, usize) = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM grepmesh_ocr_attempts WHERE path=?1), (SELECT count(*) FROM grepmesh_ocr_attempts)",
            params![key], |row| Ok((row.get(0)?,row.get(1)?)),
        ).map_err(|error| error.to_string())?;
        if !exists && count >= MAX_PENDING_OCR {
            return Err("OCR failure ledger is full".into());
        }
        transaction.execute(
            "INSERT INTO grepmesh_ocr_attempts(path,size,mtime_ns,failures) VALUES (?1,?2,?3,?4) \
             ON CONFLICT(path) DO UPDATE SET size=excluded.size,mtime_ns=excluded.mtime_ns,failures= \
             MIN(4, CASE WHEN size=excluded.size AND mtime_ns=excluded.mtime_ns THEN failures ELSE 0 END + excluded.failures)",
            params![key,size as i64,mtime_ns.to_string(),attempts.min(4) as i64],
        ).map_err(|error| error.to_string())?;
        let failures = transaction
            .query_row(
                "SELECT failures FROM grepmesh_ocr_attempts WHERE path=?1",
                params![key],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(failures)
    }

    fn replace_indexed_document(&self, document: &IndexedDocument) -> Result<(), String> {
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let path = document.path.display().to_string();
        // FTS5 cannot index an UNINDEXED path column. Looking up a path there
        // scans the complete content table and turns a rebuild into O(files^2).
        // Keep the FTS rowid in an ordinary keyed table and validate exactly
        // one row instead.
        let (cache_exists, cache_matches, fts_rowid): (bool, bool, Option<i64>) = transaction
            .query_row(
                "SELECT \
                   EXISTS(SELECT 1 FROM grepmesh_extraction_cache WHERE path=?1), \
                   EXISTS(SELECT 1 FROM grepmesh_extraction_cache \
                          WHERE path=?1 AND size=?2 AND mtime_ns=?3 AND body=?4), \
                   (SELECT fts_rowid FROM grepmesh_document_rows WHERE path=?1)",
                params![
                    path,
                    document.size as i64,
                    document.mtime_ns.to_string(),
                    document.body
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|error| error.to_string())?;
        let fts_matches = match fts_rowid {
            Some(rowid) => transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM grepmesh_documents \
                     WHERE rowid=?1 AND path=?2 AND body=?3)",
                    params![rowid, path, document.body],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(|error| error.to_string())?,
            None => false,
        };
        if cache_matches && fts_matches {
            let acknowledged = transaction
                .execute(
                    "DELETE FROM grepmesh_pending_ocr WHERE path=?1",
                    params![path],
                )
                .map_err(|error| error.to_string())?;
            let empty_cleared = transaction.execute("DELETE FROM grepmesh_ocr_empty_results WHERE path=?1 AND size=?2 AND mtime_ns=?3",params![path,document.size as i64,document.mtime_ns.to_string()]).map_err(|error|error.to_string())?;
            let cleared = transaction
                .execute(
                    "DELETE FROM grepmesh_ocr_attempts WHERE path=?1 AND size=?2 AND mtime_ns=?3",
                    params![path, document.size as i64, document.mtime_ns.to_string()],
                )
                .map_err(|error| error.to_string())?;
            if acknowledged != 0 || cleared != 0 || empty_cleared != 0 {
                transaction.commit().map_err(|error| error.to_string())?;
            }
            return Ok(());
        }
        if let Some(rowid) = fts_rowid {
            transaction
                .execute(
                    "DELETE FROM grepmesh_documents WHERE rowid=?1",
                    params![rowid],
                )
                .map_err(|error| error.to_string())?;
        } else if cache_exists {
            // A cache row without the row map is an interrupted migration or
            // external corruption. Pay for one legacy scan to repair it; the
            // normal new-document path never scans FTS by path.
            transaction
                .execute(
                    "DELETE FROM grepmesh_documents WHERE path=?1",
                    params![path],
                )
                .map_err(|error| error.to_string())?;
        }
        transaction
            .execute(
                "INSERT INTO grepmesh_documents(path, body) VALUES (?1, ?2)",
                params![path, document.body],
            )
            .map_err(|error| error.to_string())?;
        let fts_rowid = transaction.last_insert_rowid();
        transaction
            .execute(
                "INSERT INTO grepmesh_extraction_cache(path, size, mtime_ns, body) VALUES (?1, ?2, ?3, ?4)                  ON CONFLICT(path) DO UPDATE SET size=excluded.size, mtime_ns=excluded.mtime_ns, body=excluded.body",
                params![path, document.size as i64, document.mtime_ns.to_string(), document.body],
            )
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO grepmesh_document_rows(path, fts_rowid) VALUES (?1, ?2) \
                 ON CONFLICT(path) DO UPDATE SET fts_rowid=excluded.fts_rowid",
                params![path, fts_rowid],
            )
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO grepmesh_metadata(key, value) VALUES ('document_rows_v1', '1') \
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [],
            )
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "DELETE FROM grepmesh_pending_ocr WHERE path=?1",
                params![path],
            )
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "DELETE FROM grepmesh_ocr_attempts WHERE path=?1 AND size=?2 AND mtime_ns=?3",
                params![path, document.size as i64, document.mtime_ns.to_string()],
            )
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "DELETE FROM grepmesh_ocr_empty_results WHERE path=?1 AND size=?2 AND mtime_ns=?3",
                params![path, document.size as i64, document.mtime_ns.to_string()],
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
        let rows = {
            let mut statement = connection
                .prepare("SELECT path, fts_rowid FROM grepmesh_document_rows")
                .map_err(|error| error.to_string())?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        PathBuf::from(row.get::<_, String>(0)?),
                        row.get::<_, i64>(1)?,
                    ))
                })
                .map_err(|error| error.to_string())?;
            rows.filter_map(Result::ok).collect::<Vec<_>>()
        };
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        for (path, rowid) in rows.into_iter().filter(|(path, _)| !seen.contains(path)) {
            let path = path.display().to_string();
            transaction
                .execute(
                    "DELETE FROM grepmesh_documents WHERE rowid=?1",
                    params![rowid],
                )
                .map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "DELETE FROM grepmesh_extraction_cache WHERE path=?1",
                    params![path],
                )
                .map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "DELETE FROM grepmesh_document_rows WHERE path=?1",
                    params![path],
                )
                .map_err(|error| error.to_string())?;
        }
        transaction
            .execute(
                "INSERT INTO grepmesh_metadata(key, value) VALUES ('document_rows_v1', '1') \
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(())
    }

    fn remove_document(&self, path: &Path) -> Result<(), String> {
        self.remove_document_with_empty(path, None)
    }

    fn remove_document_with_empty(
        &self,
        path: &Path,
        empty: Option<(u64, u128)>,
    ) -> Result<(), String> {
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let path = path.display().to_string();
        let rowid = transaction
            .query_row(
                "SELECT fts_rowid FROM grepmesh_document_rows WHERE path=?1",
                params![path],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        let cache_exists: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM grepmesh_extraction_cache WHERE path=?1)",
                params![path],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        if let Some(rowid) = rowid {
            transaction
                .execute(
                    "DELETE FROM grepmesh_documents WHERE rowid=?1",
                    params![rowid],
                )
                .map_err(|error| error.to_string())?;
        } else if cache_exists {
            transaction
                .execute(
                    "DELETE FROM grepmesh_documents WHERE path=?1",
                    params![path],
                )
                .map_err(|error| error.to_string())?;
        }
        transaction
            .execute(
                "DELETE FROM grepmesh_extraction_cache WHERE path=?1",
                params![path.clone()],
            )
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "DELETE FROM grepmesh_document_rows WHERE path=?1",
                params![path],
            )
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO grepmesh_metadata(key, value) VALUES ('document_rows_v1', '1') \
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [],
            )
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "DELETE FROM grepmesh_pending_ocr WHERE path=?1",
                params![path],
            )
            .map_err(|error| error.to_string())?;
        if let Some((size, mtime_ns)) = empty {
            let (exists,count):(bool,usize) = transaction.query_row("SELECT EXISTS(SELECT 1 FROM grepmesh_ocr_empty_results WHERE path=?1),(SELECT count(*) FROM grepmesh_ocr_empty_results)",params![path],|row|Ok((row.get(0)?,row.get(1)?))).map_err(|error|error.to_string())?;
            if !exists && count >= MAX_PENDING_OCR {
                return Err("OCR empty-result ledger is full".into());
            }
            transaction.execute("INSERT INTO grepmesh_ocr_empty_results(path,size,mtime_ns,disposition) VALUES (?1,?2,?3,?4) ON CONFLICT(path) DO UPDATE SET size=excluded.size,mtime_ns=excluded.mtime_ns,disposition=excluded.disposition",params![path,size as i64,mtime_ns.to_string(),if size==0 {"empty_input"} else {"empty_output"}]).map_err(|error|error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())
    }

    fn migrate_document_rows(&self) -> Result<(), String> {
        let mut connection = self.connection()?;
        let migrated = connection
            .query_row(
                "SELECT count(*)=2 FROM grepmesh_metadata \
                 WHERE key IN ('document_rows_v1', 'document_rows_triggers_v1') \
                 AND value='1'",
                [],
                |row| row.get::<_, bool>(0),
            )
            .map_err(|error| error.to_string())?;
        if migrated {
            return Ok(());
        }
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        transaction
            .execute("DELETE FROM grepmesh_document_rows", [])
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO grepmesh_document_rows(path, fts_rowid) \
                 SELECT path, max(rowid) FROM grepmesh_documents GROUP BY path",
                [],
            )
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "DELETE FROM grepmesh_documents \
                 WHERE rowid NOT IN (SELECT fts_rowid FROM grepmesh_document_rows)",
                [],
            )
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO grepmesh_metadata(key, value) VALUES ('document_rows_v1', '1') \
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [],
            )
            .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO grepmesh_metadata(key, value) VALUES ('document_rows_triggers_v1', '1') \
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())
    }

    fn backfill_cache_from_fts(&self) -> Result<(), String> {
        let mut connection = self.connection()?;
        let missing = {
            let mut statement = connection
                .prepare(
                    "SELECT rows.path, rows.fts_rowid \
                     FROM grepmesh_document_rows AS rows \
                     LEFT JOIN grepmesh_extraction_cache AS cache ON cache.path=rows.path \
                     WHERE cache.path IS NULL",
                )
                .map_err(|error| error.to_string())?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(|error| error.to_string())?;
            rows.filter_map(Result::ok).collect::<Vec<_>>()
        };
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        for (path, rowid) in missing {
            let metadata = match fs::metadata(&path) {
                Ok(metadata) if metadata.is_file() => metadata,
                _ => continue,
            };
            let body = transaction
                .query_row(
                    "SELECT body FROM grepmesh_documents WHERE rowid=?1",
                    params![rowid],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|error| error.to_string())?;
            transaction.execute(
                "INSERT OR IGNORE INTO grepmesh_extraction_cache(path, size, mtime_ns, body) VALUES (?1, ?2, ?3, ?4)",
                params![path, metadata.len() as i64, metadata_mtime_ns(&metadata).to_string(), body],
            ).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO grepmesh_document_rows(path, fts_rowid) VALUES (?1, ?2) \
                     ON CONFLICT(path) DO UPDATE SET fts_rowid=excluded.fts_rowid",
                    params![path, rowid],
                )
                .map_err(|error| error.to_string())?;
        }
        transaction
            .execute(
                "INSERT INTO grepmesh_metadata(key, value) VALUES ('document_rows_v1', '1') \
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn clear(&self) -> Result<(), String> {
        let connection = self.connection()?;
        connection
            .execute_batch("DELETE FROM grepmesh_documents; DELETE FROM grepmesh_extraction_cache; DELETE FROM grepmesh_document_rows; DELETE FROM grepmesh_metadata;")
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn candidates(&self, query: &str) -> Result<Vec<PathBuf>, String> {
        let connection = self.read_connection(Instant::now() + Duration::from_secs(2))?;
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

    fn read_connection(&self, deadline: Instant) -> Result<Connection, String> {
        if Instant::now() >= deadline {
            return Err("indexed search deadline".into());
        }
        // No schema/PRAGMA writes on an interactive read. Each worker owns its connection.
        let connection = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| e.to_string())?;
        connection
            .busy_timeout(
                Duration::from_millis(100).min(deadline.saturating_duration_since(Instant::now())),
            )
            .map_err(|e| e.to_string())?;
        connection.progress_handler(256, Some(move || Instant::now() >= deadline));
        connection
            .execute_batch("BEGIN DEFERRED;")
            .map_err(|e| e.to_string())?;
        Ok(connection)
    }

    #[cfg(test)]
    fn visit_matching_documents(
        &self,
        connection: &Connection,
        query: &str,
        root: &Path,
        deadline: Instant,
        allow: impl FnMut(&Path) -> bool,
        visit: impl FnMut(PathBuf, String) -> bool,
    ) -> Result<bool, String> {
        self.visit_matching_documents_with_paths(
            connection,
            query,
            root,
            &[],
            deadline,
            allow,
            visit,
        )
    }

    fn visit_matching_documents_with_paths(
        &self,
        connection: &Connection,
        query: &str,
        root: &Path,
        path_globs: &[String],
        deadline: Instant,
        mut allow: impl FnMut(&Path) -> bool,
        mut visit: impl FnMut(PathBuf, String) -> bool,
    ) -> Result<bool, String> {
        let root_text = root.to_string_lossy().into_owned();
        let separator = std::path::MAIN_SEPARATOR;
        let stem = root_text.trim_end_matches(separator);
        let prefix = format!("{stem}{separator}");
        let upper = format!("{stem}{}", char::from_u32(separator as u32 + 1).unwrap());
        let fts_query = fts_literal_query(query);
        let pattern = format!(
            "%{}%",
            query
                .replace('\\', "\\\\")
                .replace('%', r"\%")
                .replace('_', r"\_")
        );
        let mut seen = BTreeSet::new();
        let mut body_bytes = 0usize;
        const BODY_BUDGET: usize = 64 * 1024 * 1024;
        // Paths/row IDs first; never materialize global document bodies or rank-sort them.
        let names = exact_glob_basenames(path_globs);
        let queries: Vec<(&str, String)> = if let Some(names) = names {
            // A known filename must not first traverse global FTS postings.
            // This is only a conservative prefilter; allow() still applies
            // complete glob/root/exclusion semantics before loading a body.
            names
                .into_iter()
                .map(|name| {
                    (
                        "SELECT fts_rowid,path FROM grepmesh_document_rows \
                 WHERE path>=?3 AND path<?4 AND substr(path,-length(?1))=?1 \
                 UNION ALL SELECT fts_rowid,path FROM grepmesh_document_rows \
                 WHERE path=?2 AND substr(path,-length(?1))=?1",
                        format!("{separator}{name}"),
                    )
                })
                .collect()
        } else {
            vec![
                (
                    "SELECT d.fts_rowid,d.path FROM grepmesh_documents f \
             JOIN grepmesh_document_rows d ON d.fts_rowid=f.rowid \
             WHERE f.body MATCH ?1 AND (d.path=?2 OR (d.path>=?3 AND d.path<?4))",
                    fts_query,
                ),
                (
                    "SELECT fts_rowid,path FROM grepmesh_document_rows \
              WHERE path>=?3 AND path<?4 AND path LIKE ?1 ESCAPE '\\' \
              UNION ALL SELECT fts_rowid,path FROM grepmesh_document_rows \
              WHERE path=?2 AND path LIKE ?1 ESCAPE '\\'",
                    pattern,
                ),
            ]
        };
        let mut body = connection.prepare(
            "SELECT body FROM grepmesh_documents WHERE rowid=?1 AND length(CAST(body AS BLOB))<=?2")
            .map_err(|e| e.to_string())?;
        for (sql, term) in queries {
            let mut statement = connection.prepare(sql).map_err(|e| e.to_string())?;
            let mut rows = statement
                .query(params![term, root_text, prefix, upper])
                .map_err(|e| e.to_string())?;
            while let Some(row) = rows.next().map_err(|e| e.to_string())? {
                if Instant::now() >= deadline {
                    return Err("indexed search deadline".into());
                }
                let id: i64 = row.get(0).map_err(|e| e.to_string())?;
                let path = PathBuf::from(row.get::<_, String>(1).map_err(|e| e.to_string())?);
                if !path.starts_with(root) || !allow(&path) || !seen.insert(id) {
                    continue;
                }
                if seen.len() > 20_000 {
                    return Err("indexed search candidate budget".into());
                }
                let remaining = BODY_BUDGET.saturating_sub(body_bytes);
                let text: Option<String> = body
                    .query_row(params![id, remaining as i64], |r| r.get(0))
                    .optional()
                    .map_err(|e| e.to_string())?;
                let text = text.ok_or_else(|| "indexed search body budget".to_string())?;
                body_bytes += text.len();
                if !visit(path, text) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
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
    ocr: Option<Arc<OcrEngine>>,
    pending_store_error: Option<String>,
}

impl IndexManager {
    #[cfg(test)]
    pub(crate) fn cached_test_index(root: &Path, db: PathBuf, documents: &[(&Path, &str)]) -> Self {
        let persistent = PersistentIndex::open(db).unwrap();
        for (path, body) in documents {
            persistent.replace_document(path, body).unwrap();
        }
        let mut index = Self::disabled();
        index
            .ready_roots
            .write()
            .unwrap()
            .insert(root.to_path_buf());
        index.persistent = Some(persistent);
        index
    }

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
            ocr: None,
            pending_store_error: None,
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
        let ocr = OcrEngine::new(ocr_config).map(Arc::new);
        let pending_store_error = match (ocr.as_ref(), persistent.as_ref()) {
            (Some(ocr), Some(index)) => ocr.attach_pending_store(index.clone()).err(),
            (Some(ocr), None) if ocr.is_mesh() => Some("durable OCR queue is unavailable".into()),
            _ => None,
        };
        let thread_pending_store_error = pending_store_error.clone();
        let manager_ocr = ocr.clone();
        thread::spawn(move || {
            let index_storage_path = persistent_state.as_ref().map(|index| index.path.clone());
            let (events, rx) = mpsc::sync_channel(1);
            let pending_event_paths =
                Arc::new(Mutex::new(BTreeMap::<PathBuf, PendingIndexEvent>::new()));
            let watcher_pending_paths = Arc::clone(&pending_event_paths);
            let watched_index_storage_path = index_storage_path.clone();
            let watch_roots = ordered_roots(&roots);
            let watch_excludes = compile_excludes(&excludes).unwrap_or_default();
            let callback_roots = watch_roots.clone();
            let callback_excludes = watch_excludes.clone();
            let (watch_events, watch_rx) = mpsc::sync_channel(MAX_WATCH_RECOVERY_UNITS);
            thread::spawn(move || {
                let mut watcher: RecommendedWatcher = match notify::recommended_watcher(
                    move |event: notify::Result<notify::Event>| {
                        if let Ok(event) = event {
                            let new_subtree = new_subtree_event(event.kind);
                            let rename_target = matches!(
                                event.kind,
                                EventKind::Modify(ModifyKind::Name(RenameMode::Both))
                            )
                            .then(|| event.paths.last().cloned())
                            .flatten();
                            let paths =
                                event
                                    .paths
                                    .into_iter()
                                    .filter(|path| {
                                        !watched_index_storage_path.as_ref().is_some_and(
                                            |index_path| is_index_storage_path(path, index_path),
                                        ) && callback_roots
                                            .iter()
                                            .filter(|root| path.starts_with(root))
                                            .max_by_key(|root| root.components().count())
                                            .is_some_and(|root| {
                                                !excluded(path, root, &callback_excludes)
                                            })
                                    })
                                    .collect::<Vec<_>>();
                            if !paths.is_empty() {
                                if let Ok(mut pending) = watcher_pending_paths.lock() {
                                    for path in paths {
                                        let is_target = rename_target
                                            .as_ref()
                                            .is_none_or(|target| target == &path);
                                        if new_subtree && is_target && path.is_dir() {
                                            if let Err(error) = watch_events.try_send(path.clone())
                                            {
                                                tracing::error!(path = %path.display(), error = %error, "queue new GrepMesh watch failed");
                                            }
                                        }
                                        let event = pending.entry(path).or_default();
                                        event.count = event.count.saturating_add(1);
                                        event.new_subtree |= new_subtree && is_target;
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
                for root in &watch_roots {
                    for (path, error) in register_watch_root(root, &watch_excludes, &mut |path| {
                        watcher.watch(path, RecursiveMode::NonRecursive)
                    }) {
                        tracing::error!(root = %root.display(), path = %path.display(), error = ?error, "watch GrepMesh root failed");
                    }
                }
                while let Ok(path) = watch_rx.recv() {
                    if let Some(root) = watch_roots
                        .iter()
                        .filter(|root| path.starts_with(root))
                        .max_by_key(|root| root.components().count())
                    {
                        for (path, error) in
                            register_watch_subtree(&path, root, &watch_excludes, &mut |path| {
                                watcher.watch(path, RecursiveMode::NonRecursive)
                            })
                        {
                            tracing::error!(root = %root.display(), path = %path.display(), error = ?error, "watch new GrepMesh subtree failed");
                        }
                    }
                }
            });
            let configured_roots = ordered_roots(&roots).into_iter().collect::<BTreeSet<_>>();
            let mut last_full_rebuild_ms = persistent_state
                .as_ref()
                .and_then(|index| index.last_full_rebuild_ms().ok().flatten());
            let rebuild_due = |last: Option<u64>| {
                let now = unix_time_ms();
                let recovery_required = ocr.as_ref().is_none_or(|ocr| ocr.deferred_count() == 0)
                    && persistent_state.as_ref().is_some_and(|index| {
                        index.ocr_recovery_full_scan_required().unwrap_or(false)
                    });
                recovery_required
                    || last.is_none_or(|last| {
                        now < last || now.saturating_sub(last) >= full_rebuild_min_interval_ms
                    })
            };
            let run_rebuild = |last: &mut Option<u64>, generation: &mut u64| {
                let succeeded = rebuild_index(
                    &roots,
                    &excludes,
                    max_file_bytes,
                    stt.as_ref(),
                    ocr.as_deref(),
                    &activity_config,
                    RebuildState {
                        snapshot: &state,
                        candidates: &candidate_state,
                        ready_roots: &ready_root_state,
                        persistent: persistent_state.as_ref(),
                    },
                    generation,
                );
                if succeeded
                    && thread_pending_store_error.is_none()
                    && ocr.as_ref().is_none_or(|ocr| ocr.deferred_count() == 0)
                {
                    let completed_ms = unix_time_ms();
                    if let Some(index) = persistent_state.as_ref() {
                        if let Err(error) = index.mark_full_rebuild_ms(completed_ms) {
                            if let Ok(mut current) = state.write() {
                                current.state = IndexState::Degraded;
                                current.last_error =
                                    Some(format!("persist full reconciliation marker: {error}"));
                            }
                            return;
                        }
                    }
                    *last = Some(completed_ms);
                }
            };
            let mut generation = 0;
            if (ocr.as_ref().is_none_or(|ocr| ocr.deferred_count() == 0)
                && rebuild_due(last_full_rebuild_ms))
                || persistent_state.is_none()
            {
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
            let mut pending = ocr.as_ref().is_some_and(|ocr| ocr.deferred_count() > 0);
            let mut directory_activity = BTreeMap::<PathBuf, DirectoryActivityState>::new();
            let mut running = BTreeMap::<PathBuf, thread::JoinHandle<()>>::new();
            loop {
                let finished: Vec<_> = running
                    .iter()
                    .filter(|(_, task)| task.is_finished())
                    .map(|(path, _)| path.clone())
                    .collect();
                for path in finished {
                    if running.remove(&path).expect("running task").join().is_err() {
                        tracing::warn!(input = %path.display(), "OCR retry panicked; durable input remains pending");
                    }
                }
                let activity_delay_until = directory_activity
                    .values()
                    .map(|stats| stats.hot_until_ms)
                    .max()
                    .unwrap_or(0);
                let remaining = if !running.is_empty() {
                    Duration::from_secs(1)
                } else if ocr.as_ref().is_some_and(|ocr| ocr.deferred_count() > 0) {
                    // An overdue full pass is intentionally blocked while GPU
                    // work is deferred. Never turn its expired deadline into
                    // recv_timeout(0) and spin throughout the GPU outage.
                    ocr.as_ref()
                        .map(|ocr| ocr.retry_poll_interval())
                        .unwrap_or(Duration::from_secs(30))
                        .max(Duration::from_secs(1))
                } else if pending {
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
                    let event_batch = pending_event_paths
                        .lock()
                        .map(|mut pending| std::mem::take(&mut *pending))
                        .unwrap_or_default();
                    let event_batch = event_batch
                        .into_iter()
                        .filter(|(path, _)| {
                            !index_storage_path
                                .as_ref()
                                .is_some_and(|index_path| is_index_storage_path(path, index_path))
                        })
                        .collect::<BTreeMap<_, _>>();
                    if event_batch.is_empty() {
                        continue;
                    }
                    let new_subtrees = event_batch
                        .iter()
                        .filter(|(_, event)| event.new_subtree)
                        .map(|(path, _)| path.clone())
                        .collect::<BTreeSet<_>>();
                    let event_counts = event_batch
                        .into_iter()
                        .map(|(path, event)| (path, event.count))
                        .collect::<BTreeMap<_, _>>();
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
                            ocr.as_deref(),
                            index,
                        );
                        reconcile_new_subtrees(
                            &new_subtrees,
                            &roots,
                            &excludes,
                            &activity_config,
                            &directory_activity,
                            max_file_bytes,
                            stt.as_ref(),
                            ocr.as_deref(),
                            index,
                        );
                    }
                    pending = true;
                }
                // Keep up to three independent queue jobs live; workers admit one
                // request each using their fresh measured CPU/RAM/GPU capacity.
                if let (Some(ocr), Some(index)) = (ocr.as_ref(), persistent_state.as_ref()) {
                    let metadata_matcher =
                        compile_excludes(&activity_config.metadata_only_globs).ok();
                    let ignored_matcher = compile_excludes(&excludes).ok();
                    let configured = ordered_roots(&roots);
                    let cleanup = ocr.metadata_retry_paths(64, |path| {
                        !running.contains_key(path)
                            && configured
                                .iter()
                                .filter(|root| path.starts_with(root))
                                .max_by_key(|root| root.components().count())
                                .is_some_and(|root| {
                                    ignored_matcher
                                        .as_ref()
                                        .is_some_and(|matcher| excluded(path, root, matcher))
                                        || metadata_matcher
                                            .as_ref()
                                            .is_some_and(|matcher| excluded(path, root, matcher))
                                })
                    });
                    for path in cleanup {
                        retry_ocr_path(
                            &path,
                            &roots,
                            &excludes,
                            &activity_config,
                            max_file_bytes,
                            None,
                            ocr,
                            index,
                        );
                        pending = true;
                    }
                    let active = running.keys().cloned().collect();
                    let retry = ocr.take_retry_paths_excluding(
                        crate::compute_client::MAX_PARALLEL_OCR.saturating_sub(running.len()),
                        &active,
                    );
                    for path in retry {
                        let Some(path_lease) = ocr.claim_retry(&path) else {
                            continue;
                        };
                        let (job_roots, job_excludes, job_activity, job_ocr, job_index, job_path) = (
                            roots.clone(),
                            excludes.clone(),
                            activity_config.clone(),
                            Arc::clone(ocr),
                            index.clone(),
                            path.clone(),
                        );
                        match thread::Builder::new()
                            .name("grepmesh-ocr-retry".into())
                            .spawn(move || {
                                let _path_lease = path_lease;
                                retry_ocr_path(
                                    &job_path,
                                    &job_roots,
                                    &job_excludes,
                                    &job_activity,
                                    max_file_bytes,
                                    None,
                                    &job_ocr,
                                    &job_index,
                                );
                            }) {
                            Ok(task) => {
                                running.insert(path, task);
                                pending = true;
                            }
                            Err(error) => {
                                tracing::warn!(error = %error, "OCR retry thread unavailable; input retained")
                            }
                        }
                    }
                    if ocr.clear_overflow_if_drained() {
                        pending = true;
                    }
                }
                if pending
                    && running.is_empty()
                    && ocr.as_ref().is_none_or(|ocr| ocr.deferred_count() == 0)
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
            ocr: manager_ocr,
            pending_store_error,
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
        let deferred = self.ocr.as_ref().map_or(0, |ocr| ocr.deferred_count());
        if deferred > 0 && snapshot.state != IndexState::Building {
            snapshot.state = IndexState::Degraded;
            let pending = format!("{deferred} OCR inputs await an available OCR worker");
            snapshot.last_error = Some(match snapshot.last_error {
                Some(error) => format!("{error}; {pending}"),
                None => pending,
            });
        }
        if let Some(error) = &self.pending_store_error {
            snapshot.state = IndexState::Degraded;
            let pending = format!("attach durable OCR queue: {error}");
            snapshot.last_error = Some(match snapshot.last_error {
                Some(error) => format!("{error}; {pending}"),
                None => pending,
            });
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
        self.search_text_hits_filtered(query, root, limit, context_lines, case_sensitive, &[], &[])
    }

    pub fn search_text_hits_filtered(
        &self,
        query: &str,
        root: &Path,
        limit: usize,
        context_lines: usize,
        case_sensitive: bool,
        path_globs: &[String],
        exclude_globs: &[String],
    ) -> Option<Vec<IndexTextHit>> {
        self.search_text_hits_filtered_until(
            query,
            root,
            limit,
            context_lines,
            case_sensitive,
            path_globs,
            exclude_globs,
            Instant::now() + Duration::from_secs(2),
        )
        .ok()
        .flatten()
        .map(|page| page.hits)
    }

    pub fn search_text_hits_filtered_until(
        &self,
        query: &str,
        root: &Path,
        limit: usize,
        context_lines: usize,
        case_sensitive: bool,
        path_globs: &[String],
        exclude_globs: &[String],
        deadline: Instant,
    ) -> Result<Option<IndexedSearch>, String> {
        if query.as_bytes().len() < 3 || limit == 0 {
            return Ok(None);
        }
        let Some(persistent) = self.persistent.as_ref() else {
            return Ok(None);
        };
        let connection = persistent.read_connection(deadline)?;
        let root_ready = self
            .ready_roots
            .read()
            .ok()
            .is_some_and(|roots| roots.iter().any(|ready| root.starts_with(ready)));
        if !root_ready {
            let baseline: Option<String> = connection
                .query_row(
                    "SELECT value FROM grepmesh_metadata WHERE key='last_full_rebuild_ms'",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| e.to_string())?;
            if baseline.is_none() {
                return Ok(None);
            }
        }
        let excludes = compile_excludes(exclude_globs).map_err(|e| e.to_string())?;
        let positive = path_globs
            .iter()
            .filter(|g| !g.starts_with('!'))
            .cloned()
            .collect::<Vec<_>>();
        let negative = path_globs
            .iter()
            .filter_map(|g| g.strip_prefix('!').map(str::to_owned))
            .collect::<Vec<_>>();
        let includes = compile_excludes(&positive).map_err(|e| e.to_string())?;
        let rejects = compile_excludes(&negative).map_err(|e| e.to_string())?;
        let needle = if case_sensitive {
            query.to_string()
        } else {
            query.to_lowercase()
        };
        let mut hits = Vec::new();
        let mut parse_deadline = false;
        let exhausted = persistent.visit_matching_documents_with_paths(
            &connection,
            query,
            root,
            &positive,
            deadline,
            |path| {
                if !path.starts_with(root) || excluded(path, root, &excludes) {
                    return false;
                }
                let relative = path.strip_prefix(root).unwrap_or(path);
                let matches = |set: &GlobSet| set.is_match(path) || set.is_match(relative);
                (positive.is_empty() || matches(&includes)) && !matches(&rejects)
            },
            |path, body| {
                let mut content_match = false;
                let lines: Vec<&str> = body.lines().collect();
                for (index, line) in lines.iter().enumerate() {
                    if Instant::now() >= deadline {
                        parse_deadline = true;
                        return false;
                    }
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
                        return false;
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
                        return false;
                    }
                }
                true
            },
        )?;
        if parse_deadline {
            return Err("indexed search deadline".into());
        }
        Ok(Some(IndexedSearch {
            hits,
            truncated: !exhausted,
        }))
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

/// Create/rename-target events can precede recursive watch attachment. Walk
/// only those new subtrees on the existing index worker to recover child events
/// missed in that interval; ordinary directory metadata never reaches here.
fn reconcile_new_subtrees(
    paths: &BTreeSet<PathBuf>,
    roots: &BTreeMap<String, Vec<PathBuf>>,
    excludes: &[String],
    activity_config: &IndexActivityConfig,
    activity: &BTreeMap<PathBuf, DirectoryActivityState>,
    max_file_bytes: u64,
    stt: Option<&SttEngine>,
    ocr: Option<&OcrEngine>,
    persistent: &PersistentIndex,
) {
    let combined_excludes = excludes
        .iter()
        .chain(&activity_config.exclude_globs)
        .cloned()
        .collect::<Vec<_>>();
    let Ok(matcher) = compile_excludes(&combined_excludes) else {
        return;
    };
    let configured_roots = ordered_roots(roots)
        .into_iter()
        .map(|root| {
            let canonical = fs::canonicalize(&root).unwrap_or_else(|_| root.clone());
            (root, canonical)
        })
        .collect::<Vec<_>>();
    let activity_bucket_roots = activity_config
        .activity_bucket_roots
        .iter()
        .map(|root| fs::canonicalize(root).unwrap_or_else(|_| root.clone()))
        .collect::<Vec<_>>();
    for raw_path in paths {
        if raw_path
            .ancestors()
            .skip(1)
            .any(|parent| paths.contains(parent))
        {
            continue;
        }
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
        if !path.is_dir() || excluded(&path, root, &matcher) {
            continue;
        }
        let bucket = activity_bucket(&path, root, &activity_bucket_roots);
        let mut metadata_globs = activity_config.metadata_only_globs.clone();
        if activity_bucket_is_hot(activity, &bucket, unix_time_ms()) {
            metadata_globs.push("**".into());
        }
        let metadata_only = compile_excludes(&metadata_globs).ok();
        // Reuse the full walk's serial directory-unit traversal. Commit each
        // file immediately rather than buffering all subtree document bodies.
        let mut units = VecDeque::from([path]);
        while let Some(unit) = units.pop_front() {
            match scan_directory_unit(
                &unit,
                root,
                &matcher,
                max_file_bytes,
                false,
                stt,
                ocr,
                Some(persistent),
                metadata_only.as_ref(),
                false,
            ) {
                Ok((_, _, documents, children)) => {
                    units.extend(children);
                    for document in documents {
                        match persistent.replace_indexed_document(&document) {
                            Ok(()) => {
                                if let Some(ocr) = ocr {
                                    ocr.forget_deferred(&document.path);
                                }
                            }
                            Err(error) => {
                                if let Some(ocr) = ocr {
                                    let _ = ocr.defer_path(&document.path);
                                }
                                tracing::warn!(path = %document.path.display(), error = %error, "new subtree index update failed");
                            }
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(path = %unit.display(), error = %error, "new subtree index update failed")
                }
            }
        }
    }
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
    let activity_bucket_roots = activity_config
        .activity_bucket_roots
        .iter()
        .map(|root| fs::canonicalize(root).unwrap_or_else(|_| root.clone()))
        .collect::<Vec<_>>();
    let mut new_activity_buckets = BTreeSet::new();
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
        let bucket = activity_bucket(&path, root, &activity_bucket_roots);
        if !activity.contains_key(&bucket) {
            new_activity_buckets.insert(bucket.clone());
        }
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
    // A newly isolated source root inherits any active parent cooldown. This
    // preserves the existing hot guard when its bucket becomes more specific.
    let now = unix_time_ms();
    for bucket in &new_activity_buckets {
        inherit_active_parent_cooldown(activity, bucket, now, activity_config.hot_debounce_ms);
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
        if ocr.is_some_and(|engine| engine.is_in_flight(path)) {
            continue;
        }
        // Directory metadata events are emitted for every child write. They
        // carry no file content change and would falsely make the root hot.
        if path.exists() && !path.is_file() {
            continue;
        }
        let bucket = activity_bucket(path, root, &activity_bucket_roots);
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
            false,
        ) {
            Ok((_, _, documents, _)) if documents.is_empty() => {
                if !ocr.is_some_and(|engine| engine.is_deferred(path)) {
                    let _ = persistent.remove_document(path);
                }
            }
            Ok((_, _, documents, _)) => {
                for document in documents {
                    match persistent.replace_indexed_document(&document) {
                        Ok(()) => {
                            if let Some(ocr) = ocr {
                                ocr.forget_deferred(&document.path);
                            }
                        }
                        Err(error) => {
                            if let Some(ocr) = ocr {
                                let _ = ocr.defer_path(&document.path);
                            }
                            tracing::warn!(path = %path.display(), error = %error, "incremental index update failed");
                        }
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

fn activity_bucket(path: &Path, root: &Path, isolated_roots: &[PathBuf]) -> PathBuf {
    if let Some(isolated) = isolated_roots
        .iter()
        .filter(|isolated| path.starts_with(isolated))
        .max_by_key(|isolated| isolated.components().count())
    {
        return isolated.clone();
    }
    let relative = path.strip_prefix(root).unwrap_or(path);
    if path.parent() == Some(root) {
        root.to_path_buf()
    } else {
        relative
            .components()
            .next()
            .map(|component| root.join(component.as_os_str()))
            .unwrap_or_else(|| root.to_path_buf())
    }
}

fn inherit_active_parent_cooldown(
    activity: &mut BTreeMap<PathBuf, DirectoryActivityState>,
    bucket: &Path,
    now_ms: u64,
    hot_debounce_ms: u64,
) {
    let inherited_until = activity
        .iter()
        .filter(|(parent, stats)| {
            parent.as_path() != bucket
                && (bucket.starts_with(parent.as_path()) || parent.starts_with(bucket))
                && now_ms < stats.hot_until_ms
        })
        .map(|(_, stats)| stats.hot_until_ms)
        .max()
        .unwrap_or_default();
    if inherited_until > 0 {
        let stats = activity.entry(bucket.to_path_buf()).or_default();
        stats.hot_until_ms = stats.hot_until_ms.max(inherited_until);
        stats.metadata_only = true;
        stats.debounce_ms = hot_debounce_ms;
    }
}

fn activity_bucket_is_hot(
    activity: &BTreeMap<PathBuf, DirectoryActivityState>,
    bucket: &Path,
    now_ms: u64,
) -> bool {
    if let Some(stats) = activity.get(bucket) {
        return now_ms < stats.hot_until_ms;
    }
    activity.iter().any(|(parent, stats)| {
        (bucket.starts_with(parent) || parent.starts_with(bucket)) && now_ms < stats.hot_until_ms
    })
}

/// Retry an actual pending file without fabricating filesystem activity or
/// allowing a hot bucket's metadata cache to masquerade as completed OCR.
fn retry_ocr_path(
    path: &Path,
    roots: &BTreeMap<String, Vec<PathBuf>>,
    excludes: &[String],
    activity_config: &IndexActivityConfig,
    max_file_bytes: u64,
    stt: Option<&SttEngine>,
    ocr: &OcrEngine,
    persistent: &PersistentIndex,
) {
    let metadata = match fs::symlink_metadata(path) {
        Ok(value) => value,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            match persistent.remove_document(path) {
                Ok(()) => ocr.forget_deferred(path),
                Err(error) => {
                    tracing::warn!(error = %error, "remove missing deferred OCR document failed")
                }
            }
            return;
        }
        Err(_) => return,
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        ocr.forget_deferred(path);
        return;
    }
    let configured = ordered_roots(roots);
    let Some(root) = configured
        .iter()
        .filter(|root| path.starts_with(root))
        .max_by_key(|root| root.components().count())
    else {
        ocr.forget_deferred(path);
        return;
    };
    if !ocr.is_image(path) && !ocr.is_pdf(path) {
        ocr.forget_deferred(path);
        return;
    }
    if let Ok(root_metadata) = fs::symlink_metadata(root) {
        if device_id(&metadata).is_some_and(|device| Some(device) != device_id(&root_metadata)) {
            ocr.forget_deferred(path);
            return;
        }
    }
    let mut all_excludes = excludes.to_vec();
    all_excludes.extend(activity_config.exclude_globs.iter().cloned());
    let Ok(matcher) = compile_excludes(&all_excludes) else {
        return;
    };
    if excluded(path, root, &matcher) {
        if persistent.remove_document(path).is_ok() {
            ocr.forget_deferred(path);
        }
        return;
    }
    let metadata_only = compile_excludes(&activity_config.metadata_only_globs).ok();
    match scan_directory_unit(
        path,
        root,
        &matcher,
        max_file_bytes,
        false,
        stt,
        Some(ocr),
        Some(persistent),
        metadata_only.as_ref(),
        true,
    ) {
        Ok((_, _, documents, _)) => {
            for document in documents {
                if !fs::metadata(&document.path).is_ok_and(|now| {
                    now.len() == document.size && metadata_mtime_ns(&now) == document.mtime_ns
                }) {
                    let _ = ocr.defer_path(&document.path);
                    continue;
                }
                match persistent.replace_indexed_document(&document) {
                    Ok(()) => {
                        ocr.forget_deferred(&document.path);
                        tracing::info!(input = %document.path.display(), "mesh OCR index committed");
                    }
                    Err(error) => {
                        let _ = ocr.defer_path(&document.path);
                        tracing::warn!(error = %error, "deferred OCR index write failed");
                    }
                }
            }
        }
        Err(error) => tracing::warn!(error = %error, "deferred OCR scan failed"),
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
                false,
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
                        if let Some(ocr) = ocr {
                            let _ = ocr.defer_path(&document.path);
                        }
                        if let Ok(mut current) = rebuild.snapshot.write() {
                            current.state = IndexState::Degraded;
                            current.last_error =
                                Some(format!("{}: {error}", document.path.display()));
                            current.generation = generation.saturating_add(1);
                        }
                        return false;
                    }
                    if let Some(ocr) = ocr {
                        ocr.forget_deferred(&document.path);
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
        dispatch_mesh: false,
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
    dispatch_mesh: bool,
) -> Result<DirectoryScan, String> {
    let root_device = device_id(
        &fs::symlink_metadata(root).map_err(|error| format!("{}: {error}", root.display()))?,
    );
    let mut map = BTreeMap::new();
    let metadata = match fs::symlink_metadata(unit) {
        Ok(metadata) => metadata,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::PermissionDenied | ErrorKind::NotFound
            ) =>
        {
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
            dispatch_mesh,
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
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::PermissionDenied | ErrorKind::NotFound
            ) =>
        {
            Vec::new()
        }
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

fn complete_empty_ocr(
    path: &Path,
    context: &ScanContext<'_>,
    ocr: &OcrEngine,
    expected: Option<&fs::Metadata>,
) -> Result<usize, String> {
    if expected.is_some_and(|before| {
        !fs::metadata(path).is_ok_and(|now| {
            now.len() == before.len() && metadata_mtime_ns(&now) == metadata_mtime_ns(before)
        })
    }) {
        ocr.defer_path(path).map_err(|error| error.to_string())?;
        return Ok(0);
    }
    if ocr.is_mesh() {
        if let Some(index) = context.persistent {
            let empty = match expected {
                Some(metadata)
                    if index.ocr_failure_count(
                        path,
                        metadata.len(),
                        metadata_mtime_ns(metadata),
                    )? < crate::compute_client::MAX_RECOGNITION_FAILURES =>
                {
                    Some((metadata.len(), metadata_mtime_ns(metadata)))
                }
                _ => None,
            };
            if let Err(error) = index.remove_document_with_empty(path, empty) {
                let _ = ocr.defer_path(path);
                return Err(error);
            }
        }
        ocr.forget_deferred(path);
    }
    Ok(0)
}

fn walk(
    path: &PathBuf,
    context: &ScanContext<'_>,
    map: &mut BTreeMap<String, BTreeSet<PathBuf>>,
    documents: &mut Vec<IndexedDocument>,
) -> Result<usize, String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::PermissionDenied | ErrorKind::NotFound
            ) =>
        {
            return Ok(0)
        }
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
        if !context.dispatch_mesh && context.ocr.is_some_and(|ocr| ocr.is_in_flight(path)) {
            return Ok(0);
        }
        let size = metadata.len();
        let mtime_ns = metadata_mtime_ns(&metadata);
        if context
            .metadata_only
            .is_some_and(|matcher| excluded(path, context.root, matcher))
        {
            // Scanning is not a durable ACK. The caller commits the metadata
            // document/FTS/cache and only then removes the deferred entry.
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
        if let Some(persistent) = context
            .persistent
            .filter(|_| context.ocr.is_none_or(|ocr| !ocr.is_deferred(path)))
        {
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
            if ocr
                .skip_recognition_for_version(path, size, mtime_ns)
                .map_err(|error| error.to_string())?
            {
                return complete_empty_ocr(path, context, ocr, Some(&metadata));
            }
            if metadata.len() == 0 {
                return complete_empty_ocr(path, context, ocr, Some(&metadata));
            }
            if ocr.max_image_bytes() != 0 && metadata.len() > ocr.max_image_bytes() {
                ocr.forget_deferred(path);
                return Ok(0);
            }
            if ocr.is_mesh() && !context.dispatch_mesh {
                ocr.defer_path(path).map_err(|error| error.to_string())?;
                return Ok(0);
            }
            match ocr.extract_image(path) {
                Ok(text) if !text.trim().is_empty() => text,
                Ok(_) => return complete_empty_ocr(path, context, ocr, Some(&metadata)),
                Err(error) => {
                    tracing::warn!(path = %path.display(), error = %error, "OCR skipped image file");
                    return Ok(0);
                }
            }
        } else {
            let pdf_ocr = context.ocr.filter(|ocr| ocr.is_pdf(path));
            if let Some(ocr) = pdf_ocr {
                if ocr
                    .skip_recognition_for_version(path, size, mtime_ns)
                    .map_err(|error| error.to_string())?
                {
                    return complete_empty_ocr(path, context, ocr, Some(&metadata));
                }
            }
            let read_limit = pdf_ocr
                .map(OcrEngine::max_image_bytes)
                .unwrap_or(context.max_file_bytes);
            if read_limit != 0 && metadata.len() > read_limit {
                if let Some(ocr) = context.ocr {
                    ocr.forget_deferred(path);
                }
                return Ok(0);
            }
            let bytes = match fs::read(path) {
                Ok(bytes) => bytes,
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::PermissionDenied | ErrorKind::NotFound
                    ) =>
                {
                    return Ok(0)
                }
                Err(error) => return Err(format!("{}: {error}", path.display())),
            };
            let extracted = extract_index_text(path, bytes);
            if let Some(ocr) = context
                .ocr
                .filter(|ocr| ocr.is_pdf(path) && ocr.should_ocr_pdf(extracted.as_deref()))
            {
                if ocr.is_mesh() && !context.dispatch_mesh {
                    ocr.defer_path(path).map_err(|error| error.to_string())?;
                    return Ok(0);
                }
                match ocr.extract_pdf(path) {
                    Ok(text) if !text.trim().is_empty() => text,
                    Ok(_) => match extracted {
                        Some(text) => text,
                        None => return complete_empty_ocr(path, context, ocr, Some(&metadata)),
                    },
                    Err(error) => {
                        tracing::warn!(path = %path.display(), error = %error, "OCR PDF fallback failed");
                        if ocr.is_mesh() {
                            // Never persist a thin PDF layer as complete after
                            // a temporary GPU admission/transport failure.
                            return Ok(0);
                        }
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
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::PermissionDenied | ErrorKind::NotFound
            ) =>
        {
            return Ok(0)
        }
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    let mut count = 0;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::PermissionDenied | ErrorKind::NotFound
                ) =>
            {
                continue
            }
            Err(error) => return Err(error.to_string()),
        };
        count += walk(&entry.path(), context, map, documents)?;
    }
    Ok(count)
}

pub(crate) fn metadata_mtime_ns(metadata: &fs::Metadata) -> u128 {
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

fn exact_glob_basenames(globs: &[String]) -> Option<BTreeSet<String>> {
    if globs.is_empty() || globs.len() > 16 {
        return None;
    }
    globs
        .iter()
        .map(|glob| {
            // Complex/escaped filename patterns keep the general reader. Never
            // narrow a union if even one branch cannot be conservatively derived.
            if glob.contains('\\') {
                return None;
            }
            let name = glob.rsplit('/').next()?;
            if name.is_empty() || name.contains(['*', '?', '[', ']', '{', '}']) {
                None
            } else {
                Some(name.to_owned())
            }
        })
        .collect()
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
    let matches = |candidate: &Path| {
        excludes.is_match(candidate)
            || excludes.is_match(candidate.join(".grepmesh-directory-probe"))
    };
    matches(relative)
        || root
            .file_name()
            .map(|name| PathBuf::from(name).join(relative))
            .as_deref()
            .is_some_and(matches)
}

#[cfg(test)]
mod tests {
    #[test]
    fn configured_source_bucket_preserves_guard_without_sibling_extension() {
        use super::*;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("home");
        let source = root.join("agents-projects/grepmesh/docs");
        let quiet_source = root.join("agents-projects/quiet-project/docs");
        let sibling = root.join("agents-projects/another-project/README.md");
        fs::create_dir_all(&source).unwrap();
        fs::create_dir_all(&quiet_source).unwrap();
        fs::create_dir_all(sibling.parent().unwrap()).unwrap();
        let guarded_file = source.join("canary.md");
        fs::write(&guarded_file, "GUARDED_SOURCE_BODY").unwrap();
        let index = PersistentIndex::open(temp.path().join("index.sqlite3")).unwrap();
        let roots = BTreeMap::from([("home".into(), vec![root.clone()])]);
        let config = IndexActivityConfig {
            activity_bucket_roots: vec![source.clone(), quiet_source.clone()],
            ..Default::default()
        };
        let parent_bucket = root.join("agents-projects");
        let now = unix_time_ms();
        let inherited_until = now.saturating_add(60_000);
        let mut activity = BTreeMap::from([(
            parent_bucket.clone(),
            DirectoryActivityState {
                window_started_ms: now,
                events: config.hot_event_threshold - 1,
                hot_until_ms: inherited_until,
                ..Default::default()
            },
        )]);
        let snapshot = Arc::new(RwLock::new(IndexSnapshot::default()));

        let reconcile_one = |path: &Path, activity: &mut BTreeMap<_, _>| {
            reconcile_event_paths(
                &BTreeSet::from([path.to_path_buf()]),
                &BTreeMap::from([(path.to_path_buf(), 1)]),
                &roots,
                &[],
                &config,
                activity,
                &snapshot,
                0,
                None,
                None,
                &index,
            );
        };

        reconcile_one(&guarded_file, &mut activity);
        let guarded_until = activity[&source].hot_until_ms;
        assert_eq!(guarded_until, inherited_until);
        assert!(activity[&source].metadata_only);
        let guarded_metadata = fs::metadata(&guarded_file).unwrap();
        let guarded_body = index
            .cached_body(
                &guarded_file,
                guarded_metadata.len(),
                metadata_mtime_ns(&guarded_metadata),
            )
            .unwrap()
            .unwrap();
        assert!(!guarded_body.contains("GUARDED_SOURCE_BODY"));

        fs::write(&sibling, "SIBLING_ACTIVITY").unwrap();
        reconcile_one(&sibling, &mut activity);
        assert!(activity[&parent_bucket].hot_until_ms > guarded_until);
        assert_eq!(activity[&source].hot_until_ms, guarded_until);
        assert!(!activity_bucket_is_hot(
            &activity,
            &source,
            guarded_until.saturating_add(1)
        ));

        activity.get_mut(&parent_bucket).unwrap().hot_until_ms = unix_time_ms() - 1;
        activity.get_mut(&parent_bucket).unwrap().events = 0;
        let quiet_file = quiet_source.join("source.md");
        fs::write(&quiet_file, "QUIET_SOURCE_BODY").unwrap();
        reconcile_one(&quiet_file, &mut activity);
        assert!(!activity[&quiet_source].metadata_only);
        let quiet_metadata = fs::metadata(&quiet_file).unwrap();
        let quiet_body = index
            .cached_body(
                &quiet_file,
                quiet_metadata.len(),
                metadata_mtime_ns(&quiet_metadata),
            )
            .unwrap()
            .unwrap();
        assert!(quiet_body.contains("QUIET_SOURCE_BODY"));
    }

    #[cfg(feature = "ocr")]
    #[cfg(feature = "ocr")]
    #[test]
    fn empty_ocr_disposition_is_atomic_versioned_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty-result.png");
        fs::write(&path, b"unchanged source").unwrap();
        let database = dir.path().join("index.sqlite3");
        let store = PersistentIndex::open(database.clone()).unwrap();
        store
            .replace_document(&path, "old searchable body")
            .unwrap();
        store.defer_ocr(&path).unwrap();
        let metadata = fs::metadata(&path).unwrap();
        let version = (metadata.len(), metadata_mtime_ns(&metadata));
        let connection = store.connection().unwrap();
        connection.execute_batch("CREATE TRIGGER empty_receipt_failure BEFORE INSERT ON grepmesh_ocr_empty_results BEGIN SELECT RAISE(ABORT,'controlled receipt failure'); END;").unwrap();
        assert!(store
            .remove_document_with_empty(&path, Some(version))
            .is_err());
        assert_eq!(store.load_pending_ocr().unwrap().0, vec![path.clone()]);
        assert_eq!(store.candidates("searchable").unwrap(), vec![path.clone()]);
        assert!(!store
            .ocr_terminal_empty(&path, version.0, version.1)
            .unwrap());
        connection
            .execute_batch("DROP TRIGGER empty_receipt_failure;")
            .unwrap();
        store
            .remove_document_with_empty(&path, Some(version))
            .unwrap();
        drop(connection);
        drop(store);
        let store = PersistentIndex::open(database).unwrap();
        assert!(store
            .ocr_terminal_empty(&path, version.0, version.1)
            .unwrap());
        assert!(!store
            .ocr_terminal_empty(&path, version.0, version.1 + 1)
            .unwrap());
        assert!(store.load_pending_ocr().unwrap().0.is_empty());
        assert!(store.candidates("searchable").unwrap().is_empty());
        assert_eq!(fs::read(&path).unwrap(), b"unchanged source");
    }

    #[test]
    fn ocr_failure_ledger_survives_reopen_and_resets_for_new_version() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("index.sqlite3");
        let path = dir.path().join("retained.png");
        fs::write(&path, b"preserved input").unwrap();
        let store = PersistentIndex::open(database.clone()).unwrap();
        assert_eq!(store.record_ocr_failures(&path, 14, 100, 2).unwrap(), 2);
        drop(store);
        let store = PersistentIndex::open(database).unwrap();
        assert_eq!(store.ocr_failure_count(&path, 14, 100).unwrap(), 2);
        assert_eq!(store.record_ocr_failures(&path, 14, 100, 1).unwrap(), 3);
        assert_eq!(store.record_ocr_failures(&path, 14, 100, 1).unwrap(), 4);
        assert_eq!(store.record_ocr_failures(&path, 14, 100, 5).unwrap(), 4);
        store.remove_document(&path).unwrap();
        assert_eq!(store.ocr_failure_count(&path, 14, 100).unwrap(), 4);
        assert_eq!(fs::read(&path).unwrap(), b"preserved input");
        assert_eq!(store.ocr_failure_count(&path, 14, 101).unwrap(), 0);
        assert_eq!(store.record_ocr_failures(&path, 14, 101, 1).unwrap(), 1);
        store
            .replace_indexed_document(&IndexedDocument {
                path: path.clone(),
                size: 14,
                mtime_ns: 101,
                body: "recognized".into(),
            })
            .unwrap();
        assert_eq!(store.ocr_failure_count(&path, 14, 101).unwrap(), 0);
    }

    #[cfg(feature = "ocr")]
    #[test]
    fn ocr_failure_budget_stops_dispatch_and_rescan_without_deleting_input() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("retained.png");
        fs::write(&path, b"original input").unwrap();
        let store = PersistentIndex::open(dir.path().join("index.sqlite3")).unwrap();
        let ocr = OcrEngine::new(OcrConfig {
            backend: "mesh".into(),
            ..Default::default()
        })
        .unwrap();
        ocr.attach_pending_store(store.clone()).unwrap();
        let metadata = fs::metadata(&path).unwrap();
        let size = metadata.len();
        let mtime_ns = metadata_mtime_ns(&metadata);
        // No available worker is a deferred transport/admission outcome, not a native failure.
        assert!(ocr.extract_image(&path).is_err());
        assert_eq!(store.ocr_failure_count(&path, size, mtime_ns).unwrap(), 0);
        store.record_ocr_failures(&path, size, mtime_ns, 4).unwrap();
        assert_eq!(ocr.extract_image(&path).unwrap(), "");
        let matcher = compile_excludes(&[]).unwrap();
        let (_, _, documents, _) = scan_directory_unit(
            &path,
            dir.path(),
            &matcher,
            0,
            false,
            None,
            Some(&ocr),
            Some(&store),
            None,
            false,
        )
        .unwrap();
        assert!(documents.is_empty());
        assert!(store.load_pending_ocr().unwrap().0.is_empty());
        assert_eq!(store.ocr_failure_count(&path, size, mtime_ns).unwrap(), 4);
        assert_eq!(fs::read(&path).unwrap(), b"original input");
        fs::write(&path, b"new larger generation").unwrap();
        let (_, _, documents, _) = scan_directory_unit(
            &path,
            dir.path(),
            &matcher,
            0,
            false,
            None,
            Some(&ocr),
            Some(&store),
            None,
            false,
        )
        .unwrap();
        assert!(documents.is_empty());
        assert_eq!(store.load_pending_ocr().unwrap().0, vec![path]);
    }

    #[test]
    fn empty_result_cannot_acknowledge_a_newer_file_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("changed.png");
        fs::write(&path, []).unwrap();
        let before = fs::metadata(&path).unwrap();
        let store = PersistentIndex::open(dir.path().join("index.sqlite3")).unwrap();
        let ocr = OcrEngine::new(OcrConfig {
            backend: "mesh".into(),
            ..Default::default()
        })
        .unwrap();
        ocr.attach_pending_store(store.clone()).unwrap();
        ocr.defer_path(&path).unwrap();
        fs::write(&path, "new input arrived during inference").unwrap();
        let matcher = compile_excludes(&[]).unwrap();
        let context = ScanContext {
            dispatch_mesh: true,
            root: dir.path(),
            root_device: None,
            excludes: &matcher,
            max_file_bytes: 0,
            build_candidates: false,
            stt: None,
            ocr: Some(&ocr),
            persistent: Some(&store),
            metadata_only: None,
        };
        complete_empty_ocr(&path, &context, &ocr, Some(&before)).unwrap();
        assert_eq!(store.load_pending_ocr().unwrap().0, vec![path.clone()]);
        assert!(ocr.is_deferred(&path));
        assert!(path.exists());
    }

    #[cfg(feature = "ocr")]
    #[test]
    fn in_flight_input_cannot_be_acknowledged_by_metadata_scan() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("active.png");
        fs::write(&path, "input").unwrap();
        let store = PersistentIndex::open(dir.path().join("index.sqlite3")).unwrap();
        let ocr = Arc::new(
            OcrEngine::new(OcrConfig {
                backend: "mesh".into(),
                ..Default::default()
            })
            .unwrap(),
        );
        ocr.attach_pending_store(store.clone()).unwrap();
        ocr.defer_path(&path).unwrap();
        let lease = ocr.claim_retry(&path).unwrap();
        assert!(ocr.claim_retry(&path).is_none());
        let matcher = compile_excludes(&[]).unwrap();
        let metadata = compile_excludes(&["**/*.png".into()]).unwrap();
        let (_, _, documents, _) = scan_directory_unit(
            &path,
            dir.path(),
            &matcher,
            0,
            false,
            None,
            Some(&ocr),
            Some(&store),
            Some(&metadata),
            false,
        )
        .unwrap();
        assert!(documents.is_empty());
        assert_eq!(store.load_pending_ocr().unwrap().0, vec![path.clone()]);
        drop(lease);
        assert!(!ocr.is_in_flight(&path));
    }

    #[test]
    fn mesh_discovery_queues_durably_and_excludes_never_enter_ocr() {
        let dir = tempfile::tempdir().unwrap();
        let image = dir.path().join("new.png");
        let ignored = dir.path().join("node_modules/ignored.png");
        let empty = dir.path().join("empty.png");
        fs::write(&empty, []).unwrap();
        fs::create_dir_all(ignored.parent().unwrap()).unwrap();
        fs::write(
            &image,
            "deliberately undecodable; discovery must not run OCR",
        )
        .unwrap();
        fs::write(&ignored, "excluded content must not reach OCR").unwrap();
        let index = PersistentIndex::open(dir.path().join("index.sqlite3")).unwrap();
        let ocr = OcrEngine::new(OcrConfig {
            backend: "mesh".into(),
            ..Default::default()
        })
        .unwrap();
        ocr.attach_pending_store(index.clone()).unwrap();
        let matcher = compile_excludes(&["**/node_modules/**".into()]).unwrap();
        for path in [&image, &ignored, &empty] {
            let (_, _, documents, _) = scan_directory_unit(
                path,
                dir.path(),
                &matcher,
                0,
                false,
                None,
                Some(&ocr),
                Some(&index),
                None,
                false,
            )
            .unwrap();
            assert!(documents.is_empty());
        }
        assert_eq!(index.load_pending_ocr().unwrap().0, vec![image.clone()]);
        assert!(!ocr.is_deferred(&ignored));
        assert!(!ocr.is_deferred(&empty));
        assert!(empty.exists());
        // An old excluded entry retires without any worker or content decoder.
        index
            .replace_document(&ignored, "obsolete indexed ignored text")
            .unwrap();
        ocr.defer_path(&ignored).unwrap();
        retry_ocr_path(
            &ignored,
            &BTreeMap::from([("local".into(), vec![dir.path().into()])]),
            &["**/node_modules/**".into()],
            &Default::default(),
            0,
            None,
            &ocr,
            &index,
        );
        assert!(!ocr.is_deferred(&ignored));
        assert!(index.cached_body(&ignored, 0, 0).unwrap().is_none());
        assert_eq!(index.load_pending_ocr().unwrap().0, vec![image]);
        assert!(ignored.exists());
    }

    #[test]
    fn energy_recovery_watch_filters_nested_excluded_subtrees() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for child in [
            "healthy/src",
            "healthy/node_modules/dependency",
            "healthy/.git/objects",
        ] {
            std::fs::create_dir_all(root.join(child)).unwrap();
        }
        let mut watched = std::collections::BTreeSet::new();
        let errors = super::register_watch_root(
            root,
            &super::compile_excludes(&["**/node_modules/**".into(), "**/.git/**".into()]).unwrap(),
            &mut |path| {
                if path == root {
                    return Err(notify::Error::generic("one recursive boundary"));
                }
                watched.insert(path.to_path_buf());
                Ok(())
            },
        );
        assert!(!errors.is_empty());
        assert!(
            watched.contains(&root.join("healthy/src")),
            "healthy nested directories need their own filtered watch"
        );
        assert!(!watched
            .iter()
            .any(|p| p.starts_with(root.join("healthy/node_modules"))
                || p.starts_with(root.join("healthy/.git"))));
    }

    #[test]
    fn energy_recovery_watch_error_keeps_healthy_siblings() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for name in ["healthy-a", "broken", "healthy-b", ".git"] {
            std::fs::create_dir(root.join(name)).unwrap();
        }
        let mut registered = std::collections::BTreeSet::new();
        let errors = super::register_watch_root(
            root,
            &super::compile_excludes(&["**/.git/**".into()]).unwrap(),
            &mut |path| {
                if path == root || path == root.join("broken") {
                    Err(notify::Error::io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "registration boundary",
                    ))
                    .add_path(path.to_path_buf()))
                } else {
                    registered.insert(path.to_path_buf());
                    Ok(())
                }
            },
        );
        assert!(registered.contains(&root.join("healthy-a")));
        assert!(registered.contains(&root.join("healthy-b")));
        assert!(!registered.contains(&root.join(".git")));
        assert!(!errors.is_empty());
    }

    #[test]
    fn energy_recovery_metadata_scan_waits_for_document_commit() {
        use super::*;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        std::fs::create_dir_all(root.join(".tmpbin")).unwrap();
        let path = root.join(".tmpbin/pending.png");
        std::fs::write(&path, b"metadata input, not an image").unwrap();
        let index = PersistentIndex::open(dir.path().join("index.sqlite")).unwrap();
        let ocr = OcrEngine::new(OcrConfig {
            backend: "mesh".into(),
            ..OcrConfig::default()
        })
        .unwrap();
        ocr.attach_pending_store(index.clone()).unwrap();
        ocr.defer_path(&path).unwrap();
        let matcher = compile_excludes(&[]).unwrap();
        let metadata = compile_excludes(&["**/.tmpbin/**".into()]).unwrap();
        let (_, _, documents, _) = scan_directory_unit(
            &path,
            &root,
            &matcher,
            1024,
            false,
            None,
            Some(&ocr),
            Some(&index),
            Some(&metadata),
            false,
        )
        .unwrap();
        assert!(
            ocr.is_deferred(&path),
            "scan must not acknowledge before the FTS commit"
        );
        assert_eq!(index.load_pending_ocr().unwrap().0, vec![path.clone()]);
        assert_eq!(documents.len(), 1);
        index.replace_indexed_document(&documents[0]).unwrap();
        ocr.forget_deferred(&path);
        assert!(index.load_pending_ocr().unwrap().0.is_empty());
        assert!(index
            .cached_body(&path, documents[0].size, documents[0].mtime_ns)
            .unwrap()
            .unwrap()
            .contains("modified_ns:"));
    }

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
    fn created_subtree_recovers_nested_files_but_directory_metadata_does_not_rescan() {
        let temp_root = Path::new(".tmp/index-tests");
        fs::create_dir_all(temp_root).unwrap();
        let fixture = tempfile::tempdir_in(temp_root).unwrap();
        let root = fixture.path().join("watched");
        fs::create_dir(&root).unwrap();
        let index = PersistentIndex::open(fixture.path().join("index.sqlite")).unwrap();
        let nested = root.join("new-subtree/nested");
        fs::create_dir_all(&nested).unwrap();
        let file = nested.join("missed-child-event.txt");
        fs::write(&file, "CREATED_SUBTREE_TOKEN").unwrap();
        let new_directory = fs::canonicalize(root.join("new-subtree")).unwrap();
        let roots = BTreeMap::from([("home".into(), vec![root.clone()])]);
        let activity_config = IndexActivityConfig::default();
        let mut activity = BTreeMap::new();
        assert!(new_subtree_event(EventKind::Create(CreateKind::Folder)));
        assert!(new_subtree_event(EventKind::Modify(ModifyKind::Name(
            RenameMode::To
        ))));
        reconcile_new_subtrees(
            &BTreeSet::from([new_directory.clone()]),
            &roots,
            &[],
            &activity_config,
            &activity,
            0,
            None,
            None,
            &index,
        );
        assert!(index
            .candidates("CREATED_SUBTREE_TOKEN")
            .unwrap()
            .contains(&file));

        let late_file = nested.join("ordinary-metadata-child.txt");
        fs::write(&late_file, "METADATA_GUARD_TOKEN").unwrap();
        let metadata_kind =
            EventKind::Modify(ModifyKind::Metadata(notify::event::MetadataKind::Any));
        assert!(!new_subtree_event(metadata_kind));
        let metadata_paths = BTreeSet::from([new_directory.clone()]);
        let event_counts = BTreeMap::from([(new_directory, 1)]);
        let snapshot = Arc::new(RwLock::new(IndexSnapshot::default()));
        reconcile_event_paths(
            &metadata_paths,
            &event_counts,
            &roots,
            &[],
            &activity_config,
            &mut activity,
            &snapshot,
            0,
            None,
            None,
            &index,
        );
        assert!(index.candidates("METADATA_GUARD_TOKEN").unwrap().is_empty());
    }

    // Fast unit: real SQLite rows, only local indexed-filter logic.
    // Expected <1s/max5s. Catches stale ignored rows and loss of PDF/OCR filters.
    #[test]
    fn indexed_ocr_filters_ignore_stale_packages_before_hit_limit() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let index = PersistentIndex::open(root.join("index.sqlite3")).unwrap();
        let ignored = root.join("Android/Sdk/stale.pdf");
        let document = root.join("documents/paper.pdf");
        for path in [ignored.clone(), document.clone()] {
            index
                .replace_indexed_document(&IndexedDocument {
                    path,
                    size: 1,
                    mtime_ns: 1,
                    body: "NATIVE_OCR_FILTER_MARKER".into(),
                })
                .unwrap();
        }
        let mut manager = IndexManager::disabled();
        manager.persistent = Some(index);
        manager.snapshot.write().unwrap().state = IndexState::Building;
        assert!(manager
            .search_text_hits_filtered(
                "NATIVE_OCR_FILTER_MARKER",
                root,
                1,
                0,
                true,
                &["**/*.pdf".into()],
                &["**/Android/Sdk/**".into()]
            )
            .is_none());
        manager
            .persistent
            .as_ref()
            .unwrap()
            .mark_full_rebuild_ms(unix_time_ms())
            .unwrap();
        let hits = manager
            .search_text_hits_filtered(
                "NATIVE_OCR_FILTER_MARKER",
                root,
                1,
                0,
                true,
                &["**/*.pdf".into()],
                &["**/Android/Sdk/**".into()],
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].path, document);
        assert!(manager
            .search_text_hits_filtered(
                "NATIVE_OCR_FILTER_MARKER",
                root,
                10,
                0,
                true,
                &["**/*.pdf".into(), "!**/documents/**".into()],
                &["**/Android/Sdk/**".into()]
            )
            .unwrap()
            .is_empty());
    }

    #[cfg(feature = "ocr")]
    #[test]
    fn pending_ocr_status_preserves_underlying_walk_failure() {
        let mut manager = IndexManager::disabled();
        let config = OcrConfig {
            backend: "mesh".into(),
            ..Default::default()
        };
        let ocr = OcrEngine::new(config).unwrap();
        ocr.defer_path(Path::new(".tmp/pending-status-input.png"))
            .unwrap();
        manager.ocr = Some(Arc::new(ocr));
        manager.snapshot.write().unwrap().last_error = Some("underlying walk failure".into());
        let status = manager.status();
        assert_eq!(status.state, IndexState::Degraded);
        assert_eq!(
            status.last_error.as_deref(),
            Some("underlying walk failure; 1 OCR inputs await an available OCR worker")
        );
        assert_eq!(
            manager.snapshot.read().unwrap().last_error.as_deref(),
            Some("underlying walk failure")
        );
    }

    #[test]
    fn disappearing_enumerated_child_does_not_abort_remaining_directory_units() {
        let temp_root = Path::new(".tmp/index-tests");
        fs::create_dir_all(temp_root).unwrap();
        let root = tempfile::tempdir_in(temp_root).unwrap();
        let disappearing = root.path().join("a-transient.txt");
        let surviving = root.path().join("z-surviving.txt");
        fs::write(&disappearing, "temporary generated content").unwrap();
        fs::write(&surviving, "SURVIVING_INDEX_TOKEN").unwrap();
        let units = root_units(root.path()).unwrap();
        assert!(units.contains(&disappearing));
        fs::remove_file(&disappearing).unwrap();
        let excludes = compile_excludes(&[]).unwrap();
        let mut count = 0;
        let mut documents = Vec::new();
        for unit in units {
            let (unit_count, _, unit_documents, children) = scan_directory_unit(
                &unit,
                root.path(),
                &excludes,
                0,
                true,
                None,
                None,
                None,
                None,
                false,
            )
            .unwrap();
            count += unit_count;
            documents.extend(unit_documents);
            assert!(children.is_empty());
        }
        assert_eq!(count, 1);
        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0].path, surviving);
        assert!(documents[0].body.contains("SURVIVING_INDEX_TOKEN"));
        assert!(root_units(&root.path().join("missing-configured-root")).is_err());
    }

    #[test]
    fn durable_ocr_pending_reopens_and_acks_with_fts_commit() {
        let temp_root = Path::new(".tmp/index-tests");
        fs::create_dir_all(temp_root).unwrap();
        let dir = tempfile::tempdir_in(temp_root).unwrap();
        let database = dir.path().join("index.sqlite");
        let path = dir.path().join("scan.png");
        fs::write(&path, "controlled input; no inference is run").unwrap();
        {
            let index = PersistentIndex::open(database.clone()).unwrap();
            index
                .replace_document(&path, "previous durable OCR token")
                .unwrap();
            index.defer_ocr(&path).unwrap();
            index.set_ocr_overflow(true).unwrap();
        }
        let index = PersistentIndex::open(database).unwrap();
        assert_eq!(
            index.load_pending_ocr().unwrap(),
            (vec![path.clone()], true)
        );
        let connection = index.connection().unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER durable_ocr_ack_failure BEFORE DELETE ON grepmesh_pending_ocr \
             BEGIN SELECT RAISE(ABORT, 'controlled pending ACK failure'); END;",
            )
            .unwrap();
        let error = index
            .replace_document(&path, "replacement durable OCR token")
            .unwrap_err();
        assert!(error.contains("controlled pending ACK failure"));
        assert_eq!(
            index.load_pending_ocr().unwrap(),
            (vec![path.clone()], true)
        );
        assert_eq!(index.candidates("previous").unwrap(), vec![path.clone()]);
        assert!(index.candidates("replacement").unwrap().is_empty());
        connection
            .execute_batch("DROP TRIGGER durable_ocr_ack_failure;")
            .unwrap();
        index
            .replace_document(&path, "replacement durable OCR token")
            .unwrap();
        assert_eq!(index.load_pending_ocr().unwrap(), (Vec::new(), true));
        assert!(index.candidates("previous").unwrap().is_empty());
        assert_eq!(index.candidates("replacement").unwrap(), vec![path.clone()]);
        index.set_ocr_overflow(false).unwrap();
        index.defer_ocr(&path).unwrap();
        // An unchanged cached document still needs to commit its pending ACK.
        index
            .replace_document(&path, "replacement durable OCR token")
            .unwrap();
        assert_eq!(index.load_pending_ocr().unwrap(), (Vec::new(), false));
        assert!(index.ocr_recovery_full_scan_required().unwrap());
        index.mark_full_rebuild_ms(unix_time_ms()).unwrap();
        assert!(!index.ocr_recovery_full_scan_required().unwrap());
    }

    #[cfg(feature = "ocr")]
    #[test]
    fn empty_mesh_ocr_ack_waits_for_sqlite_commit() {
        let temp_root = Path::new(".tmp/index-tests");
        fs::create_dir_all(temp_root).unwrap();
        let dir = tempfile::tempdir_in(temp_root).unwrap();
        let path = dir.path().join("scan.png");
        fs::write(&path, "controlled input; no inference is run").unwrap();
        let index = PersistentIndex::open(dir.path().join("index.sqlite")).unwrap();
        index
            .replace_document(&path, "previous searchable OCR token")
            .unwrap();
        let metadata = fs::metadata(&path).unwrap();
        let ocr = OcrEngine::new(OcrConfig {
            backend: "mesh".into(),
            ..OcrConfig::default()
        })
        .unwrap();
        ocr.defer_path(&path).unwrap();
        let excludes = GlobSetBuilder::new().build().unwrap();
        let context = ScanContext {
            dispatch_mesh: false,
            root: dir.path(),
            root_device: None,
            excludes: &excludes,
            max_file_bytes: 0,
            build_candidates: false,
            stt: None,
            ocr: Some(&ocr),
            persistent: Some(&index),
            metadata_only: None,
        };
        let connection = index.connection().unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER mesh_ocr_ack_fail_delete \
             BEFORE DELETE ON grepmesh_extraction_cache \
             BEGIN SELECT RAISE(ABORT, 'controlled SQLite failure'); END;",
            )
            .unwrap();

        let error = complete_empty_ocr(&path, &context, &ocr, None).unwrap_err();
        assert!(error.contains("controlled SQLite failure"));
        assert!(
            ocr.is_deferred(&path),
            "a rolled-back deletion must retain its retry"
        );
        assert_eq!(index.candidates("previous").unwrap(), vec![path.clone()]);
        assert_eq!(
            index
                .cached_body(&path, metadata.len(), metadata_mtime_ns(&metadata))
                .unwrap()
                .as_deref(),
            Some("previous searchable OCR token")
        );

        connection
            .execute_batch("DROP TRIGGER mesh_ocr_ack_fail_delete;")
            .unwrap();
        assert_eq!(complete_empty_ocr(&path, &context, &ocr, None).unwrap(), 0);
        assert!(
            !ocr.is_deferred(&path),
            "only a committed deletion acknowledges completion"
        );
        assert_eq!(ocr.deferred_count(), 0);
        assert!(index.candidates("previous").unwrap().is_empty());
        assert!(index
            .cached_body(&path, metadata.len(), metadata_mtime_ns(&metadata))
            .unwrap()
            .is_none());
        assert_eq!(index.document_count().unwrap(), 0);
    }

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
    fn root_named_policy_matches_contents_of_a_separate_root() {
        let downloads = PathBuf::from("/Users/user/Downloads");
        let desktop = PathBuf::from("/Users/user/Desktop");
        let matcher = compile_excludes(&["Downloads/**".to_string()]).unwrap();
        assert!(excluded(&downloads.join("image.png"), &downloads, &matcher));
        assert!(excluded(&downloads.join("archive"), &downloads, &matcher));
        assert!(!excluded(&desktop.join("image.png"), &desktop, &matcher));
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
        let connection = index
            .read_connection(Instant::now() + Duration::from_secs(2))
            .unwrap();
        let mut matches = Vec::new();
        index
            .visit_matching_documents(
                &connection,
                "roadmap",
                dir.path(),
                Instant::now() + Duration::from_secs(2),
                |_| true,
                |path, body| {
                    matches.push((path, body));
                    true
                },
            )
            .unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].0, document);
    }

    #[test]
    fn exact_filename_reads_cached_pdf_before_global_fts_postings() {
        // Focused SQLite integration, expected <2s/max5s. Count real VM work
        // instead of relying on a machine-dependent wall-clock threshold.
        let dir = tempfile::tempdir().unwrap();
        let store = PersistentIndex::open(dir.path().join("index.sqlite")).unwrap();
        let pdf = dir.path().join("wanted_%(1).pdf");
        fs::write(&pdf, b"%PDF\0binary without searchable text").unwrap();
        store.replace_document(&pdf, "needle cached PDF").unwrap();
        for n in 0..500 {
            store
                .replace_document(
                    &dir.path().join(format!("unrelated-{n}.txt")),
                    "needle unrelated",
                )
                .unwrap();
        }
        let read = |globs: &[String]| {
            let deadline = Instant::now() + Duration::from_secs(5);
            let connection = store.read_connection(deadline).unwrap();
            let steps = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let observed = steps.clone();
            connection.progress_handler(
                1,
                Some(move || {
                    observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    false
                }),
            );
            let mut found = Vec::new();
            store
                .visit_matching_documents_with_paths(
                    &connection,
                    "needle",
                    dir.path(),
                    globs,
                    deadline,
                    |path| path == pdf,
                    |path, body| {
                        found.push((path, body));
                        true
                    },
                )
                .unwrap();
            (found, steps.load(std::sync::atomic::Ordering::Relaxed))
        };
        let baseline = read(&[]);
        let scoped = read(&["**/wanted_%(1).pdf".into()]);
        assert_eq!(scoped.0, vec![(pdf, "needle cached PDF".into())]);
        assert_eq!(baseline.0, scoped.0);
        assert!(
            scoped.1 < baseline.1 / 2,
            "scoped/global steps: {}/{}",
            scoped.1,
            baseline.1
        );
        assert!(exact_glob_basenames(&["**/*.pdf".into()]).is_none());
        assert!(exact_glob_basenames(&["**/wanted.pdf".into(), "**/*.pdf".into()]).is_none());
    }

    #[test]
    fn indexed_scope_filters_unrelated_bodies_before_decoding() {
        // Fast unit. A scoped OCR/PDF read must not load an unrelated body.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("selected");
        let pdf = root.join("scan.pdf");
        let unrelated = dir.path().join("outside.png");
        let persistent = PersistentIndex::open(dir.path().join("index.sqlite")).unwrap();
        persistent
            .replace_document(&pdf, "cached OCR needle from PDF")
            .unwrap();
        persistent
            .replace_document(&unrelated, "unrelated needle")
            .unwrap();
        // SQLite permits a non-text value; outside-scope data must be irrelevant.
        persistent
            .connection()
            .unwrap()
            .execute(
                "UPDATE grepmesh_documents SET body=CAST(body AS BLOB) WHERE path=?1",
                params![unrelated.display().to_string()],
            )
            .unwrap();
        let mut index = IndexManager::disabled();
        index.ready_roots.write().unwrap().insert(root.clone());
        index.persistent = Some(persistent);
        let hits = index
            .search_text_hits_filtered("needle", &root, 3, 0, true, &["**/*.pdf".to_string()], &[])
            .expect("scoped cached PDF remains searchable");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].path, pdf);
    }

    #[test]
    fn indexed_scope_keeps_pdf_image_text_and_component_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("scope_%");
        let store = PersistentIndex::open(dir.path().join("index.sqlite")).unwrap();
        let pdf = root.join("cached.pdf");
        let image = root.join("cached.png");
        store
            .replace_document(&pdf, "PDF needle cached extraction")
            .unwrap();
        store
            .replace_document(&image, "OCR needle cached extraction")
            .unwrap();
        store
            .replace_document(
                &dir.path().join("scope_%other/outside.pdf"),
                "needle outside",
            )
            .unwrap();
        let mut index = IndexManager::disabled();
        index.ready_roots.write().unwrap().insert(root.clone());
        index.persistent = Some(store);
        let hits = index
            .search_text_hits_filtered("needle", &root, 10, 0, true, &[], &[])
            .unwrap();
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|h| h.path == pdf));
        assert!(hits.iter().any(|h| h.path == image));
        let only_pdf = index
            .search_text_hits_filtered("needle", &root, 10, 0, true, &["**/*.pdf".into()], &[])
            .unwrap();
        assert_eq!(only_pdf.len(), 1);
        assert_eq!(only_pdf[0].path, pdf);
    }

    #[test]
    fn indexed_deadline_and_result_limit_are_explicit() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = PersistentIndex::open(root.join("index.sqlite")).unwrap();
        store
            .replace_document(&root.join("cached.pdf"), "needle first\nneedle second")
            .unwrap();
        let mut index = IndexManager::disabled();
        index
            .ready_roots
            .write()
            .unwrap()
            .insert(root.to_path_buf());
        index.persistent = Some(store);
        assert!(index
            .search_text_hits_filtered_until(
                "needle",
                root,
                5,
                0,
                true,
                &[],
                &[],
                Instant::now() - Duration::from_secs(1)
            )
            .is_err());
        let page = index
            .search_text_hits_filtered_until(
                "needle",
                root,
                1,
                0,
                true,
                &[],
                &[],
                Instant::now() + Duration::from_secs(2),
            )
            .unwrap()
            .unwrap();
        assert_eq!(page.hits.len(), 1);
        assert!(page.truncated);
    }

    #[test]
    fn indexed_reader_uses_one_readonly_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = PersistentIndex::open(root.join("index.sqlite")).unwrap();
        let pdf = root.join("scan.pdf");
        store.replace_document(&pdf, "needle initial").unwrap();
        let reader = store
            .read_connection(Instant::now() + Duration::from_secs(2))
            .unwrap();
        let _: i64 = reader
            .query_row("SELECT count(*) FROM grepmesh_document_rows", [], |r| {
                r.get(0)
            })
            .unwrap();
        store.replace_document(&pdf, "needle changed").unwrap();
        let mut bodies = Vec::new();
        store
            .visit_matching_documents(
                &reader,
                "needle",
                root,
                Instant::now() + Duration::from_secs(2),
                |_| true,
                |_, body| {
                    bodies.push(body);
                    true
                },
            )
            .unwrap();
        assert_eq!(bodies, vec!["needle initial"]);
        assert!(reader.execute("DELETE FROM grepmesh_metadata", []).is_err());
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
    fn unchanged_document_repairs_missing_or_stale_fts_and_cache() {
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
    fn document_row_migration_deduplicates_legacy_fts_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.sqlite");
        let document = dir.path().join("document.txt");
        fs::write(&document, "deduplicated searchable token").unwrap();
        {
            let index = PersistentIndex::open(db.clone()).unwrap();
            index
                .replace_document(&document, "deduplicated searchable token")
                .unwrap();
            let connection = index.connection().unwrap();
            connection
                .execute(
                    "INSERT INTO grepmesh_documents(path, body) \
                     VALUES (?1, 'legacy duplicate body')",
                    params![document.display().to_string()],
                )
                .unwrap();
            connection
                .execute(
                    "DELETE FROM grepmesh_metadata WHERE key='document_rows_v1'",
                    [],
                )
                .unwrap();
        }

        let index = PersistentIndex::open(db).unwrap();
        index
            .replace_document(&document, "deduplicated searchable token")
            .unwrap();
        assert_eq!(
            index.candidates("deduplicated").unwrap(),
            vec![document.clone()]
        );
        assert!(index.candidates("legacy duplicate").unwrap().is_empty());
        let count: i64 = index
            .connection()
            .unwrap()
            .query_row(
                "SELECT count(*) FROM grepmesh_documents WHERE path=?1",
                params![document.display().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn unchanged_validation_uses_fts_rowid_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let index = PersistentIndex::open(dir.path().join("index.sqlite")).unwrap();
        let plan = index
            .connection()
            .unwrap()
            .prepare(
                "EXPLAIN QUERY PLAN \
                 SELECT EXISTS(SELECT 1 FROM grepmesh_documents \
                 WHERE rowid=?1 AND path=?2 AND body=?3)",
            )
            .unwrap()
            .query_map(params![1_i64, "/document.txt", "body"], |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            plan.iter().any(|detail| detail.ends_with("INDEX 0:=")),
            "FTS lookup must constrain rowid instead of scanning every document: {plan:?}"
        );
    }

    #[test]
    fn rollback_writer_invalidates_stale_row_mapping() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.sqlite");
        let first = dir.path().join("first.txt");
        let second = dir.path().join("second.txt");
        fs::write(&first, "first original token").unwrap();
        fs::write(&second, "second preserved token").unwrap();
        let index = PersistentIndex::open(db.clone()).unwrap();
        index
            .replace_document(&first, "first original token")
            .unwrap();
        index
            .replace_document(&second, "second preserved token")
            .unwrap();

        fs::write(&first, "first rollback replacement").unwrap();
        let metadata = fs::metadata(&first).unwrap();
        let path = first.display().to_string();
        let mut connection = index.connection().unwrap();
        let transaction = connection.transaction().unwrap();
        // Simulate the parent binary: it rewrites FTS and cache but knows
        // nothing about grepmesh_document_rows.
        transaction
            .execute(
                "DELETE FROM grepmesh_documents WHERE path=?1",
                params![path],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO grepmesh_documents(path, body) VALUES (?1, ?2)",
                params![path, "first rollback replacement"],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO grepmesh_extraction_cache(path, size, mtime_ns, body) \
                 VALUES (?1, ?2, ?3, ?4) ON CONFLICT(path) DO UPDATE SET \
                 size=excluded.size, mtime_ns=excluded.mtime_ns, body=excluded.body",
                params![
                    path,
                    metadata.len() as i64,
                    metadata_mtime_ns(&metadata).to_string(),
                    "first rollback replacement"
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM grepmesh_document_rows WHERE path=?1",
                    params![path],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0,
            "legacy cache writes must invalidate rowids they cannot maintain"
        );
        assert!(connection
            .query_row(
                "SELECT value FROM grepmesh_metadata WHERE key='document_rows_v1'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .unwrap()
            .is_none());
        drop(connection);
        drop(index);

        let reopened = PersistentIndex::open(db).unwrap();
        reopened
            .replace_document(&first, "first rollback replacement")
            .unwrap();
        assert_eq!(
            reopened.candidates("rollback replacement").unwrap(),
            vec![first]
        );
        assert_eq!(
            reopened.candidates("second preserved").unwrap(),
            vec![second]
        );
    }

    #[test]
    fn rollback_writer_rowid_reuse_is_reconciled_before_prune() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.sqlite");
        let first = dir.path().join("first.txt");
        let second = dir.path().join("second.txt");
        let removed = dir.path().join("removed.txt");
        let replacement = dir.path().join("replacement.txt");
        fs::write(&first, "first keep token").unwrap();
        fs::write(&second, "second keep token").unwrap();
        fs::write(&removed, "removed legacy token").unwrap();
        let index = PersistentIndex::open(db.clone()).unwrap();
        index.replace_document(&first, "first keep token").unwrap();
        index
            .replace_document(&second, "second keep token")
            .unwrap();
        index
            .replace_document(&removed, "removed legacy token")
            .unwrap();

        fs::remove_file(&removed).unwrap();
        fs::write(&replacement, "replacement rollback token").unwrap();
        let replacement_metadata = fs::metadata(&replacement).unwrap();
        let removed_path = removed.display().to_string();
        let replacement_path = replacement.display().to_string();
        let mut connection = index.connection().unwrap();
        let removed_rowid: i64 = connection
            .query_row(
                "SELECT rowid FROM grepmesh_documents WHERE path=?1",
                params![removed_path],
                |row| row.get(0),
            )
            .unwrap();
        let transaction = connection.transaction().unwrap();
        // Simulate a rolled-back parent binary deleting its highest row and
        // inserting a different path. SQLite is allowed to reuse that rowid.
        transaction
            .execute(
                "DELETE FROM grepmesh_documents WHERE path=?1",
                params![removed_path],
            )
            .unwrap();
        transaction
            .execute(
                "DELETE FROM grepmesh_extraction_cache WHERE path=?1",
                params![removed_path],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO grepmesh_documents(path, body) VALUES (?1, ?2)",
                params![replacement_path, "replacement rollback token"],
            )
            .unwrap();
        assert_eq!(transaction.last_insert_rowid(), removed_rowid);
        transaction
            .execute(
                "INSERT INTO grepmesh_extraction_cache(path, size, mtime_ns, body) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    replacement_path,
                    replacement_metadata.len() as i64,
                    metadata_mtime_ns(&replacement_metadata).to_string(),
                    "replacement rollback token"
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
        drop(connection);
        drop(index);

        let reopened = PersistentIndex::open(db).unwrap();
        assert_eq!(reopened.document_count().unwrap(), 3);
        assert!(reopened.candidates("removed legacy").unwrap().is_empty());
        assert_eq!(
            reopened.candidates("replacement rollback").unwrap(),
            vec![replacement.clone()]
        );
        reopened
            .prune_except(&BTreeSet::from([first.clone(), second.clone()]))
            .unwrap();
        assert!(reopened
            .candidates("replacement rollback")
            .unwrap()
            .is_empty());
        assert_eq!(reopened.document_count().unwrap(), 2);
        assert_eq!(reopened.candidates("first keep").unwrap(), vec![first]);
        assert_eq!(reopened.candidates("second keep").unwrap(), vec![second]);
    }

    #[test]
    fn first_trigger_install_remigrates_a_legacy_v1_marker() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.sqlite");
        let old_path = dir.path().join("old.txt");
        let new_path = dir.path().join("new.txt");
        let index = PersistentIndex::open(db.clone()).unwrap();
        index.replace_document(&old_path, "old token").unwrap();
        let mut connection = index.connection().unwrap();
        connection
            .execute_batch(
                "DROP TRIGGER grepmesh_cache_rows_after_insert; \
                 DROP TRIGGER grepmesh_cache_rows_after_update; \
                 DROP TRIGGER grepmesh_cache_rows_after_delete; \
                 DELETE FROM grepmesh_metadata WHERE key='document_rows_triggers_v1';",
            )
            .unwrap();
        let old_rowid: i64 = connection
            .query_row(
                "SELECT rowid FROM grepmesh_documents WHERE path=?1",
                params![old_path.display().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        let transaction = connection.transaction().unwrap();
        transaction
            .execute(
                "DELETE FROM grepmesh_documents WHERE path=?1",
                params![old_path.display().to_string()],
            )
            .unwrap();
        transaction
            .execute(
                "DELETE FROM grepmesh_extraction_cache WHERE path=?1",
                params![old_path.display().to_string()],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO grepmesh_documents(path, body) VALUES (?1, ?2)",
                params![new_path.display().to_string(), "new token"],
            )
            .unwrap();
        assert_eq!(transaction.last_insert_rowid(), old_rowid);
        transaction
            .execute(
                "INSERT INTO grepmesh_extraction_cache(path, size, mtime_ns, body) \
                 VALUES (?1, 0, '0', ?2)",
                params![new_path.display().to_string(), "new token"],
            )
            .unwrap();
        transaction.commit().unwrap();
        drop(connection);
        drop(index);

        let reopened = PersistentIndex::open(db).unwrap();
        assert!(reopened.candidates("old token").unwrap().is_empty());
        assert_eq!(reopened.candidates("new token").unwrap(), vec![new_path]);
        assert_eq!(reopened.document_count().unwrap(), 1);
        assert_eq!(
            reopened
                .connection()
                .unwrap()
                .query_row(
                    "SELECT value FROM grepmesh_metadata \
                     WHERE key='document_rows_triggers_v1'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "1"
        );
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
