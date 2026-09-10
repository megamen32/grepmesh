//! End-to-end adapter tests against a fixture UserIO SQLite store: render,
//! incremental update, removal, attachment materialization, and proof that a
//! userio cache directory behaves like any other indexed root (FTS finds the
//! rendered conversation text through the regular IndexManager pipeline).

use grepmesh::config::{
    AppConfig, IndexActivityConfig, OcrConfig, SttConfig, UserioAttachmentsConfig, UserioConfig,
};
use grepmesh::index::IndexManager;
use grepmesh::userio;
use rusqlite::Connection;
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tempfile::TempDir;

const USER: &str = "user_owner";

fn userio_schema(connection: &Connection) {
    connection
        .execute_batch(
            "CREATE TABLE conversations (
                user_id TEXT NOT NULL,id TEXT NOT NULL,conversation_key TEXT NOT NULL,
                route_id TEXT NOT NULL,source TEXT NOT NULL,sender TEXT NOT NULL,
                identity_id TEXT,response_mode TEXT NOT NULL DEFAULT 'approve',
                updated_at REAL NOT NULL, account_ref TEXT NOT NULL DEFAULT '',
                PRIMARY KEY(user_id,id),UNIQUE(user_id,conversation_key));
             CREATE TABLE messages (
                user_id TEXT NOT NULL,source TEXT NOT NULL,message_id TEXT NOT NULL,
                conversation_id TEXT NOT NULL,sender TEXT NOT NULL,body TEXT NOT NULL,
                received_at REAL NOT NULL,seen_at REAL,
                PRIMARY KEY(user_id,source,message_id));
             CREATE TABLE message_attachments (
                user_id TEXT NOT NULL,source TEXT NOT NULL,message_id TEXT NOT NULL,
                idx INTEGER NOT NULL,kind TEXT NOT NULL,content_type TEXT NOT NULL,
                filename TEXT NOT NULL,size INTEGER,src TEXT,
                attachment_id TEXT,provider_ref TEXT, transcript TEXT,
                transcription_status TEXT, transcription_model TEXT,
                PRIMARY KEY(user_id,source,message_id,idx));
             CREATE TABLE contact_names (
                user_id TEXT NOT NULL,source TEXT NOT NULL,sender TEXT NOT NULL,
                name TEXT NOT NULL,updated_at REAL NOT NULL,
                PRIMARY KEY(user_id,source,sender));",
        )
        .expect("create userio fixture schema");
}

fn seed_fixture(connection: &Connection) {
    connection
        .execute_batch(&format!(
            "INSERT INTO conversations (user_id,id,conversation_key,route_id,source,sender,updated_at,account_ref) VALUES
               ('{USER}','conv_t1','telegram|540308572','telegram-read','telegram','540308572',1788971520,'telegram:8810909089'),
               ('{USER}','conv_g1','gmail|noreply','gmail-read','gmail','noreply@example.com',1788971500,'');
             INSERT INTO messages (user_id,source,message_id,conversation_id,sender,body,received_at) VALUES
               ('{USER}','telegram','540308572:1','conv_t1','540308572','GREPMESH-USERIO-TEST telegram plain text',1788971500),
               ('{USER}','telegram','540308572:2','conv_t1','540308572','',1788971510),
               ('{USER}','gmail','g1','conv_g1','noreply@example.com','<html><body><p>Gmail GREPMESH-USERIO-TEST body &amp; more</p></body></html>',1788971490);
             INSERT INTO message_attachments (user_id,source,message_id,idx,kind,content_type,filename,size,transcript,transcription_status) VALUES
               ('{USER}','telegram','540308572:2',0,'voice','audio/ogg','telegram-2.ogg',1234,'Голосовое GREPMESH-USERIO-TEST transcript','completed');
             INSERT INTO contact_names (user_id,source,sender,name,updated_at) VALUES
               ('{USER}','telegram','540308572','Никита Р',1788970000);"
        ))
        .expect("seed fixture rows");
}

fn fixture_db(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("userio.sqlite3");
    let connection = Connection::open(&path).expect("open fixture db");
    userio_schema(&connection);
    seed_fixture(&connection);
    path
}

fn test_config(sqlite: &Path, cache: &Path) -> UserioConfig {
    UserioConfig {
        enabled: true,
        sqlite_path: sqlite.to_path_buf(),
        cache_dir: cache.to_path_buf(),
        poll_interval_ms: 1_000,
        user_ids: Vec::new(),
        max_writes_per_sync: 40,
        include_sources: Vec::new(),
        attachments: UserioAttachmentsConfig::default(),
        api_base: "http://127.0.0.1:9".to_string(),
        token_env: "GREPMESH_USERIO_TOKEN".to_string(),
        max_download_attempts: 2,
    }
}

fn read_cache(cache: &Path, source: &str, conversation: &str) -> String {
    fs::read_to_string(
        cache
            .join("conversations")
            .join(source)
            .join(format!("{conversation}.txt")),
    )
    .unwrap_or_else(|error| panic!("read {conversation}: {error}"))
}

#[test]
fn renders_updates_and_prunes_conversations() {
    let dir = TempDir::new().unwrap();
    let sqlite = fixture_db(&dir);
    let cache = dir.path().join("cache");
    let config = test_config(&sqlite, &cache);
    let mut downloader = offline_downloader(&config);

    let report = userio::sync_once(&config, &mut downloader).expect("first sync");
    assert_eq!(report.conversations, 2);
    assert_eq!(report.written, 2);

    let telegram = read_cache(&cache, "telegram", "conv_t1");
    assert!(telegram.contains("GREPMESH-USERIO-TEST telegram plain text"));
    assert!(telegram.contains("[attachment kind=voice name=telegram-2.ogg"));
    assert!(telegram.contains("transcript: Голосовое GREPMESH-USERIO-TEST transcript"));
    assert!(
        telegram.contains("Никита Р <540308572>"),
        "contact name should be resolved"
    );

    let mail = read_cache(&cache, "gmail", "conv_g1");
    assert!(mail.contains("Gmail GREPMESH-USERIO-TEST body & more"));
    assert!(!mail.contains("<p>"), "html tags must be stripped");

    // A no-op poll must not bump any file.
    let report = userio::sync_once(&config, &mut downloader).expect("second sync");
    assert_eq!(report.unchanged, 2);
    assert_eq!(report.written, 0);
    assert_eq!(report.removed, 0);

    // New message: only that conversation is rewritten.
    Connection::open(&sqlite)
        .unwrap()
        .execute(
            "INSERT INTO messages (user_id,source,message_id,conversation_id,sender,body,received_at)
             VALUES (?1,'telegram','540308572:3','conv_t1','540308572','GREPMESH-USERIO-TEST second arrival',1788971600)",
            [USER],
        )
        .unwrap();
    let report = userio::sync_once(&config, &mut downloader).expect("third sync");
    assert_eq!(report.written, 1);
    assert!(read_cache(&cache, "telegram", "conv_t1").contains("second arrival"));

    // Deleted conversation: its cache file is pruned.
    Connection::open(&sqlite)
        .unwrap()
        .execute("DELETE FROM conversations WHERE id='conv_g1'", [])
        .unwrap();
    Connection::open(&sqlite)
        .unwrap()
        .execute("DELETE FROM messages WHERE conversation_id='conv_g1'", [])
        .unwrap();
    let report = userio::sync_once(&config, &mut downloader).expect("fourth sync");
    assert!(report.removed >= 1);
    assert!(!cache.join("conversations/gmail/conv_g1.txt").exists());
    assert!(cache.join("conversations/telegram/conv_t1.txt").exists());
}

fn offline_downloader(config: &UserioConfig) -> userio::Downloader {
    // No GREPMESH_USERIO_TOKEN in the test environment, so any attempted
    // download fails fast and locally instead of hitting the network.
    std::env::remove_var(&config.token_env);
    userio::Downloader::from_config(config)
}

#[test]
fn materializes_local_doc_attachments_and_respects_stage_flags() {
    let dir = TempDir::new().unwrap();
    let sqlite = fixture_db(&dir);
    let cache = dir.path().join("cache");
    let doc_source = dir.path().join("report.txt");
    fs::write(&doc_source, "DOC GREPMESH-USERIO-TEST payload").unwrap();

    let connection = Connection::open(&sqlite).unwrap();
    connection
        .execute(
            "INSERT INTO message_attachments (user_id,source,message_id,idx,kind,content_type,filename,size,src)
             VALUES (?1,'telegram','540308572:1',0,'document','text/plain','report.txt',27,?2)",
            rusqlite::params![USER, doc_source.display().to_string()],
        )
        .unwrap();

    let mut config = test_config(&sqlite, &cache);
    config.attachments.docs = true;
    config.attachments.media = false;
    let mut downloader = offline_downloader(&config);

    let report = userio::sync_once(&config, &mut downloader).expect("sync with docs stage");
    assert_eq!(
        report.attachments_materialized, 1,
        "doc attachment should be materialized"
    );
    let attachments_dir = cache.join("attachments/telegram");
    let written: Vec<PathBuf> = fs::read_dir(&attachments_dir)
        .unwrap_or_else(|error| panic!("read attachments dir: {error}"))
        .flatten()
        .map(|entry| entry.path())
        .collect();
    assert_eq!(written.len(), 1, "exactly one attachment file");
    assert_eq!(
        fs::read_to_string(&written[0]).unwrap(),
        "DOC GREPMESH-USERIO-TEST payload"
    );

    // The voice attachment keeps its transcript inline and is NOT downloaded
    // (downloads would need a token; none is set).
    assert!(read_cache(&cache, "telegram", "conv_t1").contains("transcript:"));
    assert_eq!(written.len(), 1);

    // Stage off: a new eligible attachment is not materialized, and turning
    // the stage off prunes previously materialized files.
    connection
        .execute(
            "INSERT INTO message_attachments (user_id,source,message_id,idx,kind,content_type,filename,size,src)
             VALUES (?1,'telegram','540308572:1',1,'document','text/plain','notes.txt',4,?2)",
            rusqlite::params![USER, doc_source.display().to_string()],
        )
        .unwrap();
    config.attachments.docs = false;
    let report = userio::sync_once(&config, &mut downloader).expect("sync with stages off");
    assert_eq!(report.attachments_skipped_stage, 3);
    assert!(
        !attachments_dir.join("540308572_1__1__notes.txt").exists(),
        "stage-off attachments must not appear"
    );
}

#[test]
fn userio_cache_root_is_indexed_by_the_regular_pipeline() {
    let dir = TempDir::new().unwrap();
    let sqlite = fixture_db(&dir);
    let cache = dir.path().join("cache");
    let config = test_config(&sqlite, &cache);
    let mut downloader = offline_downloader(&config);
    userio::sync_once(&config, &mut downloader).expect("seed cache");

    let mut roots = std::collections::BTreeMap::new();
    roots.insert("userio".to_string(), vec![cache.clone()]);
    let manager = IndexManager::start(
        roots,
        Vec::new(),
        16 * 1024 * 1024,
        3_600_000,
        IndexActivityConfig::default(),
        Some(dir.path().join("index.sqlite3")),
        SttConfig::default(),
        OcrConfig::default(),
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let status = manager.status();
        if format!("{:?}", status.state).contains("Ready") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "index never became ready: {:?}",
            status.state
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    let hits = manager
        .candidate_paths("GREPMESH-USERIO-TEST", &cache)
        .expect("candidate lookup");
    assert!(
        hits.iter().any(|path| path.ends_with("conv_t1.txt")),
        "expected conv_t1.txt among hits: {hits:?}"
    );
    assert!(
        hits.iter().any(|path| path.ends_with("conv_g1.txt")),
        "expected conv_g1.txt among hits: {hits:?}"
    );
}

#[test]
fn example_config_parses_with_userio_disabled_by_default() {
    let config = AppConfig::from_path("config.example.json").expect("parse example config");
    assert!(!config.userio.enabled, "userio must stay opt-in");
    assert_eq!(
        config.userio.sqlite_path,
        Path::new("/var/lib/universal-userio/userio.sqlite3")
    );
    assert!(!config.userio.attachments.docs);
    assert!(!config.userio.attachments.media);
}
