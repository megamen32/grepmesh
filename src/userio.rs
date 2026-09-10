//! Opt-in UserIO message cache adapter.
//!
//! Renders cached chat text from a local Universal UserIO SQLite store into
//! plain-text conversation files under a cache directory, and materializes
//! chat attachments there so the regular index extraction pipeline can index
//! documents (anydoc), images (OCR) and audio/video (whisper STT) exactly like
//! any other indexed file. The cache directory is expected to be listed in
//! the configured index roots; this module never talks to the index directly,
//! which keeps read_text, previews and mesh fan-out unchanged.
//!
//! The adapter only ever reads the UserIO database (SQLite `mode=ro`) and, for
//! attachment bytes that the store does not keep on disk, calls the local
//! UserIO MCP `userio.channels.download` tool. It never writes to UserIO.

use crate::config::UserioConfig;
use rusqlite::{Connection, OpenFlags};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::Duration,
};

/// The newest messages of every conversation stay in the main cache file;
/// older history moves to a `.archive.txt` file instead of being dropped.
const TAIL_RENDER_BUDGET_CHARS: usize = 400_000;
/// Archive files keep up to this much history; anything older is omitted with
/// an explicit marker.
const ARCHIVE_RENDER_BUDGET_CHARS: usize = 4_000_000;
/// Single message bodies longer than this are truncated in the render.
const MAX_MESSAGE_BODY_CHARS: usize = 100_000;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SyncReport {
    pub conversations: usize,
    pub written: usize,
    pub unchanged: usize,
    pub removed: usize,
    pub attachments_seen: usize,
    pub attachments_materialized: usize,
    pub attachments_skipped_stage: usize,
    pub attachments_skipped_size: usize,
    pub download_failures: usize,
    pub orphan_messages: usize,
}

#[derive(Debug, Clone)]
struct ConversationRow {
    user_id: String,
    id: String,
    source: String,
    sender: String,
    account_ref: String,
    updated_at: f64,
}

#[derive(Debug, Clone)]
struct MessageRow {
    conversation_id: String,
    source: String,
    message_id: String,
    sender: String,
    body: String,
    received_at: f64,
}

#[derive(Debug, Clone)]
struct AttachmentRow {
    source: String,
    message_id: String,
    idx: i64,
    kind: String,
    content_type: String,
    filename: String,
    size: Option<i64>,
    src: Option<String>,
    transcript: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttachmentStage {
    /// Documents and images: materialized for anydoc/OCR extraction.
    Doc,
    /// Audio/video: materialized for whisper STT extraction.
    Media,
}

/// Spawn the background poller. Errors are logged and retried on the next
/// tick; the task never panics the server.
pub fn spawn_poller(config: UserioConfig) {
    tokio::spawn(async move {
        let interval_ms = config.poll_interval_ms.max(1_000);
        let downloader = Downloader::from_config(&config);
        let mut downloader = downloader;
        tracing::info!(
            sqlite = %config.sqlite_path.display(),
            cache = %config.cache_dir.display(),
            interval_ms,
            "userio cache adapter enabled"
        );
        loop {
            match sync_once(&config, &mut downloader) {
                Ok(report) if !report.is_empty() => tracing::info!(
                    conversations = report.conversations,
                    written = report.written,
                    unchanged = report.unchanged,
                    removed = report.removed,
                    attachments = report.attachments_seen,
                    materialized = report.attachments_materialized,
                    "userio cache sync"
                ),
                Ok(_) => {}
                Err(error) => tracing::warn!(error = %error, "userio cache sync failed"),
            }
            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
        }
    });
}

impl SyncReport {
    fn is_empty(&self) -> bool {
        *self == SyncReport::default()
    }
}

pub fn sync_once(config: &UserioConfig, downloader: &mut Downloader) -> Result<SyncReport, String> {
    let mut report = SyncReport::default();
    let connection = open_read_only(&config.sqlite_path)?;
    let conversations = query_conversations(&connection, config)?;
    let messages = query_messages(&connection, config)?;
    let attachments = query_attachments(&connection, config)?;
    let contacts = query_contacts(&connection)?;
    report.attachments_seen = attachments.len();

    fs::create_dir_all(config.cache_dir.join("conversations"))
        .map_err(|error| format!("create {}: {error}", config.cache_dir.display()))?;
    restrict_permissions(&config.cache_dir);

    let mut messages_by_conversation: BTreeMap<(String, String), Vec<MessageRow>> = BTreeMap::new();
    for message in messages {
        messages_by_conversation
            .entry((message.conversation_id.clone(), message.source.clone()))
            .or_default()
            .push(message);
    }
    let mut attachments_by_message: BTreeMap<(String, String), Vec<AttachmentRow>> =
        BTreeMap::new();
    for attachment in &attachments {
        attachments_by_message
            .entry((attachment.message_id.clone(), attachment.source.clone()))
            .or_default()
            .push(attachment.clone());
    }

    let mut expected_conversation_files: BTreeSet<PathBuf> = BTreeSet::new();
    let mut expected_attachment_files: BTreeSet<PathBuf> = BTreeSet::new();
    let mut write_budget = config.max_writes_per_sync.max(1);
    let mut writes_capped = false;
    for conversation in &conversations {
        report.conversations += 1;
        let key = (conversation.id.clone(), conversation.source.clone());
        let conversation_messages = messages_by_conversation
            .get(&key)
            .cloned()
            .unwrap_or_default();
        if conversation_messages.is_empty() {
            continue;
        }
        let files = render_conversation_files(
            conversation,
            &conversation_messages,
            &attachments_by_message,
            &contacts,
        );
        let pending: Vec<&(&'static str, String)> = files
            .iter()
            .filter(|(suffix, rendered)| {
                let path = conversation_cache_path(
                    &config.cache_dir,
                    &conversation.source,
                    &format!("{}{suffix}", sanitize_component(&conversation.id)),
                );
                fs::read(&path)
                    .map(|existing| existing != rendered.as_bytes())
                    .unwrap_or(true)
            })
            .collect();
        if pending.is_empty() {
            for (suffix, _) in &files {
                expected_conversation_files.insert(conversation_cache_path(
                    &config.cache_dir,
                    &conversation.source,
                    &format!("{}{suffix}", sanitize_component(&conversation.id)),
                ));
            }
            report.unchanged += 1;
            continue;
        }
        if write_budget == 0 {
            writes_capped = true;
            continue;
        }
        for (suffix, rendered) in files {
            let path = conversation_cache_path(
                &config.cache_dir,
                &conversation.source,
                &format!("{}{suffix}", sanitize_component(&conversation.id)),
            );
            expected_conversation_files.insert(path.clone());
            if fs::read(&path)
                .map(|existing| existing != rendered.as_bytes())
                .unwrap_or(true)
            {
                if write_budget == 0 {
                    writes_capped = true;
                    continue;
                }
                write_budget -= 1;
                write_if_changed(&path, rendered.as_bytes())?;
                report.written += 1;
            } else {
                report.unchanged += 1;
            }
        }
    }
    let known_conversations: BTreeSet<(String, String)> = conversations
        .iter()
        .map(|conversation| (conversation.id.clone(), conversation.source.clone()))
        .collect();
    for (key, grouped) in &messages_by_conversation {
        if !known_conversations.contains(key) {
            report.orphan_messages += grouped.len();
        }
    }

    if config.downloads_enabled() || !attachments.is_empty() {
        fs::create_dir_all(config.cache_dir.join("attachments"))
            .map_err(|error| format!("create {}: {error}", config.cache_dir.display()))?;
    }
    for attachment in &attachments {
        let Some(stage) = classify_attachment(
            &attachment.kind,
            &attachment.content_type,
            &attachment.filename,
        ) else {
            continue;
        };
        let stage_enabled = match stage {
            AttachmentStage::Doc => config.attachments.docs,
            AttachmentStage::Media => config.attachments.media,
        };
        let transcript_present = attachment
            .transcript
            .as_deref()
            .is_some_and(|transcript| !transcript.trim().is_empty());
        if !stage_enabled {
            report.attachments_skipped_stage += 1;
            continue;
        }
        if transcript_present && !config.attachments.materialize_transcribed {
            // The transcript is already inlined in the conversation render.
            continue;
        }
        if let Some(size) = attachment
            .size
            .filter(|size| *size > config.attachments.max_bytes as i64)
        {
            tracing::warn!(
                source = %attachment.source,
                message_id = %attachment.message_id,
                size,
                "userio attachment exceeds max_bytes; skipping"
            );
            report.attachments_skipped_size += 1;
            continue;
        }
        match materialize_attachment(config, downloader, attachment)? {
            MaterializeOutcome::Written(path, changed) => {
                if changed {
                    report.attachments_materialized += 1;
                }
                expected_attachment_files.insert(path);
            }
            MaterializeOutcome::Unavailable(reason) => {
                tracing::warn!(
                    source = %attachment.source,
                    message_id = %attachment.message_id,
                    reason = %reason,
                    "userio attachment not materialized"
                );
            }
        }
    }
    if downloader.failed_downloads > 0 {
        report.download_failures = downloader.failed_downloads;
    }

    report.removed += prune_except(
        &config.cache_dir.join("conversations"),
        &expected_conversation_files,
    )?;
    report.removed += prune_except(
        &config.cache_dir.join("attachments"),
        &expected_attachment_files,
    )?;
    Ok(report)
}

enum MaterializeOutcome {
    Written(PathBuf, bool),
    Unavailable(String),
}

fn materialize_attachment(
    config: &UserioConfig,
    downloader: &mut Downloader,
    attachment: &AttachmentRow,
) -> Result<MaterializeOutcome, String> {
    let directory = config
        .cache_dir
        .join("attachments")
        .join(sanitize_component(&attachment.source));
    fs::create_dir_all(&directory)
        .map_err(|error| format!("create {}: {error}", directory.display()))?;
    let path = directory.join(attachment_file_name(attachment));
    if path.is_file() {
        return Ok(MaterializeOutcome::Written(path, false));
    }
    let bytes = if let Some(src) = attachment
        .src
        .as_deref()
        .filter(|src| !src.is_empty() && Path::new(src).is_file())
    {
        fs::read(src).map_err(|error| format!("read {src}: {error}"))?
    } else if config.downloads_enabled() {
        match downloader.download(attachment) {
            Ok(bytes) => bytes,
            Err(error) => {
                downloader.failed_downloads += 1;
                return Ok(MaterializeOutcome::Unavailable(error.to_string()));
            }
        }
    } else {
        return Ok(MaterializeOutcome::Unavailable(
            "attachment bytes are not local and downloads are disabled".to_string(),
        ));
    };
    if bytes.len() as u64 > config.attachments.max_bytes {
        return Ok(MaterializeOutcome::Unavailable(format!(
            "downloaded {} bytes exceeds max_bytes",
            bytes.len()
        )));
    }
    let changed = write_if_changed(&path, &bytes)?;
    Ok(MaterializeOutcome::Written(path, changed))
}

/// MCP download client for the local UserIO HTTP surface. Kept cheap: only
/// used for attachments whose bytes are not on disk.
pub struct Downloader {
    api_base: String,
    token: Option<String>,
    client: Option<reqwest::Client>,
    next_id: AtomicU64,
    failures: Mutex<HashMap<(String, String, i64), u32>>,
    max_attempts: u32,
    pub failed_downloads: usize,
}

impl Downloader {
    pub fn from_config(config: &UserioConfig) -> Self {
        let token = std::env::var(&config.token_env)
            .ok()
            .filter(|token| !token.trim().is_empty());
        Self {
            api_base: config.api_base.trim_end_matches('/').to_string(),
            token,
            client: None,
            next_id: AtomicU64::new(1),
            failures: Mutex::new(HashMap::new()),
            max_attempts: config.max_download_attempts.max(1),
            failed_downloads: 0,
        }
    }

    fn download(&mut self, attachment: &AttachmentRow) -> Result<Vec<u8>, String> {
        let key = (
            attachment.source.clone(),
            attachment.message_id.clone(),
            attachment.idx,
        );
        {
            let failures = self
                .failures
                .lock()
                .map_err(|_| "failure registry poisoned".to_string())?;
            if failures.get(&key).copied().unwrap_or(0) >= self.max_attempts {
                return Err(format!(
                    "download gave up after {} attempts",
                    self.max_attempts
                ));
            }
        }
        // UserIO file_ref contract: `<channel>:<provider_ref>`, where the
        // first segment selects the channel adapter. Most adapters resolve a
        // whole message (`<channel>:<message_id>`); VK additionally accepts
        // `vk:<message_id>:<idx>` for one attachment among many.
        let channel = Self::public_channel(&attachment.source);
        if channel.is_empty() {
            return Err(format!(
                "userio source {:?} has no download channel adapter",
                attachment.source
            ));
        }
        let message_ref = format!("{channel}:{}", attachment.message_id);
        let indexed_ref = format!("{message_ref}:{}", attachment.idx);
        let result = self
            .call_download(&message_ref)
            .or_else(|message_error| self.call_download(&indexed_ref).map_err(|_| message_error));
        match result {
            Ok(bytes) => {
                if let Ok(mut failures) = self.failures.lock() {
                    failures.remove(&key);
                }
                Ok(bytes)
            }
            Err(error) => {
                self.record_failure(key);
                Err(error)
            }
        }
    }

    /// Map a UserIO message source to the channel name its MCP API expects.
    fn public_channel(source: &str) -> &str {
        if source == "mail"
            || source == "email"
            || source == "gmail"
            || source.starts_with("gmail:")
        {
            "mail"
        } else if let Some(account) = source.strip_prefix("chatgpt:") {
            let _ = account;
            "chatgpt"
        } else {
            match source {
                "telegram" | "whatsapp" | "matrix" | "vk" | "sms" => source,
                _ => "",
            }
        }
    }

    fn call_download(&mut self, file_ref: &str) -> Result<Vec<u8>, String> {
        if self.token.is_none() {
            return Err(format!(
                "userio download token env is not set; cannot fetch {file_ref}"
            ));
        }
        if self.client.is_none() {
            self.client = Some(
                reqwest::Client::builder()
                    .timeout(Duration::from_secs(120))
                    .build()
                    .map_err(|error| format!("build userio client: {error}"))?,
            );
        }
        let client = self.client.as_ref().expect("client initialized");
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let body = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {
                "name": "userio.channels.download",
                "arguments": { "file_ref": file_ref },
            },
        });
        let request = client
            .post(format!("{}/mcp", self.api_base))
            .bearer_auth(self.token.as_deref().expect("token checked"))
            .json(&body);
        let response = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(request.send())
        })
        .map_err(|error| format!("userio download {file_ref}: {error}"))?;
        if !response.status().is_success() {
            return Err(format!(
                "userio download {file_ref}: HTTP {}",
                response.status()
            ));
        }
        let payload = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(response.json::<serde_json::Value>())
        })
        .map_err(|error| format!("userio download {file_ref}: bad JSON: {error}"))?;
        if let Some(error) = payload.get("error").and_then(|value| value.as_str()) {
            return Err(format!("userio download {file_ref}: {error}"));
        }
        // The MCP tool-call envelope carries the tool result in
        // result.structuredContent; older/plain responses use result directly.
        let result = payload
            .get("result")
            .ok_or_else(|| format!("userio download {file_ref}: no result"))?;
        if result.get("isError").and_then(|value| value.as_bool()) == Some(true) {
            let detail = result
                .pointer("/structuredContent/error")
                .and_then(|value| value.as_str())
                .unwrap_or("tool error");
            return Err(format!("userio download {file_ref}: {detail}"));
        }
        let file = result
            .pointer("/structuredContent/file")
            .or_else(|| result.pointer("/file"))
            .ok_or_else(|| format!("userio download {file_ref}: no file in result"))?;
        let data = file
            .get("data")
            .and_then(|value| value.as_str())
            .ok_or_else(|| format!("userio download {file_ref}: no base64 data"))?;
        base64_decode(data).map_err(|error| format!("userio download {file_ref}: base64: {error}"))
    }

    fn record_failure(&mut self, key: (String, String, i64)) {
        if let Ok(mut failures) = self.failures.lock() {
            *failures.entry(key).or_insert(0) += 1;
        }
    }
}

fn open_read_only(path: &Path) -> Result<Connection, String> {
    let url = format!("file:{}?mode=ro", path.display());
    let connection = Connection::open_with_flags(
        url,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| format!("open userio db {}: {error}", path.display()))?;
    connection
        .busy_timeout(Duration::from_millis(2_000))
        .map_err(|error| format!("userio db busy timeout: {error}"))?;
    Ok(connection)
}

fn user_filter(config: &UserioConfig) -> (String, Vec<String>) {
    if config.user_ids.is_empty() {
        (String::new(), Vec::new())
    } else {
        let placeholders = vec!["?"; config.user_ids.len()].join(",");
        (
            format!(" WHERE user_id IN ({placeholders})"),
            config.user_ids.clone(),
        )
    }
}

fn source_filter(config: &UserioConfig, column: &str) -> String {
    if config.include_sources.is_empty() {
        String::new()
    } else {
        let placeholders = vec!["?"; config.include_sources.len()].join(",");
        format!(" AND {column} IN ({placeholders})")
    }
}

fn bind_filter(params: &mut Vec<String>, values: &[String]) {
    params.extend(values.iter().cloned());
}

fn query_conversations(
    connection: &Connection,
    config: &UserioConfig,
) -> Result<Vec<ConversationRow>, String> {
    let (user_clause, user_values) = user_filter(config);
    let source_clause = source_filter(config, "source");
    // user_filter returns either "" or " WHERE ..."; source_filter always
    // starts with AND, so it can only be appended to an existing WHERE.
    let where_clause = if user_clause.is_empty() && !source_clause.is_empty() {
        format!(" WHERE 1=1{source_clause}")
    } else {
        format!("{user_clause}{source_clause}")
    };
    let sql = format!(
        "SELECT user_id, id, source, sender, account_ref, updated_at FROM conversations{where_clause} ORDER BY id"
    );
    let mut params = Vec::new();
    bind_filter(&mut params, &user_values);
    bind_filter(&mut params, &config.include_sources);
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| format!("prepare conversations: {error}"))?;
    let rows = statement
        .query_map(rusqlite::params_from_iter(params), |row| {
            Ok(ConversationRow {
                user_id: row.get(0)?,
                id: row.get(1)?,
                source: row.get(2)?,
                sender: row.get(3)?,
                account_ref: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                updated_at: row.get(5)?,
            })
        })
        .map_err(|error| format!("query conversations: {error}"))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("read conversations: {error}"))
}

fn query_messages(
    connection: &Connection,
    config: &UserioConfig,
) -> Result<Vec<MessageRow>, String> {
    let (user_clause, user_values) = user_filter(config);
    let source_clause = source_filter(config, "source");
    let where_clause = if user_clause.is_empty() && !source_clause.is_empty() {
        format!(" WHERE 1=1{source_clause}")
    } else {
        format!("{user_clause}{source_clause}")
    };
    let sql = format!(
        "SELECT conversation_id, source, message_id, sender, body, received_at FROM messages{where_clause} ORDER BY received_at"
    );
    let mut params = Vec::new();
    bind_filter(&mut params, &user_values);
    bind_filter(&mut params, &config.include_sources);
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| format!("prepare messages: {error}"))?;
    let rows = statement
        .query_map(rusqlite::params_from_iter(params), |row| {
            Ok(MessageRow {
                conversation_id: row.get(0)?,
                source: row.get(1)?,
                message_id: row.get(2)?,
                sender: row.get(3)?,
                body: row.get(4)?,
                received_at: row.get(5)?,
            })
        })
        .map_err(|error| format!("query messages: {error}"))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("read messages: {error}"))
}

fn query_attachments(
    connection: &Connection,
    config: &UserioConfig,
) -> Result<Vec<AttachmentRow>, String> {
    let (user_clause, user_values) = user_filter(config);
    let source_clause = source_filter(config, "source");
    let where_clause = if user_clause.is_empty() && !source_clause.is_empty() {
        format!(" WHERE 1=1{source_clause}")
    } else {
        format!("{user_clause}{source_clause}")
    };
    let sql = format!(
        "SELECT source, message_id, idx, kind, content_type, filename, size, src, transcript FROM message_attachments{where_clause}"
    );
    let mut params = Vec::new();
    bind_filter(&mut params, &user_values);
    bind_filter(&mut params, &config.include_sources);
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| format!("prepare attachments: {error}"))?;
    let rows = statement
        .query_map(rusqlite::params_from_iter(params), |row| {
            Ok(AttachmentRow {
                source: row.get(0)?,
                message_id: row.get(1)?,
                idx: row.get(2)?,
                kind: row.get(3)?,
                content_type: row.get(4)?,
                filename: row.get(5)?,
                size: row.get(6)?,
                src: row.get(7)?,
                transcript: row.get(8)?,
            })
        })
        .map_err(|error| format!("query attachments: {error}"))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("read attachments: {error}"))
}

fn query_contacts(connection: &Connection) -> Result<HashMap<String, String>, String> {
    let mut statement = connection
        .prepare("SELECT user_id, source, sender, name FROM contact_names")
        .map_err(|error| format!("prepare contact_names: {error}"))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                format!(
                    "{}|{}|{}",
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?
                ),
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(|error| format!("query contact_names: {error}"))?;
    let mut contacts = HashMap::new();
    for row in rows {
        let (key, name) = row.map_err(|error| format!("read contact: {error}"))?;
        contacts.insert(key, name);
    }
    Ok(contacts)
}

fn conversation_cache_path(cache_dir: &Path, source: &str, file_stem: &str) -> PathBuf {
    cache_dir
        .join("conversations")
        .join(sanitize_component(source))
        .join(format!("{file_stem}.txt"))
}

fn attachment_file_name(attachment: &AttachmentRow) -> String {
    let safe = sanitize_component(&attachment.filename);
    let safe = if safe.is_empty() {
        "attachment.bin".to_string()
    } else {
        safe
    };
    format!(
        "{}__{}__{}",
        sanitize_component(&attachment.message_id),
        attachment.idx.max(0),
        safe
    )
}

/// Keep one path component filesystem-safe and stable.
fn sanitize_component(raw: &str) -> String {
    let mut sanitized: String = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    sanitized.truncate(96);
    while sanitized.ends_with('.') {
        sanitized.pop();
    }
    if sanitized.is_empty() {
        sanitized.push('_');
    }
    sanitized
}

fn render_conversation_files(
    conversation: &ConversationRow,
    messages: &[MessageRow],
    attachments_by_message: &BTreeMap<(String, String), Vec<AttachmentRow>>,
    contacts: &HashMap<String, String>,
) -> Vec<(&'static str, String)> {
    let contact = |sender: &str| -> String {
        let key = format!(
            "{}|{}|{}",
            conversation.user_id, conversation.source, sender
        );
        match contacts.get(&key) {
            Some(name) if !name.trim().is_empty() => format!("{name} <{sender}>"),
            _ => sender.to_string(),
        }
    };
    let mut blocks: Vec<String> = Vec::with_capacity(messages.len());
    for message in messages {
        let mut block = String::new();
        let body = normalize_body(&message.body);
        let sender_display = contact(&message.sender);
        let _ = writeln!(
            block,
            "[{}] {}: {}",
            epoch_to_utc(message.received_at),
            sender_display,
            truncate_chars(&body, MAX_MESSAGE_BODY_CHARS)
        );
        if let Some(attachments) =
            attachments_by_message.get(&(message.message_id.clone(), message.source.clone()))
        {
            for attachment in attachments {
                let _ = writeln!(
                    block,
                    "[attachment kind={} name={} type={}]",
                    attachment.kind, attachment.filename, attachment.content_type
                );
                if let Some(transcript) = attachment
                    .transcript
                    .as_deref()
                    .filter(|transcript| !transcript.trim().is_empty())
                {
                    let _ = writeln!(
                        block,
                        "transcript: {}",
                        truncate_chars(&normalize_body(transcript), MAX_MESSAGE_BODY_CHARS)
                    );
                }
            }
        }
        blocks.push(block);
    }

    let header = |total: usize, note: &str| -> String {
        let sender_display = contact(&conversation.sender);
        let mut head = String::new();
        let _ = writeln!(
            head,
            "# userio conversation {} [{}]",
            conversation.id, conversation.source
        );
        let _ = writeln!(
            head,
            "# peer: {} | account: {} | updated: {}",
            sender_display,
            if conversation.account_ref.is_empty() {
                "-"
            } else {
                &conversation.account_ref
            },
            epoch_to_utc(conversation.updated_at)
        );
        let _ = writeln!(head, "# messages: {total}");
        if !note.is_empty() {
            let _ = writeln!(head, "# {note}");
        }
        let _ = writeln!(head);
        head
    };

    // Split per message so the recent tail always stays in the main file and
    // older history moves to an archive file instead of being dropped.
    let tail = budget_tail(&blocks, TAIL_RENDER_BUDGET_CHARS);
    if tail == blocks.len() {
        let rendered = header(blocks.len(), "");
        return vec![("", format!("{rendered}{}", blocks.concat()))];
    }
    let archive_blocks = &blocks[..blocks.len() - tail];
    let (kept_archive, omitted) = budget_head(archive_blocks, ARCHIVE_RENDER_BUDGET_CHARS);
    let archive_note = if omitted > 0 {
        format!("{omitted} oldest messages omitted beyond the archive render budget")
    } else {
        String::new()
    };
    let archive = format!(
        "{}{}",
        header(blocks.len(), &archive_note),
        archive_blocks[kept_archive..].concat()
    );
    let main = format!(
        "{}{}",
        header(
            blocks.len(),
            &format!("older messages continue in {}.archive.txt", conversation.id)
        ),
        blocks[blocks.len() - tail..].concat()
    );
    vec![("", main), (".archive", archive)]
}

/// Number of message blocks (counted from the newest) that fit the budget.
fn budget_tail(blocks: &[String], budget: usize) -> usize {
    let mut used = 0usize;
    for (index, block) in blocks.iter().enumerate().rev() {
        let size = block.len() + 1;
        if used + size > budget && index + 1 < blocks.len() {
            return blocks.len() - index - 1;
        }
        used += size;
    }
    blocks.len()
}

/// Number of leading archive blocks to drop so the rest fits the budget.
fn budget_head(blocks: &[String], budget: usize) -> (usize, usize) {
    let mut used = 0usize;
    for (index, block) in blocks.iter().enumerate().rev() {
        let size = block.len() + 1;
        if used + size > budget && index + 1 < blocks.len() {
            return (index + 1, index + 1);
        }
        used += size;
    }
    (0, 0)
}

fn normalize_body(body: &str) -> String {
    let trimmed = body.trim_start();
    let looks_like_html = trimmed.starts_with("<!doctype")
        || trimmed.starts_with("<html")
        || trimmed.starts_with("<HTML")
        || trimmed.starts_with("<!DOCTYPE");
    if !looks_like_html {
        return body.trim_end_matches(['\r']).to_string();
    }
    strip_html(body)
}

fn strip_html(html: &str) -> String {
    let lower = html.to_ascii_lowercase();
    let mut remove_ranges: Vec<(usize, usize)> = Vec::new();
    for (tag, end_marker) in [("<script", "</script>"), ("<style", "</style>")] {
        let mut cursor = 0;
        while let Some(start) = lower[cursor..].find(tag) {
            let start = cursor + start;
            if let Some(end) = lower[start..].find(end_marker) {
                let end = start + end + end_marker.len();
                remove_ranges.push((start, end));
                cursor = end;
            } else {
                remove_ranges.push((start, lower.len()));
                break;
            }
        }
    }
    remove_ranges.sort_unstable();
    let mut text = String::with_capacity(html.len() / 2);
    let mut index = 0usize;
    let mut in_tag = false;
    for (position, ch) in html.char_indices() {
        while index < remove_ranges.len() && position >= remove_ranges[index].1 {
            index += 1;
        }
        if index < remove_ranges.len() && position >= remove_ranges[index].0 {
            continue;
        }
        match ch {
            '<' => in_tag = true,
            '>' => {
                if in_tag {
                    in_tag = false;
                    text.push(' ');
                }
            }
            _ if !in_tag => text.push(ch),
            _ => {}
        }
    }
    let decoded = decode_entities(&text);
    decoded
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn decode_entities(text: &str) -> String {
    let mut decoded = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find('&') {
        decoded.push_str(&rest[..start]);
        let tail = &rest[start..];
        let end = tail.find(';');
        let entity_len = end.map(|end| end + 1);
        let Some(entity_len) = entity_len.filter(|len| *len <= 12) else {
            decoded.push('&');
            rest = &tail[1..];
            continue;
        };
        let entity = &tail[..entity_len];
        let replacement = match entity {
            "&amp;" => Some('&'),
            "&lt;" => Some('<'),
            "&gt;" => Some('>'),
            "&quot;" => Some('"'),
            "&apos;" => Some('\''),
            "&nbsp;" => Some(' '),
            other => {
                let digits = other.trim_start_matches('&').trim_end_matches(';');
                if let Some(number) = digits
                    .strip_prefix("#")
                    .and_then(|number| u32::from_str_radix(number, 10).ok())
                    .or_else(|| {
                        digits
                            .strip_prefix("#x")
                            .or_else(|| digits.strip_prefix("#X"))
                            .and_then(|hex| u32::from_str_radix(hex, 16).ok())
                    })
                {
                    char::from_u32(number)
                } else {
                    None
                }
            }
        };
        match replacement {
            Some(ch) => decoded.push(ch),
            None => decoded.push_str(entity),
        }
        rest = &tail[entity_len..];
    }
    decoded.push_str(rest);
    decoded
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut truncated: String = text.chars().take(max_chars).collect();
    let _ = writeln!(truncated, "[truncated at {max_chars} characters]");
    truncated
}

/// Convert a Unix timestamp (seconds, possibly fractional) to `YYYY-MM-DD HH:MM` UTC.
fn epoch_to_utc(timestamp: f64) -> String {
    let seconds = timestamp.floor() as i64;
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}",
        seconds_of_day / 3_600,
        (seconds_of_day % 3_600) / 60
    )
}

/// Howard Hinnant's civil-from-days algorithm.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

/// Write only when the content actually changed so unchanged polls do not
/// bump mtimes and stir the index watcher.
fn write_if_changed(path: &Path, bytes: &[u8]) -> Result<bool, String> {
    if let Ok(existing) = fs::read(path) {
        if existing == bytes {
            return Ok(false);
        }
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    let temp = path.with_extension(format!("tmp{}", std::process::id()));
    fs::write(&temp, bytes).map_err(|error| format!("write {}: {error}", temp.display()))?;
    set_private_permissions(&temp);
    fs::rename(&temp, path).map_err(|error| format!("rename {}: {error}", path.display()))?;
    Ok(true)
}

/// Remove cached files that the current store state no longer expects.
fn prune_except(directory: &Path, expected: &BTreeSet<PathBuf>) -> Result<usize, String> {
    let mut removed = 0;
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(format!("read {}: {error}", directory.display())),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            removed += prune_except(&path, expected)?;
            if fs::read_dir(&path)
                .map(|mut entries| entries.next().is_none())
                .unwrap_or(false)
            {
                let _ = fs::remove_dir(&path);
            }
            continue;
        }
        let is_temp = path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.starts_with("tmp"));
        if is_temp {
            let _ = fs::remove_file(&path);
            continue;
        }
        if !expected.contains(&path) {
            match fs::remove_file(&path) {
                Ok(()) => removed += 1,
                Err(error) => {
                    return Err(format!("remove {}: {error}", path.display()));
                }
            }
        }
    }
    Ok(removed)
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
}

#[cfg(unix)]
fn restrict_permissions(directory: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(directory, fs::Permissions::from_mode(0o700));
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) {}
#[cfg(not(unix))]
fn restrict_permissions(_directory: &Path) {}

fn classify_attachment(kind: &str, content_type: &str, filename: &str) -> Option<AttachmentStage> {
    let kind = kind.trim().to_ascii_lowercase();
    let content_type = content_type.trim().to_ascii_lowercase();
    if kind == "telegram_routing" {
        return None;
    }
    let is_media = matches!(
        kind.as_str(),
        "voice" | "audio" | "video" | "video_note" | "round" | "animation" | "ptt"
    ) || content_type.starts_with("audio/")
        || content_type.starts_with("video/");
    if is_media {
        return Some(AttachmentStage::Media);
    }
    let is_doc = content_type.starts_with("text/")
        || content_type.starts_with("image/")
        || content_type.contains("pdf")
        || content_type.contains("officedocument")
        || content_type.contains("msword")
        || content_type.contains("document")
        || content_type.contains("spreadsheet")
        || content_type.contains("presentation")
        || content_type.contains("rtf")
        || anydoc::Format::from_path(Path::new(filename)).is_some();
    if is_doc {
        return Some(AttachmentStage::Doc);
    }
    None
}

/// Minimal standard base64 decoder (no external dependency).
fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut lookup = [255u8; 256];
    for (index, byte) in TABLE.iter().enumerate() {
        lookup[*byte as usize] = index as u8;
    }
    let mut output = Vec::with_capacity(input.len() / 4 * 3);
    let mut buffer: u32 = 0;
    let mut bits = 0u32;
    for byte in input.bytes() {
        if byte == b'=' || byte == b'\n' || byte == b'\r' {
            continue;
        }
        let value = lookup[byte as usize];
        if value == 255 {
            return Err(format!("invalid base64 byte {byte:#x}"));
        }
        buffer = (buffer << 6) | value as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push((buffer >> bits) as u8);
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_path_components() {
        assert_eq!(sanitize_component("gmail"), "gmail");
        assert_eq!(
            sanitize_component("chatgpt:maha.grossomaha-gmail.com"),
            "chatgpt_maha.grossomaha-gmail.com"
        );
        assert_eq!(sanitize_component("/../etc/passwd"), "_.._etc_passwd");
        assert_eq!(sanitize_component(""), "_");
    }

    #[test]
    fn formats_epoch_as_utc() {
        assert_eq!(epoch_to_utc(1_788_971_520.0), "2026-09-09 16:32");
        assert_eq!(epoch_to_utc(0.0), "1970-01-01 00:00");
        assert_eq!(epoch_to_utc(951_782_400.0), "2000-02-29 00:00");
    }

    #[test]
    fn strips_html_documents_to_text() {
        let html = "<!DOCTYPE html><html><head><style>body{color:red}</style></head>\
<body><p>Hi&nbsp;there &amp; welcome</p><script>alert(1)</script><p>Second line</p></body></html>";
        let text = strip_html(html);
        assert!(text.contains("Hi there & welcome"), "got: {text}");
        assert!(text.contains("Second line"));
        assert!(
            !text.contains("alert"),
            "script body must be dropped: {text}"
        );
        assert!(
            !text.contains("color:red"),
            "style body must be dropped: {text}"
        );
    }

    #[test]
    fn decode_entities_handles_named_and_numeric() {
        assert_eq!(decode_entities("a&amp;b"), "a&b");
        assert_eq!(decode_entities("&#1048;&#1090;"), "Ит");
        assert_eq!(decode_edges("&unknown;"), "&unknown;");
        assert_eq!(decode_edges("5 &lt; 6"), "5 < 6");
    }

    fn decode_edges(input: &str) -> String {
        decode_entities(input)
    }

    #[test]
    fn classifies_attachments_into_stages() {
        assert_eq!(
            classify_attachment("voice", "audio/ogg", "x.ogg"),
            Some(AttachmentStage::Media)
        );
        assert_eq!(
            classify_attachment("video", "video/mp4", "clip.mp4"),
            Some(AttachmentStage::Media)
        );
        assert_eq!(
            classify_attachment("document", "application/pdf", "report.pdf"),
            Some(AttachmentStage::Doc)
        );
        assert_eq!(
            classify_attachment("image", "image/png", "shot.png"),
            Some(AttachmentStage::Doc)
        );
        assert_eq!(
            classify_attachment(
                "file",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "doc.docx"
            ),
            Some(AttachmentStage::Doc)
        );
        assert_eq!(
            classify_attachment(
                "telegram_routing",
                "application/vnd.userio.telegram-routing+json",
                "r.json"
            ),
            None
        );
    }

    #[test]
    fn decodes_base64() {
        assert_eq!(base64_decode("aGVsbG8=").unwrap(), b"hello");
        assert_eq!(base64_decode("w6k=").unwrap(), "é".as_bytes());
        assert!(base64_decode("a*b").is_err());
    }

    #[test]
    fn large_conversations_split_into_tail_and_archive() {
        let conversation = ConversationRow {
            user_id: "user_owner".into(),
            id: "conv_big".into(),
            source: "whatsapp".into(),
            sender: "peer@s".into(),
            account_ref: String::new(),
            updated_at: 1_788_971_520.0,
        };
        let marker_newest = "NEWEST-MARKER-should-stay-in-main";
        let marker_oldest = "OLDEST-MARKER-should-live-in-archive";
        let mut messages: Vec<MessageRow> = (0..6_000)
            .map(|index| MessageRow {
                conversation_id: "conv_big".into(),
                source: "whatsapp".into(),
                message_id: format!("m{index}"),
                sender: "peer@s".into(),
                body: if index == 0 {
                    marker_oldest.to_string()
                } else if index == 5_999 {
                    marker_newest.to_string()
                } else {
                    "x".repeat(120)
                },
                received_at: 1_700_000_000.0 + index as f64,
            })
            .collect();
        messages.reverse(); // exercise ordering coming from SQL anyway
        messages.sort_by(|a, b| a.received_at.total_cmp(&b.received_at));
        let files =
            render_conversation_files(&conversation, &messages, &BTreeMap::new(), &HashMap::new());
        assert_eq!(files.len(), 2, "expected main + archive files");
        let main = &files[0].1;
        let archive = &files[1].1;
        assert!(main.contains(marker_newest), "newest must stay in main");
        assert!(!main.contains(marker_oldest));
        assert!(archive.contains(marker_oldest), "oldest must be archived");
        assert!(!archive.contains(marker_newest));
        assert!(main.contains("older messages continue in conv_big.archive.txt"));
    }

    #[test]
    fn truncates_long_bodies_with_marker() {
        let long = "x".repeat(300);
        let truncated = truncate_chars(&long, 100);
        assert!(truncated.starts_with(&"x".repeat(100)));
        assert!(truncated.contains("[truncated at 100 characters]"));
    }

    #[test]
    fn conversation_render_includes_header_and_transcript() {
        let conversation = ConversationRow {
            user_id: "user_owner".into(),
            id: "conv_abc".into(),
            source: "telegram".into(),
            sender: "540308572".into(),
            account_ref: "telegram:8810909089".into(),
            updated_at: 1_788_971_520.0,
        };
        let messages = vec![
            MessageRow {
                conversation_id: "conv_abc".into(),
                source: "telegram".into(),
                message_id: "540308572:1".into(),
                sender: "540308572".into(),
                body: "plain message".into(),
                received_at: 1_788_971_000.0,
            },
            MessageRow {
                conversation_id: "conv_abc".into(),
                source: "telegram".into(),
                message_id: "540308572:2".into(),
                sender: "540308572".into(),
                body: "".into(),
                received_at: 1_788_971_500.0,
            },
        ];
        let mut attachments = BTreeMap::new();
        attachments.insert(
            ("540308572:2".to_string(), "telegram".to_string()),
            vec![AttachmentRow {
                source: "telegram".into(),
                message_id: "540308572:2".into(),
                idx: 0,
                kind: "voice".into(),
                content_type: "audio/ogg".into(),
                filename: "telegram-2.ogg".into(),
                size: Some(1234),
                src: None,
                transcript: Some("Голосовое сообщение".into()),
            }],
        );
        let files =
            render_conversation_files(&conversation, &messages, &attachments, &HashMap::new());
        assert_eq!(files.len(), 1);
        let rendered = &files[0].1;
        assert!(rendered.contains("# userio conversation conv_abc [telegram]"));
        assert!(rendered.contains("plain message"));
        assert!(rendered.contains("[attachment kind=voice name=telegram-2.ogg"));
        assert!(rendered.contains("transcript: Голосовое сообщение"));
    }
}
