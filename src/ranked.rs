//! Bounded lexical ranked search over the existing index, isolated from exact modes.
use crate::backend::LocalBackend;
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs,
    io::Read,
    path::PathBuf,
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct RankedRequest {
    pub query: String,
    pub roots: Vec<String>,
    pub read_roots: Vec<String>,
    pub path_globs: Vec<String>,
    pub limit: usize,
    pub context_lines: usize,
    pub overall_timeout: Duration,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RankedCoverage {
    StrongMatch,
    WeakMatch,
    NoCoverage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedHit {
    pub host_id: String,
    pub path: String,
    pub start_line: usize,
    pub end_line: usize,
    pub line_number: usize,
    pub text: String,
    pub score: f64,
    pub matched_terms: Vec<String>,
    pub match_quality: RankedCoverage,
    pub freshness: String,
    pub body_sha256: String,
    pub index_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedOutcome {
    pub hits: Vec<RankedHit>,
    pub coverage: RankedCoverage,
    pub partial: bool,
    pub truncated: bool,
    pub warnings: Vec<String>,
    pub index_generation: u64,
    pub index_state: String,
}

/// Terms are quoted later, never interpreted as FTS operators. Original
/// identifiers survive alongside their snake/camel components.
pub fn query_terms(query: &str) -> Result<Vec<String>> {
    ensure!(
        !query.trim().is_empty() && query.len() <= 2048,
        "ranked query must contain 1..2048 bytes"
    );
    const STOPWORDS: &[&str] = &[
        "about", "after", "again", "against", "also", "and", "any", "are", "because", "been",
        "before", "being", "between", "both", "but", "can", "could", "did", "does", "each", "else",
        "for", "from", "had", "has", "have", "having", "here", "how", "into", "its", "just", "may",
        "more", "most", "once", "only", "other", "our", "out", "own", "same", "should", "some",
        "such", "than", "that", "the", "their", "then", "there", "these", "they", "this", "those",
        "too", "under", "until", "very", "was", "were", "what", "when", "where", "which", "while",
        "who", "whom", "why", "will", "with", "would", "you", "your",
    ];
    let mut terms = BTreeSet::new();
    // Preserve structured identifiers and host/CLI tokens as exact anchors in
    // addition to their searchable components. The anchor is never treated as
    // a substring and is not tied to any particular corpus path.
    for token in query.split_whitespace() {
        let token = token.trim_matches(|c: char| !c.is_alphanumeric() && !"_./-=:@".contains(c));
        let chars: Vec<char> = token.chars().collect();
        let camel = chars
            .windows(2)
            .any(|pair| pair[0].is_lowercase() && pair[1].is_uppercase());
        let structured = token.chars().any(|c| "_./-=:@".contains(c)) || camel;
        if structured && token.chars().count() >= 3 {
            terms.insert(format!("literal:{}", token.to_lowercase()));
        }
    }
    for word in query.split(|c: char| !c.is_alphanumeric() && c != '_') {
        let mut split = String::new();
        let mut previous_lower = false;
        for character in word.chars() {
            if character.is_uppercase() && previous_lower {
                split.push(' ');
            }
            split.push(character);
            previous_lower = character.is_lowercase() || character.is_ascii_digit();
        }
        for term in
            std::iter::once(word).chain(split.split(|c: char| c == '_' || c.is_whitespace()))
        {
            let term = term.to_lowercase();
            if term.chars().count() >= 3 && !STOPWORDS.contains(&term.as_str()) {
                terms.insert(term);
            }
        }
    }
    ensure!(
        !terms.is_empty(),
        "ranked search requires a term with at least three characters"
    );
    ensure!(
        terms.len() <= 32,
        "ranked search permits at most 32 normalized terms"
    );
    Ok(terms.into_iter().collect())
}

impl LocalBackend {
    pub async fn ranked_search_bounded(&self, request: RankedRequest) -> Result<RankedOutcome> {
        let terms = query_terms(&request.query)?;
        ensure!(
            !request.read_roots.is_empty(),
            "ranked search requires authorized read roots"
        );
        let valid_root = |root: &str| {
            std::path::Path::new(root).is_absolute()
                && !std::path::Path::new(root)
                    .components()
                    .any(|c| matches!(c, std::path::Component::ParentDir))
        };
        ensure!(
            request.read_roots.iter().all(|r| valid_root(r)),
            "ranked read roots must be absolute without parent traversal"
        );
        let selected = self.selected_ranked_roots(&request.roots)?;
        let mut roots = Vec::new();
        for configured in selected {
            for permitted in &request.read_roots {
                let permitted = PathBuf::from(permitted);
                let root = if configured.starts_with(&permitted) {
                    configured.clone()
                } else if permitted.starts_with(&configured) {
                    permitted
                } else {
                    continue;
                };
                if let Ok(canonical) = fs::canonicalize(&root) {
                    // Canonicalization cannot widen a lexical ACL through symlinks.
                    if canonical == root {
                        roots.push(canonical);
                    }
                }
            }
        }
        roots.sort();
        roots.dedup();
        let backend = self.clone();
        tokio::task::spawn_blocking(move || {
            let snapshot = backend.index.status();
            let mut outcome = RankedOutcome {
                hits: Vec::new(),
                coverage: RankedCoverage::NoCoverage,
                partial: false,
                truncated: false,
                warnings: Vec::new(),
                index_generation: snapshot.generation,
                index_state: format!("{:?}", snapshot.state).to_lowercase(),
            };
            let deadline = Instant::now()
                + request
                    .overall_timeout
                    .min(Duration::from_millis(backend.limits.overall_timeout_ms));
            if roots.is_empty() {
                outcome.partial = true;
                outcome
                    .warnings
                    .push("no authorized local indexed roots available".into());
                return Ok(outcome);
            }
            if backend.remote_index.is_some() {
                outcome.partial = true;
                outcome.warnings.push(
                    "ranked remote-index coverage is not implemented; local indexed sources only"
                        .into(),
                );
            }
            let max_file_bytes = backend.limits.max_file_bytes.min(1024 * 1024);
            let result = backend.index.visit_ranked_documents(
                &terms,
                &roots,
                &request.path_globs,
                &backend.exclude_globs,
                deadline,
                |path, indexed_body| {
                    if Instant::now() >= deadline {
                        outcome.partial = true;
                        return;
                    }
                    // Plain-text citations are verified against current bytes. Extracted
                    // PDF/OCR text cannot claim physical source line numbers.
                    let current = open_regular_source(&path).and_then(|file| {
                        let mut bytes = Vec::new();
                        file.take(max_file_bytes + 1).read_to_end(&mut bytes)?;
                        Ok(bytes)
                    });
                    let Ok(bytes) = current else {
                        outcome.partial = true;
                        return;
                    };
                    if bytes.len() as u64 > max_file_bytes || bytes.contains(&0) {
                        outcome.partial = true;
                        return;
                    }
                    let Ok(body) = String::from_utf8(bytes) else {
                        outcome.partial = true;
                        return;
                    };
                    if body != indexed_body {
                        outcome.partial = true;
                        if !outcome
                            .warnings
                            .iter()
                            .any(|w| w == "changed source omitted until index catches up")
                        {
                            outcome
                                .warnings
                                .push("changed source omitted until index catches up".into());
                        }
                        return;
                    }
                    if let Some(hit) = score_document(
                        &backend.host_id,
                        &path,
                        &body,
                        &terms,
                        request.context_lines.min(12),
                        snapshot.generation,
                    ) {
                        outcome.hits.push(hit);
                    }
                },
            );
            match result {
                Ok(Some(true)) => {}
                Ok(Some(false)) => {
                    outcome.partial = true;
                    outcome.truncated = true;
                    outcome
                        .warnings
                        .push("ranked candidate/body budget reached".into());
                }
                Ok(None) => {
                    outcome.partial = true;
                    outcome
                        .warnings
                        .push("ranked index unavailable or not ready".into());
                }
                Err(error) => {
                    outcome.partial = true;
                    outcome.warnings.push(error);
                }
            }
            outcome.hits.sort_by(|a, b| {
                b.score
                    .total_cmp(&a.score)
                    .then(a.path.cmp(&b.path))
                    .then(a.line_number.cmp(&b.line_number))
            });
            outcome.coverage = if outcome
                .hits
                .iter()
                .any(|hit| matches!(hit.match_quality, RankedCoverage::StrongMatch))
            {
                RankedCoverage::StrongMatch
            } else if outcome.hits.is_empty() {
                RankedCoverage::NoCoverage
            } else {
                RankedCoverage::WeakMatch
            };
            if !outcome.partial {
                match outcome.coverage {
                    RankedCoverage::NoCoverage => outcome
                        .warnings
                        .push("no ranked coverage: no indexed passage matched query terms".into()),
                    RankedCoverage::WeakMatch => outcome.warnings.push(
                        "only weak ranked matches: review passages for answer coverage".into(),
                    ),
                    RankedCoverage::StrongMatch => {}
                }
            }
            let limit = request.limit.min(backend.limits.max_results);
            outcome.truncated |= outcome.hits.len() > limit;
            outcome.hits.truncate(limit);
            let mut response_bytes = 0;
            outcome.hits.retain(|hit| {
                response_bytes += serde_json::to_vec(hit)
                    .map(|bytes| bytes.len())
                    .unwrap_or(usize::MAX / 2);
                response_bytes <= backend.limits.max_response_bytes
            });
            if response_bytes > backend.limits.max_response_bytes {
                outcome.truncated = true;
            }
            if outcome.partial && outcome.warnings.is_empty() {
                outcome.warnings.push(
                    "some indexed sources could not be verified as current plain text".into(),
                );
            }
            Ok(outcome)
        })
        .await?
    }
}

/// Open an indexed path without following a last-moment symlink replacement or
/// blocking forever if a regular file was replaced by a FIFO.
#[cfg(unix)]
fn open_regular_source(path: &std::path::Path) -> std::io::Result<fs::File> {
    use std::{
        ffi::CString,
        os::{
            fd::{AsRawFd, FromRawFd, OwnedFd},
            unix::ffi::OsStrExt,
        },
        path::Component,
    };
    if !path.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "ranked source path must be absolute",
        ));
    }
    let root = CString::new("/").expect("static path has no NUL");
    let root_fd = unsafe {
        libc::open(
            root.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if root_fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut directory = unsafe { OwnedFd::from_raw_fd(root_fd) };
    let mut components = path
        .components()
        .filter_map(|component| match component {
            Component::RootDir => None,
            Component::Normal(part) => Some(Ok(part)),
            _ => Some(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "ranked source path contains a non-normal component",
            ))),
        })
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .peekable();
    while let Some(component) = components.next() {
        let name = CString::new(component.as_bytes()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "NUL in ranked source path",
            )
        })?;
        let last = components.peek().is_none();
        let mut flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
        if last {
            flags |= libc::O_NONBLOCK;
        } else {
            flags |= libc::O_DIRECTORY;
        }
        let fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let opened = unsafe { OwnedFd::from_raw_fd(fd) };
        if last {
            let file = fs::File::from(opened);
            if !file.metadata()?.is_file() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "ranked source is not a regular file",
                ));
            }
            return Ok(file);
        }
        directory = opened;
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "ranked source path has no filename",
    ))
}

/// Windows filesystems do not expose POSIX nonblocking/no-follow flags; reject
/// symlinks and non-regular paths before opening, then validate the descriptor.
#[cfg(not(unix))]
fn open_regular_source(path: &std::path::Path) -> std::io::Result<fs::File> {
    use std::fs::OpenOptions;
    let path_type = fs::symlink_metadata(path)?.file_type();
    if !path_type.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "ranked source is not a regular file",
        ));
    }
    let file = OpenOptions::new().read(true).open(path)?;
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "ranked source is not a regular file",
        ));
    }
    Ok(file)
}

fn score_document(
    host: &str,
    path: &std::path::Path,
    body: &str,
    terms: &[String],
    context: usize,
    generation: u64,
) -> Option<RankedHit> {
    let lines: Vec<&str> = body.lines().collect();
    if lines.is_empty() {
        return None;
    }
    let line_masks = lines
        .iter()
        .map(|line| term_mask(line, terms))
        .collect::<Vec<_>>();
    let body_mask = line_masks.iter().fold(0, |all, line| all | line);
    let path_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();
    let parent_name = path
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();
    let path_mask = term_mask(&format!("{parent_name} {path_name}"), terms);
    if body_mask == 0 && path_mask == 0 {
        return None;
    }

    // Rank compact contiguous passages. Whole-document coverage is deliberately
    // secondary so a query's terms scattered across unrelated sections do not
    // look like one strong answer.
    let best = best_passage_window(&lines, &line_masks);
    let anchor = best.map(|window| window.anchor).unwrap_or(0);
    let (scope_start, scope_end, scope_heading) = structural_scope(&lines, anchor);
    let title_mask = scope_heading
        .map(|index| term_mask(lines[index], terms))
        .unwrap_or_default();
    let passage_mask = best.map(|window| window.mask).unwrap_or_default();
    let total_terms = terms.len() as f64;
    let passage_coverage = passage_mask.count_ones() as f64 / total_terms;
    let document_coverage = body_mask.count_ones() as f64 / total_terms;
    let path_coverage = path_mask.count_ones() as f64 / total_terms;
    let title_coverage = title_mask.count_ones() as f64 / total_terms;
    let definition_bonus = best.is_some_and(|window| window.definition) as u8 as f64 * 5.0;
    let dispersion_penalty = (document_coverage - passage_coverage).max(0.0) * 15.0;
    let size_penalty = if body.len() > 32 * 1024 {
        ((body.len() as f64 / (32 * 1024) as f64).log2() * 2.0).min(8.0)
    } else {
        0.0
    };
    // Fixed query-relative weights keep scores comparable across hosts; no
    // corpus-wide IDF/BM25 statistic can change a peer's score scale.
    let score = (passage_coverage * 65.0
        + document_coverage * 10.0
        + path_coverage * 10.0
        + title_coverage * 10.0
        + definition_bonus
        - dispersion_penalty
        - size_penalty)
        .clamp(0.0, 100.0);
    let mut matched_mask = passage_mask | path_mask | title_mask;
    if matched_mask == 0 {
        matched_mask = body_mask;
    }
    let match_quality = if matched_mask.count_ones() >= minimum_required_terms(terms.len()) as u32 {
        RankedCoverage::StrongMatch
    } else {
        RankedCoverage::WeakMatch
    };
    let matched = terms_for_mask(matched_mask, terms);
    let (start, end) = bounded_passage_bounds(&lines, anchor, scope_start, scope_end, context);
    let text = lines[start..end].join("\n");
    // A passage is byte-bounded without cutting a source line. Huge lines
    // cannot provide a reliable compact citation and are omitted.
    if text.len() > MAX_RANKED_PASSAGE_BYTES {
        return None;
    }
    Some(RankedHit {
        host_id: host.into(),
        path: path.to_string_lossy().into_owned(),
        start_line: start + 1,
        end_line: end,
        line_number: anchor + 1,
        text,
        score,
        matched_terms: matched,
        match_quality,
        freshness: "source_verified".into(),
        body_sha256: format!("{:x}", Sha256::digest(body.as_bytes())),
        index_generation: generation,
    })
}

fn minimum_required_terms(term_count: usize) -> usize {
    if term_count <= 1 {
        term_count
    } else {
        term_count.saturating_mul(3).div_ceil(4)
    }
}

const MAX_RANKED_WINDOW_LINES: usize = 9;
const MAX_STRUCTURAL_PASSAGE_LINES: usize = 80;
const MAX_RANKED_PASSAGE_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy)]
struct PassageWindow {
    start: usize,
    end: usize,
    anchor: usize,
    mask: u32,
    definition: bool,
}

fn term_mask(text: &str, terms: &[String]) -> u32 {
    let tokens = indexed_tokens(text);
    terms.iter().enumerate().fold(0u32, |mask, (index, term)| {
        let found = term
            .strip_prefix("literal:")
            .map_or_else(|| tokens.contains(term), |literal| tokens.contains(literal));
        if found {
            mask | (1u32 << index)
        } else {
            mask
        }
    })
}

fn indexed_tokens(text: &str) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    for chunk in text.split_whitespace() {
        let structured =
            chunk.trim_matches(|c: char| !c.is_alphanumeric() && !"_./-=:@".contains(c));
        if structured.chars().any(|c| "_./-=:@".contains(c)) {
            tokens.insert(structured.to_lowercase());
        }
    }
    for word in text.split(|c: char| !c.is_alphanumeric() && c != '_') {
        if word.is_empty() {
            continue;
        }
        tokens.insert(word.to_lowercase());
        let chars: Vec<char> = word.chars().collect();
        let mut parts = Vec::new();
        let mut start = 0;
        for index in 0..chars.len() {
            if chars[index] == '_' {
                if start < index {
                    parts.push(
                        chars[start..index]
                            .iter()
                            .collect::<String>()
                            .to_lowercase(),
                    );
                }
                start = index + 1;
            } else if index > start
                && chars[index].is_uppercase()
                && (chars[index - 1].is_lowercase()
                    || chars[index - 1].is_numeric()
                    || (index + 1 < chars.len()
                        && chars[index + 1].is_lowercase()
                        && chars[index - 1].is_uppercase()))
            {
                parts.push(
                    chars[start..index]
                        .iter()
                        .collect::<String>()
                        .to_lowercase(),
                );
                start = index;
            }
        }
        if start < chars.len() {
            parts.push(chars[start..].iter().collect::<String>().to_lowercase());
        }
        for part in &parts {
            tokens.insert(part.clone());
        }
        // Keep contiguous identifier suffixes available to queries for a
        // component sequence inside a larger snake/camel identifier.
        for first in 0..parts.len() {
            let mut with_underscore = String::new();
            let mut compact = String::new();
            for part in parts.iter().skip(first).take(8) {
                if with_underscore.len() + part.len() + usize::from(!with_underscore.is_empty()) > 256 {
                    break;
                }
                if !with_underscore.is_empty() {
                    with_underscore.push('_');
                }
                with_underscore.push_str(part);
                compact.push_str(part);
                tokens.insert(with_underscore.clone());
                tokens.insert(compact.clone());
            }
        }
    }
    tokens
}

fn terms_for_mask(mask: u32, terms: &[String]) -> Vec<String> {
    terms
        .iter()
        .enumerate()
        .filter(|(index, _)| mask & (1u32 << index) != 0)
        .map(|(_, term)| term.strip_prefix("literal:").unwrap_or(term).to_string())
        .collect()
}

fn best_passage_window(lines: &[&str], line_masks: &[u32]) -> Option<PassageWindow> {
    let mut best: Option<PassageWindow> = None;
    for start in 0..line_masks.len() {
        let last = (start + MAX_RANKED_WINDOW_LINES).min(line_masks.len());
        let mut mask = 0u32;
        for end_exclusive in start + 1..=last {
            mask |= line_masks[end_exclusive - 1];
            let coverage = mask.count_ones();
            if coverage == 0 {
                continue;
            }
            let definition = (start..end_exclusive)
                .any(|index| line_masks[index] != 0 && is_definition_line(lines[index]));
            let anchor = (start..end_exclusive)
                .max_by_key(|index| {
                    (
                        line_masks[*index].count_ones(),
                        line_masks[*index] != 0 && is_definition_line(lines[*index]),
                        std::cmp::Reverse(*index),
                    )
                })
                .unwrap_or(start);
            let candidate = PassageWindow {
                start,
                end: end_exclusive,
                anchor,
                mask,
                definition,
            };
            let replace = best.is_none_or(|current| {
                coverage > current.mask.count_ones()
                    || (coverage == current.mask.count_ones() && definition && !current.definition)
                    || (coverage == current.mask.count_ones()
                        && definition == current.definition
                        && candidate.end - candidate.start < current.end - current.start)
                    || (coverage == current.mask.count_ones()
                        && definition == current.definition
                        && candidate.end - candidate.start == current.end - current.start
                        && candidate.start < current.start)
            });
            if replace {
                best = Some(candidate);
            }
        }
    }
    best
}

fn is_definition_line(line: &str) -> bool {
    let text = line.trim_start().to_lowercase();
    [
        "fn ",
        "pub fn ",
        "pub async fn ",
        "async fn ",
        "def ",
        "class ",
        "struct ",
        "enum ",
        "function ",
        "export function ",
    ]
    .iter()
    .any(|prefix| text.starts_with(prefix))
}

fn markdown_heading_level(line: &str) -> Option<usize> {
    let trimmed = line.trim_start();
    let level = trimmed.bytes().take_while(|byte| *byte == b'#').count();
    (1..=6)
        .contains(&level)
        .then(|| trimmed.as_bytes().get(level).copied() == Some(b' '))
        .filter(|valid| *valid)
        .map(|_| level)
}

fn structural_scope(lines: &[&str], anchor: usize) -> (usize, usize, Option<usize>) {
    let heading = (0..=anchor.min(lines.len().saturating_sub(1)))
        .rev()
        .find(|index| markdown_heading_level(lines[*index]).is_some());
    let definition = (0..=anchor.min(lines.len().saturating_sub(1)))
        .rev()
        .find(|index| is_definition_line(lines[*index]));

    if let Some(function_start) =
        definition.filter(|index| heading.is_none_or(|heading_index| *index > heading_index))
    {
        let end = function_scope_end(lines, function_start);
        return (function_start, end, None);
    }
    if let Some(heading_start) = heading {
        let level = markdown_heading_level(lines[heading_start]).unwrap_or(6);
        let end = ((heading_start + 1)..lines.len())
            .find(|index| markdown_heading_level(lines[*index]).is_some_and(|next| next <= level))
            .unwrap_or(lines.len());
        return (heading_start, end, Some(heading_start));
    }
    (anchor, (anchor + 1).min(lines.len()), None)
}

fn function_scope_end(lines: &[&str], start: usize) -> usize {
    let mut depth = 0usize;
    let mut opened = false;
    for (index, line) in lines
        .iter()
        .enumerate()
        .skip(start)
        .take(MAX_STRUCTURAL_PASSAGE_LINES)
    {
        for character in line.chars() {
            match character {
                '{' => {
                    opened = true;
                    depth = depth.saturating_add(1);
                }
                '}' if opened => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return index + 1;
                    }
                }
                _ => {}
            }
        }
        if !opened && index > start && is_definition_line(line) {
            return index;
        }
    }
    (start + MAX_STRUCTURAL_PASSAGE_LINES).min(lines.len())
}

fn bounded_passage_bounds(
    lines: &[&str],
    anchor: usize,
    scope_start: usize,
    scope_end: usize,
    context: usize,
) -> (usize, usize) {
    let mut start = scope_start.min(anchor);
    let mut end = scope_end.max(anchor + 1).min(lines.len());
    if !markdown_heading_level(lines[start]).is_some() {
        start = start.saturating_sub(context);
        end = end.saturating_add(context).min(lines.len());
    }
    if end.saturating_sub(start) > MAX_STRUCTURAL_PASSAGE_LINES {
        start = if anchor.saturating_sub(scope_start) < MAX_STRUCTURAL_PASSAGE_LINES {
            scope_start
        } else {
            anchor.saturating_sub(MAX_STRUCTURAL_PASSAGE_LINES - 1)
        };
        end = (start + MAX_STRUCTURAL_PASSAGE_LINES)
            .min(scope_end)
            .min(lines.len());
        if end <= anchor {
            end = anchor + 1;
            start = end
                .saturating_sub(MAX_STRUCTURAL_PASSAGE_LINES)
                .max(scope_start);
        }
    }
    let text_bytes = |from: usize, to: usize| {
        lines[from..to]
            .iter()
            .map(|line| line.len())
            .sum::<usize>()
            .saturating_add(to.saturating_sub(from).saturating_sub(1))
    };
    while end.saturating_sub(start) > 1 && text_bytes(start, end) > MAX_RANKED_PASSAGE_BYTES {
        if anchor.saturating_sub(start) >= end.saturating_sub(anchor + 1) && start < anchor {
            start += 1;
        } else if end > anchor + 1 {
            end -= 1;
        } else {
            break;
        }
    }
    if end <= start {
        start = anchor;
        end = (anchor + 1).min(lines.len());
    }
    if text_bytes(start, end) > MAX_RANKED_PASSAGE_BYTES {
        start = anchor.saturating_sub(context.min(MAX_STRUCTURAL_PASSAGE_LINES / 2));
        end = (anchor + context.min(MAX_STRUCTURAL_PASSAGE_LINES / 2) + 1).min(lines.len());
        while end.saturating_sub(start) > 1 && text_bytes(start, end) > MAX_RANKED_PASSAGE_BYTES {
            if anchor.saturating_sub(start) >= end.saturating_sub(anchor + 1) && start < anchor {
                start += 1;
            } else if end > anchor + 1 {
                end -= 1;
            } else {
                break;
            }
        }
    }
    (start, end)
}

#[cfg(test)]
mod tests {
    #[test]
    fn long_identifiers_have_bounded_composite_tokens() {
        let identifier = (0..128).map(|n| format!("part{n}")).collect::<Vec<_>>().join("_");
        let tokens = super::indexed_tokens(&identifier);
        assert!(tokens.len() < 128 * 18);
        assert!(tokens.contains(&identifier));
        assert!(tokens.contains("part0_part1"));
        assert!(!tokens.contains("part0_part1_part2_part3_part4_part5_part6_part7_part8"));
    }
    use super::*;
    #[test]
    fn ranked_terms_preserve_identifier_and_split_components() {
        let terms = query_terms("RemoteIndexStore remote_index_search AND \"escape\"").unwrap();
        for term in [
            "remoteindexstore",
            "remote",
            "index",
            "store",
            "remote_index_search",
            "search",
            "escape",
        ] {
            assert!(terms.contains(&term.to_owned()), "{term}");
        }
        assert!(query_terms("a и to").is_err());
        assert!(query_terms("MacBook-Pro-User.local")
            .unwrap()
            .contains(&"literal:macbook-pro-user.local".to_owned()));
        assert_eq!(
            query_terms("How does the Airlock bridge work?").unwrap(),
            vec!["airlock", "bridge", "work"]
        );
    }
    #[test]
    fn ranked_definition_beats_reference_and_context_has_exact_lines() {
        let terms = query_terms("index repair").unwrap();
        let definition = score_document(
            "local",
            std::path::Path::new("/repo/index.rs"),
            "// header\npub fn index_repair() {}\n// end\n",
            &terms,
            1,
            7,
        )
        .unwrap();
        let reference = score_document(
            "local",
            std::path::Path::new("/repo/reference.rs"),
            "index repair is used\n",
            &terms,
            1,
            7,
        )
        .unwrap();
        assert!(definition.score > reference.score);
        assert_eq!(
            (
                definition.start_line,
                definition.end_line,
                definition.line_number
            ),
            (1, 3, 2)
        );
    }

    #[test]
    fn ranked_compact_passage_beats_scattered_document_mentions() {
        let query = "S21 reverse SSH 22220 22221 lease";
        let terms = query_terms(query).unwrap();
        let focused = score_document(
            "local",
            std::path::Path::new("/repo/android.md"),
            "# Android\n\n## Reverse SSH and lease\nS21 reverse SSH keeps Termux on 22220 and ADB on 22221 under the same lease.\n",
            &terms,
            4,
            7,
        )
        .unwrap();
        let mut scattered =
            String::from("# Lease history\nThe lease remains with the phone owner.\n");
        for index in 0..40 {
            scattered.push_str(&format!("unrelated background note {index}\n"));
        }
        scattered.push_str(
            "## Device transport\nS21 reverse SSH forwards Termux on 22220 and ADB on 22221.\n",
        );
        let scattered = score_document(
            "local",
            std::path::Path::new("/repo/devices.md"),
            &scattered,
            &terms,
            4,
            7,
        )
        .unwrap();
        assert!(focused.score > scattered.score + 10.0);
        assert!(focused.matched_terms.contains(&"lease".to_string()));
        assert!(!scattered.matched_terms.contains(&"lease".to_string()));
        assert!(focused.text.contains("## Reverse SSH and lease"));
        assert!(scattered.text.contains("## Device transport"));
    }

    #[test]
    fn ranked_code_definition_beats_a_reference_and_keeps_function_lines() {
        let query = "rebuild_ranked_index";
        let terms = query_terms(query).unwrap();
        let definition = score_document(
            "local",
            std::path::Path::new("/repo/index.rs"),
            "## Indexing\nBefore the implementation.\n\npub fn rebuild_ranked_index() {\n    // implementation\n}\n\nLater notes.\n",
            &terms,
            0,
            9,
        )
        .unwrap();
        let reference = score_document(
            "local",
            std::path::Path::new("/repo/guide.md"),
            "## Usage\nCall rebuild_ranked_index when the index needs repair.\n",
            &terms,
            0,
            9,
        )
        .unwrap();
        assert!(definition.score > reference.score);
        assert_eq!(definition.start_line, 4);
        assert_eq!(definition.line_number, 4);
        assert!(definition.text.starts_with("pub fn rebuild_ranked_index"));
        assert!(definition.text.ends_with('}'));
    }

    #[test]
    fn ranked_markdown_scope_is_structural_and_bounded() {
        let mut body = String::from("# Inventory\nIntroductory summary.\n\n## Recovery route\n");
        for index in 0..95 {
            if index == 68 {
                body.push_str("S21 reverse SSH keeps the phone lease intact.\n");
            } else {
                body.push_str(&format!("bounded section detail {index}\n"));
            }
        }
        body.push_str("\n## Other route\nThis later section is outside the selected scope.\n");
        let lines: Vec<_> = body.lines().collect();
        let marker_line = lines
            .iter()
            .position(|line| line.contains("S21 reverse SSH"))
            .unwrap()
            + 1;
        let terms = query_terms("S21 reverse SSH lease").unwrap();
        let hit = score_document(
            "local",
            std::path::Path::new("/repo/android.md"),
            &body,
            &terms,
            0,
            10,
        )
        .unwrap();
        assert_eq!(hit.line_number, marker_line);
        assert!(hit.start_line <= marker_line && hit.end_line >= marker_line);
        assert!(hit.end_line - hit.start_line + 1 <= MAX_STRUCTURAL_PASSAGE_LINES);
        assert!(hit.text.contains("## Recovery route"));
        assert!(!hit.text.contains("## Other route"));
        assert!(hit.text.len() <= MAX_RANKED_PASSAGE_BYTES);
    }

    #[test]
    fn ranked_sparse_terms_are_retained_as_weak_and_pin_has_word_boundaries() {
        let query = "S21 unlock PIN";
        let terms = query_terms(query).unwrap();
        let mut body = String::from("## S21 unlock\nS21 unlock is a local phone action.\n");
        for index in 0..24 {
            body.push_str(&format!("unrelated device note {index}\n"));
        }
        body.push_str("## PIN reference\nPIN entry requires an operator.\n");
        let hit = score_document(
            "local",
            std::path::Path::new("/repo/android.md"),
            &body,
            &terms,
            4,
            11,
        )
        .unwrap();
        assert_eq!(hit.match_quality, RankedCoverage::WeakMatch);
        assert!(hit.matched_terms.iter().all(|term| term != "pin"));
        assert_eq!(term_mask("pipeline pins are nearby", &["pin".into()]), 0);
        assert_eq!(term_mask("PIN is required", &["pin".into()]), 1);
        let snake_terms = query_terms("mcp_servers").unwrap();
        let snake_anchor = snake_terms
            .iter()
            .position(|term| term == "literal:mcp_servers")
            .map(|i| 1u32 << i)
            .unwrap();
        assert_ne!(
            term_mask("agent_mcp_servers routes the bridge", &snake_terms) & snake_anchor,
            0
        );
        let composite_hit = score_document(
            "local",
            std::path::Path::new("/repo/config.rs"),
            "agent_mcp_servers routes requests through the configured bridge",
            &snake_terms,
            0,
            1,
        )
        .unwrap();
        let mut scattered = String::from("MCP server overview\n");
        for i in 0..20 {
            scattered.push_str(&format!("unrelated configuration note {i}\n"));
        }
        scattered.push_str("servers are configured separately\n");
        let scattered_hit = score_document(
            "local",
            std::path::Path::new("/repo/notes.rs"),
            &scattered,
            &snake_terms,
            0,
            1,
        )
        .unwrap();
        assert!(composite_hit.score > scattered_hit.score);
        assert_eq!(composite_hit.match_quality, RankedCoverage::StrongMatch);
        let host_terms = query_terms("MacBook-Pro-User.local").unwrap();
        let host_anchor = host_terms
            .iter()
            .position(|term| term == "literal:macbook-pro-user.local")
            .map(|i| 1u32 << i)
            .unwrap();
        let exact_host = score_document(
            "local",
            std::path::Path::new("/repo/hosts.md"),
            "SSH target: MacBook-Pro-User.local",
            &host_terms,
            0,
            1,
        )
        .unwrap();
        let component_only = score_document(
            "local",
            std::path::Path::new("/repo/notes.md"),
            "MacBook Pro User local is reachable by SSH",
            &host_terms,
            0,
            1,
        )
        .unwrap();
        assert!(term_mask(&exact_host.text, &host_terms) & host_anchor != 0);
        assert!(exact_host.score > component_only.score);
        let camel_terms = query_terms("repair index").unwrap();
        assert_eq!(
            term_mask("pub fn repairIndex() {}", &camel_terms).count_ones(),
            2
        );
        assert_eq!(minimum_required_terms(2), 2);
        assert_eq!(minimum_required_terms(3), 3);
        assert_eq!(minimum_required_terms(6), 5);
    }

    fn fixture_backend(
        root: &std::path::Path,
        documents: &[(&std::path::Path, &str)],
    ) -> LocalBackend {
        let mut backend = LocalBackend::new("test", root, crate::config::LimitsConfig::default());
        backend.index = crate::index::IndexManager::cached_test_index(
            root,
            root.join("index.sqlite"),
            documents,
        );
        backend
    }

    fn request(root: &std::path::Path, query: &str) -> RankedRequest {
        RankedRequest {
            query: query.into(),
            roots: vec![],
            read_roots: vec![root.display().to_string()],
            path_globs: vec!["*.rs".into()],
            limit: 3,
            context_lines: 1,
            overall_timeout: Duration::from_secs(5),
        }
    }

    #[tokio::test]
    async fn ranked_sqlite_acl_precedes_scoring_and_topk() {
        let temp = tempfile::tempdir().unwrap();
        let allowed = temp.path().join("allowed");
        fs::create_dir(&allowed).unwrap();
        let good = allowed.join("index.rs");
        let denied = temp.path().join("denied.rs");
        fs::write(&good, "pub fn index_repair() {}\n").unwrap();
        fs::write(
            &denied,
            "pub fn index_repair_index_repair_index_repair() {}\n",
        )
        .unwrap();
        let backend = fixture_backend(
            temp.path(),
            &[
                (&good, "pub fn index_repair() {}\n"),
                (
                    &denied,
                    "pub fn index_repair_index_repair_index_repair() {}\n",
                ),
            ],
        );
        let result = backend
            .ranked_search_bounded(request(&allowed, "index repair"))
            .await
            .unwrap();
        assert!(!result.partial, "{:?}", result.warnings);
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].path, good.display().to_string());
        assert_eq!(result.hits[0].freshness, "source_verified");
        assert!(!serde_json::to_string(&result)
            .unwrap()
            .contains("denied.rs"));
    }

    #[tokio::test]
    async fn ranked_sqlite_multiword_filename_and_stale_source() {
        let temp = tempfile::tempdir().unwrap();
        let definition = temp.path().join("repair_index.rs");
        let filename = temp.path().join("unique_needle.rs");
        fs::write(&definition, "pub fn repair_index() {}\n").unwrap();
        fs::write(&filename, "// implementation follows\n").unwrap();
        let backend = fixture_backend(
            temp.path(),
            &[
                (&definition, "pub fn repair_index() {}\n"),
                (&filename, "// implementation follows\n"),
            ],
        );
        let result = backend
            .ranked_search_bounded(request(temp.path(), "index repair"))
            .await
            .unwrap();
        assert_eq!(result.hits.len(), 1);
        let result = backend
            .ranked_search_bounded(request(temp.path(), "unique needle"))
            .await
            .unwrap();
        assert_eq!(
            result.hits.len(),
            1,
            "filename-only query must retrieve the source"
        );
        fs::write(&definition, "// deleted definition\n").unwrap();
        let result = backend
            .ranked_search_bounded(request(temp.path(), "index repair"))
            .await
            .unwrap();
        assert!(result.partial);
        assert!(result.hits.is_empty());
        let mut invalid = request(temp.path(), "index repair");
        invalid.roots = vec!["not-a-configured-root".into()];
        assert!(backend.ranked_search_bounded(invalid).await.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ranked_source_fifo_and_symlink_replacements_are_nonblocking_and_denied() {
        use std::os::unix::ffi::OsStrExt;

        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.rs");
        let original = "pub fn index_repair() {}\n";
        fs::write(&source, original).unwrap();
        let backend = fixture_backend(temp.path(), &[(&source, original)]);

        fs::remove_file(&source).unwrap();
        let c_path = std::ffi::CString::new(source.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        let fifo_result = tokio::time::timeout(
            Duration::from_secs(1),
            backend.ranked_search_bounded(request(temp.path(), "index repair")),
        )
        .await
        .expect("FIFO open must not block the async or blocking worker")
        .unwrap();
        assert!(fifo_result.partial);
        assert!(fifo_result.hits.is_empty());

        fs::remove_file(&source).unwrap();
        let outside = temp.path().join("outside.rs");
        fs::write(&outside, original).unwrap();
        std::os::unix::fs::symlink(&outside, &source).unwrap();
        let link_result = tokio::time::timeout(
            Duration::from_secs(1),
            backend.ranked_search_bounded(request(temp.path(), "index repair")),
        )
        .await
        .expect("symlink replacement must return promptly")
        .unwrap();
        assert!(link_result.partial);
        assert!(link_result.hits.is_empty());
    }

    #[tokio::test]
    async fn ranked_no_match_explains_abstention_without_marking_coverage_partial() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("android.md");
        fs::write(
            &source,
            "## Device access\nS21 unlock flow is documented.\n",
        )
        .unwrap();
        let backend = fixture_backend(
            temp.path(),
            &[(
                &source,
                "## Device access\nS21 unlock flow is documented.\n",
            )],
        );
        let mut req = request(temp.path(), "Matrix remote transport");
        req.path_globs = vec!["*.md".into()];
        let result = backend.ranked_search_bounded(req).await.unwrap();
        assert!(result.hits.is_empty());
        assert_eq!(result.coverage, RankedCoverage::NoCoverage);
        assert!(!result.partial);
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("no ranked coverage")));
    }

    #[tokio::test]
    async fn broad_natural_language_query_reports_strong_passage_coverage() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("bridge.md");
        let body = "## Remote MCP bridge\nThe remote MCP bridge routes each request to the selected host and returns the response.\n";
        fs::write(&source, body).unwrap();
        let backend = fixture_backend(temp.path(), &[(&source, body)]);
        let mut req = request(temp.path(), "How does a remote MCP bridge route a request?");
        req.path_globs = vec!["*.md".into()];
        let result = backend.ranked_search_bounded(req).await.unwrap();
        assert_eq!(result.coverage, RankedCoverage::StrongMatch);
        assert_eq!(result.hits[0].match_quality, RankedCoverage::StrongMatch);
    }
}
