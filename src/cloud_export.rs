//! One-pass export from an explicit, sorted JSONL inventory; never opens an index.
use crate::{config::AppConfig, ocr::OcrEngine, stt::SttEngine};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
};

const MAX_DOCUMENT: usize = 64 * 1024 * 1024;

fn record(out: &mut impl Write, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut *out, value)?;
    out.write_all(b"\n")?;
    Ok(())
}

/// Inventory contains one JSON string (canonical absolute file path) per line,
/// strictly increasing, so duplicates require no unbounded in-memory path set.
/// Bodies up to 64MiB are retained intact for refresh_source oversize routing.
pub fn run(config: &AppConfig, inventory: Option<&Path>) -> Result<()> {
    let mut roots = std::iter::once(&config.root)
        .chain(config.roots.values().flatten())
        .map(fs::canonicalize)
        .collect::<std::io::Result<Vec<_>>>()?;
    for root in &roots {
        fs::read_dir(root)
            .with_context(|| format!("configured root inaccessible: {}", root.display()))?;
    }
    roots.sort();
    roots.dedup();
    let all_roots = roots.clone();
    roots.retain(|root| {
        !all_roots
            .iter()
            .any(|other| other != root && root.starts_with(other))
    });
    let mut exclusions = globset::GlobSetBuilder::new();
    let mut patterns = crate::config::default_exclude_globs();
    patterns.extend(config.exclude_globs.clone());
    patterns.extend(config.limits.index_activity.exclude_globs.clone());
    for pattern in &patterns {
        exclusions.add(globset::Glob::new(pattern)?);
    }
    let exclusions = exclusions.build()?;
    let ocr = OcrEngine::new(runtime_ocr_config(config));
    let stt = SttEngine::new(config.stt.clone());
    let mut out = BufWriter::new(std::io::stdout().lock());
    record(
        &mut out,
        &json!({"kind":"begin","format":1,"source_host":config.host_id,
        "source_root":"/","policy":"readable stable source files; configured roots or explicit inventory; default/config/activity exclusions; symlink/missing/permission-denied/changed/oversized input skipped; native UTF-8/AnyDoc/OCR/STT; bodies <=64MiB including oversized; unsupported binaries excluded"}),
    )?;
    let mut previous = String::new();
    let mut count = 0u64;
    let mut skipped = BTreeMap::<String, u64>::new();
    let mut logical = Sha256::new();
    let mut input: Option<Box<dyn BufRead>> = match inventory {
        Some(path) if path == Path::new("-") => Some(Box::new(BufReader::new(std::io::stdin()))),
        Some(path) => Some(Box::new(BufReader::new(File::open(path)?))),
        None => None,
    };
    let mut pending = roots.iter().rev().cloned().collect::<Vec<_>>();
    loop {
        let name = if let Some(reader) = input.as_mut() {
            let mut line = String::new();
            if reader.read_line(&mut line)? == 0 {
                break;
            }
            let name: String = serde_json::from_str(&line)?;
            ensure!(
                name > previous,
                "inventory must be strictly sorted, unique paths"
            );
            previous = name.clone();
            name
        } else {
            let Some(path) = pending.pop() else {
                break;
            };
            path.to_str()
                .context("source path is not UTF-8")?
                .to_owned()
        };
        let path = Path::new(&name);
        ensure!(
            path.is_absolute() && roots.iter().any(|root| path.starts_with(root)),
            "inventory path is outside configured roots: {name}"
        );
        let is_root = roots.iter().any(|root| root == path);
        let Some(meta) = readable(fs::symlink_metadata(path), &mut skipped, is_root)
            .with_context(|| format!("metadata {name}"))?
        else {
            continue;
        };
        if meta.file_type().is_symlink() || (!meta.is_file() && !meta.is_dir()) {
            *skipped.entry("symlink_or_special".into()).or_default() += 1;
            continue;
        }
        if exclusions.is_match(path)
            || (meta.is_dir() && exclusions.is_match(format!("{name}/")))
            || roots
                .iter()
                .filter_map(|root| path.strip_prefix(root).ok())
                .any(|rel| exclusions.is_match(rel))
        {
            *skipped.entry("configured_exclusion".into()).or_default() += 1;
            continue;
        }
        if meta.is_dir() {
            ensure!(
                input.is_none(),
                "explicit inventory must contain files: {name}"
            );
            let Some(entries) = readable(fs::read_dir(path), &mut skipped, is_root)
                .with_context(|| format!("walk {name}"))?
            else {
                continue;
            };
            let mut children = Vec::new();
            for entry in entries {
                if let Some(entry) = readable(entry, &mut skipped, is_root)? {
                    children.push(entry.path());
                }
            }
            children.sort_by(|a, b| b.cmp(a));
            pending.extend(children);
            continue;
        }
        let Some(canonical) = readable(fs::canonicalize(path), &mut skipped, false)? else {
            continue;
        };
        ensure!(canonical == path, "noncanonical source path: {name}");
        // Test source readability here, so a missing OCR model/remote failure is not a policy skip.
        let Some(_) = readable(File::open(path), &mut skipped, false)? else {
            continue;
        };
        let input_limit = if let Some(e) = stt.as_ref().filter(|e| e.is_media(path)) {
            e.max_media_bytes()
        } else if let Some(e) = ocr.as_ref().filter(|e| e.is_image(path) || e.is_pdf(path)) {
            e.max_image_bytes()
        } else {
            config.limits.max_file_bytes
        };
        if input_limit == 0 || meta.len() > input_limit {
            *skipped.entry("input_size_policy".into()).or_default() += 1;
            continue;
        }
        let started = std::time::Instant::now();
        let mut retries = 0;
        let extraction = loop {
            let result = extract(config, ocr.as_ref(), stt.as_ref(), path, &meta);
            let retry = result.as_ref().err().is_some_and(transient_mesh_error);
            if !retry || retries >= 15 || started.elapsed() >= std::time::Duration::from_secs(30) {
                break result;
            }
            retries += 1;
            tracing::info!(path = %path.display(), retries, "mesh OCR busy; retrying same source file");
            std::thread::sleep(
                std::time::Duration::from_secs(2)
                    .min(std::time::Duration::from_secs(30).saturating_sub(started.elapsed())),
            );
        };
        let body = match extraction {
            Ok(Some(body)) => body,
            Ok(None) => {
                *skipped.entry("unsupported_binary".into()).or_default() += 1;
                continue;
            }
            Err(error) => {
                let Some(now) = readable(fs::metadata(path), &mut skipped, false)? else {
                    continue;
                };
                if meta.len() != now.len() || meta.modified()? != now.modified()? {
                    *skipped
                        .entry("changed_during_extraction".into())
                        .or_default() += 1;
                    continue;
                }
                if readable(File::open(path), &mut skipped, false)?.is_none() {
                    continue;
                }
                return Err(error).with_context(|| format!("extract {name}"));
            }
        };
        let Some(after) = readable(fs::metadata(path), &mut skipped, false)? else {
            continue;
        };
        if meta.len() != after.len() || meta.modified()? != after.modified()? {
            *skipped
                .entry("changed_during_extraction".into())
                .or_default() += 1;
            continue;
        }
        let sha = format!("{:x}", Sha256::digest(body.as_bytes()));
        let mtime = meta
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let doc = json!({"kind":"document","path":name,"body":body,"size":meta.len(),
            "mtime_ns":mtime,"sha256":sha});
        ensure!(
            body.len() <= MAX_DOCUMENT,
            "extracted body exceeds 64MiB: {name}"
        );
        logical.update(serde_json::to_vec(&json!([name, sha]))?);
        record(&mut out, &doc)?;
        count += 1;
    }
    record(
        &mut out,
        &json!({"kind":"end","count":count,
        "logical_sha256":format!("{:x}",logical.finalize()),
        "complete_for_policy":true,"skipped":skipped}),
    )?;
    out.flush()?;
    Ok(())
}

fn extract(
    config: &AppConfig,
    ocr: Option<&OcrEngine>,
    stt: Option<&SttEngine>,
    path: &Path,
    meta: &fs::Metadata,
) -> Result<Option<String>> {
    let name = path.display();
    let body = if let Some(engine) = stt.filter(|e| e.is_media(path)) {
        ensure!(
            engine.max_media_bytes() != 0 && meta.len() <= engine.max_media_bytes(),
            "media exceeds bounded extraction policy: {name}"
        );
        engine.transcribe(path)?
    } else if let Some(engine) = ocr.filter(|e| e.is_image(path)) {
        ensure!(
            engine.max_image_bytes() != 0 && meta.len() <= engine.max_image_bytes(),
            "image exceeds bounded extraction policy: {name}"
        );
        engine.extract_image(path)?
    } else {
        let pdf = ocr.filter(|e| e.is_pdf(path));
        let limit = pdf
            .map(OcrEngine::max_image_bytes)
            .unwrap_or(config.limits.max_file_bytes);
        ensure!(
            limit != 0 && meta.len() <= limit,
            "source exceeds bounded extraction policy: {name}"
        );
        let mut bytes = Vec::new();
        File::open(path)?
            .take(limit.saturating_add(1))
            .read_to_end(&mut bytes)?;
        let text = if !bytes.contains(&0) && std::str::from_utf8(&bytes).is_ok() {
            Some(String::from_utf8(bytes)?)
        } else if anydoc::Format::from_path(path).is_some() {
            Some(anydoc::to_markdown(path).with_context(|| format!("extract {name}"))?)
        } else {
            None
        };
        if let Some(engine) = pdf.filter(|e| e.should_ocr_pdf(text.as_deref())) {
            engine.extract_pdf(path)?
        } else if let Some(text) = text {
            text
        } else {
            return Ok(None);
        }
    };
    Ok(Some(body))
}

fn readable<T>(
    result: std::io::Result<T>,
    skipped: &mut BTreeMap<String, u64>,
    root: bool,
) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error)
            if !root
                && matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
                ) =>
        {
            let reason = if error.kind() == std::io::ErrorKind::NotFound {
                "disappeared"
            } else {
                "permission_denied"
            };
            *skipped.entry(reason.into()).or_default() += 1;
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

fn runtime_ocr_config(config: &AppConfig) -> crate::config::OcrConfig {
    let mut ocr = config.ocr.clone();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let mut topology = config
        .topology_cache_path
        .as_deref()
        .and_then(|path| crate::topology_cache::TopologySnapshot::load(path).ok())
        .and_then(|snapshot| crate::topology::Topology::from_snapshot(snapshot, now).ok())
        .unwrap_or_else(|| {
            crate::topology::Topology::new(config.host_id.clone(), config.peers.clone())
        });
    topology.retain_known_peers(&config.peers);
    ocr.mesh_peers = crate::compute_client::merge_compute_peers(topology, &ocr.mesh_peers);
    if config.compute.enabled {
        let address = config.local_bind.unwrap_or_else(|| {
            if config.bind.ip().is_unspecified() {
                std::net::SocketAddr::from(([127, 0, 0, 1], config.bind.port()))
            } else {
                config.bind
            }
        });
        let url = format!("http://{address}/mcp");
        ocr.mesh_peers.push(crate::topology::PeerConfig {
            host_id: config.host_id.clone(),
            local_url: url.clone(),
            routable_url: url,
            gptadmin_proxy_url: None,
            gptadmin_relay_url: None,
        });
    }
    ocr.mesh_topology_cache_path = config.topology_cache_path.clone();
    ocr.mesh_peer_token_env = config.peer_auth_token_env.clone();
    ocr.mesh_relay_token_env = config.gptadmin_token_env.clone();
    ocr.mesh_relay_origin = config.gptadmin_topology_url.clone();
    ocr
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exporter_inherits_runtime_ocr_routes_auth_and_self_worker() {
        let config: AppConfig = serde_json::from_value(json!({
            "host_id":"source","root":"/source","bind":"0.0.0.0:9419",
            "local_bind":"127.0.0.1:9420",
            "peers":[{"host_id":"worker","local_url":"http://worker:9419/mcp","routable_url":"http://worker:9419/mcp"}],
            "topology_cache_path":"/cache/topology.json",
            "peer_auth_token_env":"PEER_AUTH","gptadmin_token_env":"RELAY_AUTH",
            "gptadmin_topology_url":"https://hub.example/topology",
            "compute":{"enabled":true},
            "ocr":{"enabled":true,"backend":"mesh"}
        })).unwrap();
        let ocr = runtime_ocr_config(&config);
        assert!(ocr.mesh_peers.iter().any(|p| p.host_id == "worker"));
        assert!(ocr
            .mesh_peers
            .iter()
            .any(|p| p.host_id == "source" && p.local_url == "http://127.0.0.1:9420/mcp"));
        assert_eq!(ocr.mesh_peer_token_env, config.peer_auth_token_env);
        assert_eq!(ocr.mesh_relay_token_env, config.gptadmin_token_env);
        assert_eq!(ocr.mesh_relay_origin, config.gptadmin_topology_url);
        assert_eq!(ocr.mesh_topology_cache_path, config.topology_cache_path);
    }
}

fn transient_mesh_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.to_string().as_str(),
            "mesh OCR deferred: no available OCR worker"
                | "mesh OCR deferred: no available OCR worker completed extraction"
                | "OCR worker temporarily busy"
        )
    })
}

#[cfg(test)]
#[test]
fn export_retries_only_transient_mesh_admission() {
    assert!(transient_mesh_error(&anyhow::anyhow!(
        "mesh OCR deferred: no available OCR worker"
    )));
    assert!(transient_mesh_error(&anyhow::anyhow!(
        "mesh OCR deferred: no available OCR worker completed extraction"
    )));
    assert!(transient_mesh_error(&anyhow::anyhow!(
        "OCR worker temporarily busy"
    )));
    assert!(!transient_mesh_error(&anyhow::anyhow!(
        "OCR processing failed"
    )));
    assert!(!transient_mesh_error(&anyhow::anyhow!(
        "mesh OCR input is not decodable"
    )));
}
