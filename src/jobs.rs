use crate::{
    backend::{PerHostStatus, SearchHit},
    config::LimitsConfig,
    mcp::{HostsInput, MeshService, SearchArgs},
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const MAX_JOBS: usize = 256;
const MAX_CURSORS_PER_JOB: usize = 256;
const DEFAULT_PAGE_SIZE: usize = 32;
const MAX_PAGE_SIZE: usize = 64;
const NEXT_POLL_AFTER_MS: u64 = 30_000;

#[derive(Clone)]
pub struct SearchJobs {
    inner: Arc<Mutex<BTreeMap<String, SearchJob>>>,
    persistence: Option<Arc<JobPersistence>>,
}

struct JobPersistence {
    dir: PathBuf,
    ttl_ms: u64,
    max_bytes: u64,
    store_max_bytes: u64,
}

struct SearchJob {
    created_ms: u64,
    host_id: String,
    verbose: bool,
    limit: usize,
    pending_hosts: BTreeSet<String>,
    host_status: BTreeMap<String, PerHostStatus>,
    results: Vec<SearchHit>,
    seen_results: BTreeSet<(String, String, usize)>,
    truncated: bool,
    cursors: BTreeMap<String, usize>,
    lost: bool,
    durability_error: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct PersistedSearchJob {
    created_ms: u64,
    host_id: String,
    verbose: bool,
    limit: usize,
    pending_hosts: BTreeSet<String>,
    host_status: BTreeMap<String, PerHostStatus>,
    results: Vec<SearchHit>,
    truncated: bool,
    cursors: BTreeMap<String, usize>,
    lost: bool,
    durability_error: Option<String>,
}

impl Default for SearchJobs {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BTreeMap::new())),
            persistence: None,
        }
    }
}

impl SearchJobs {
    pub fn persistent(root: PathBuf, limits: &LimitsConfig) -> Result<Self> {
        let dir = root.join(".grepmesh-jobs");
        fs::create_dir_all(&dir).map_err(|error| {
            anyhow::anyhow!("create private search job store {}: {error}", dir.display())
        })?;
        set_private_dir_permissions(&dir)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(BTreeMap::new())),
            persistence: Some(Arc::new(JobPersistence {
                dir,
                ttl_ms: limits.search_job_ttl_ms.max(1_000),
                max_bytes: limits.search_job_max_bytes.max(4 * 1024),
                store_max_bytes: limits.search_job_store_max_bytes.max(4 * 1024),
            })),
        })
    }

    pub fn start(&self, service: MeshService, args: SearchArgs) -> Result<String> {
        let job_id = fresh_handle("job")?;
        let targets = service.resolve_search_hosts(&args)?;
        let limit = args
            .limit
            .unwrap_or(service.local.limits.max_results)
            .min(service.local.limits.max_results);
        {
            let mut jobs = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("search job lock poisoned"))?;
            self.prune(&mut jobs)?;
            jobs.insert(
                job_id.clone(),
                SearchJob {
                    created_ms: now_ms(),
                    host_id: service.local.host_id.clone(),
                    verbose: args.verbose,
                    limit,
                    pending_hosts: targets.iter().cloned().collect(),
                    host_status: BTreeMap::new(),
                    results: Vec::new(),
                    seen_results: BTreeSet::new(),
                    truncated: false,
                    cursors: BTreeMap::new(),
                    lost: false,
                    durability_error: None,
                },
            );
            self.persist(&job_id, jobs.get_mut(&job_id).expect("inserted job"))?;
        }

        for target in targets {
            let jobs = self.clone();
            let completed_job_id = job_id.clone();
            let service = service.clone();
            let mut host_args = args.clone();
            host_args.hosts = Some(HostsInput::One(target.clone()));
            // Each host call must have its own mesh request id. A shared id is
            // rejected by the existing request-loop guard.
            host_args.request_id = None;
            host_args.origin_host = None;
            host_args.hop_count = None;
            host_args.verbose = true;
            tokio::spawn(async move {
                let timeout = Duration::from_millis(service.local.limits.search_job_timeout_ms);
                let outcome = tokio::time::timeout(
                    timeout,
                    service.call_search_with_overall_timeout(host_args, timeout),
                )
                .await
                .map_err(|_| anyhow::anyhow!("search job deadline exceeded"))
                .and_then(|result| result);
                let job_store = jobs.clone();
                if let Ok(mut entries) = job_store.inner.lock() {
                    let Some(job) = entries.get_mut(&completed_job_id) else {
                        return;
                    };
                    job.pending_hosts.remove(&target);
                    match outcome {
                        Ok(result) => Self::merge_result(job, result.data),
                        Err(error) => {
                            job.host_status.insert(
                                target.clone(),
                                PerHostStatus {
                                    host_id: target,
                                    ok: false,
                                    error: Some(error.to_string()),
                                },
                            );
                        }
                    }
                    if let Err(error) = job_store.persist(&completed_job_id, job) {
                        job.durability_error =
                            Some(format!("search job durability failure: {error}"));
                        job.pending_hosts.clear();
                    }
                };
            });
        }
        Ok(job_id)
    }

    pub async fn wait(&self, job_id: &str, budget: Duration) {
        let deadline = Instant::now() + budget;
        while Instant::now() < deadline {
            if !self.is_running(job_id) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    pub fn is_verbose(&self, job_id: &str) -> Result<bool> {
        let mut jobs = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("search job lock poisoned"))?;
        self.prune(&mut jobs)?;
        Ok(jobs.get(job_id).map(|job| job.verbose).unwrap_or(false))
    }

    pub fn status(
        &self,
        job_id: &str,
        cursor: Option<&str>,
        page_size: Option<usize>,
    ) -> Result<Value> {
        let mut jobs = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("search job lock poisoned"))?;
        self.prune(&mut jobs)?;
        if !jobs.contains_key(job_id) {
            if let Some(mut restored) = self.load(job_id)? {
                // A process restart cannot resume an in-flight task. Preserve
                // completed data and make the loss explicit rather than lying.
                if !restored.pending_hosts.is_empty() {
                    restored.lost = true;
                    for host_id in std::mem::take(&mut restored.pending_hosts) {
                        restored.host_status.insert(
                            host_id.clone(),
                            PerHostStatus {
                                host_id,
                                ok: false,
                                error: Some("search interrupted by service restart".into()),
                            },
                        );
                    }
                }
                jobs.insert(job_id.to_string(), restored);
            }
        }
        let Some(job) = jobs.get_mut(job_id) else {
            return Ok(json!({
                "state": "expired", "lost": true, "job_id": job_id,
                "error": "search job expired or was lost; start the search again"
            }));
        };
        let start = match cursor {
            Some(cursor) => *job
                .cursors
                .get(cursor)
                .ok_or_else(|| anyhow::anyhow!("invalid cursor for search job {job_id}"))?,
            None => 0,
        };
        let page_size = page_size
            .unwrap_or(DEFAULT_PAGE_SIZE)
            .clamp(1, MAX_PAGE_SIZE);
        let running = !job.pending_hosts.is_empty();
        // Contract: stable arrival-first limit. The first `limit` unique
        // hits are admitted and never replaced by later hosts, so a running
        // batch cannot contradict a final batch.
        let mut end = start.min(job.results.len());
        let mut page = Vec::new();
        while end < job.results.len() && page.len() < page_size {
            let hit = &job.results[end];
            end += 1;
            page.push(hit.clone());
        }
        let next_cursor = fresh_handle("page")?;
        // A cursor is returned while running even when there is no new result,
        // so the next poll cannot replay an earlier batch.
        if running || end < job.results.len() {
            job.cursors.insert(next_cursor.clone(), end);
            while job.cursors.len() > MAX_CURSORS_PER_JOB {
                if let Some(oldest) = job.cursors.keys().next().cloned() {
                    job.cursors.remove(&oldest);
                }
            }
        }
        let host_status = job.host_status.values().cloned().collect::<Vec<_>>();
        let partial = running || host_status.iter().any(|status| !status.ok);
        let failed = job.durability_error.is_some()
            || !running && !host_status.is_empty() && host_status.iter().all(|status| !status.ok);
        let mut data = json!({
            "state": if running { "running" } else if job.lost { "lost" } else if failed { "failed" } else { "complete" },
            "job_id": job_id,
            "request_id": job_id,
            "origin_host": job.host_id,
            "hop_count": 0,
            "host_id": job.host_id,
            "partial": partial,
            "truncated": job.truncated,
            "results": page,
            "matches": page,
            "host_status": host_status,
            "pending_hosts": job.pending_hosts.iter().cloned().collect::<Vec<_>>(),
        });
        if running || partial {
            data["artifact_id"] = Value::String(job_id.into());
        }
        if let Some(error) = &job.durability_error {
            data["error"] = Value::String(error.clone());
        }
        if running {
            data["next_poll_after_ms"] = Value::from(NEXT_POLL_AFTER_MS);
            data["message"] = Value::String(
                "Search continues; poll again in 30 seconds. Поиск продолжается; повторите запрос через 30 секунд.".into(),
            );
        }
        if running || end < job.results.len() {
            data["cursor"] = Value::String(next_cursor);
        }
        if let Err(error) = self.persist(job_id, job) {
            job.durability_error = Some(format!("search job durability failure: {error}"));
            job.pending_hosts.clear();
            data["state"] = Value::String("failed".into());
            data["partial"] = Value::Bool(true);
            data["artifact_id"] = Value::String(job_id.into());
            data["error"] = Value::String(job.durability_error.clone().unwrap_or_default());
        }
        Ok(data)
    }

    fn merge_result(job: &mut SearchJob, data: Value) {
        job.truncated |= data
            .get("truncated")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if let Some(statuses) = data.get("host_status").and_then(Value::as_array) {
            for status in statuses {
                if let Ok(status) = serde_json::from_value::<PerHostStatus>(status.clone()) {
                    job.host_status.insert(status.host_id.clone(), status);
                }
            }
        }
        if let Some(results) = data.get("results").and_then(Value::as_array) {
            for result in results {
                let Ok(hit) = serde_json::from_value::<SearchHit>(result.clone()) else {
                    continue;
                };
                let key = (hit.host_id.clone(), hit.path.clone(), hit.line_number);
                if job.seen_results.insert(key) && job.results.len() < job.limit {
                    job.results.push(hit);
                } else {
                    job.truncated = true;
                }
            }
        }
        // Arrival order is an append-only watermark and the admission rule.
    }

    pub fn is_running(&self, job_id: &str) -> bool {
        self.inner
            .lock()
            .ok()
            .and_then(|jobs| jobs.get(job_id).map(|job| !job.pending_hosts.is_empty()))
            .unwrap_or(false)
    }

    fn prune(&self, jobs: &mut BTreeMap<String, SearchJob>) -> Result<()> {
        let now = now_ms();
        let ttl_ms = self
            .persistence
            .as_ref()
            .map(|store| store.ttl_ms)
            .unwrap_or(300_000);
        let expired = jobs
            .iter()
            .filter(|(_, job)| now.saturating_sub(job.created_ms) >= ttl_ms)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in expired {
            jobs.remove(&id);
            self.delete_artifact(&id)?;
        }
        while jobs.len() > MAX_JOBS {
            let oldest = jobs
                .iter()
                .min_by_key(|(_, job)| job.created_ms)
                .map(|(id, _)| id.clone());
            if let Some(id) = oldest {
                jobs.remove(&id);
                self.delete_artifact(&id)?;
            } else {
                break;
            }
        }
        self.cleanup_artifacts(now, ttl_ms)?;
        Ok(())
    }

    fn persist(&self, job_id: &str, job: &mut SearchJob) -> Result<()> {
        let Some(store) = self.persistence.as_ref() else {
            return Ok(());
        };
        loop {
            let record = PersistedSearchJob::from_job(job);
            let bytes = serde_json::to_vec(&record)?;
            if bytes.len() as u64 <= store.max_bytes || job.results.is_empty() {
                if bytes.len() as u64 > store.max_bytes {
                    return Err(anyhow::anyhow!(
                        "search job artifact exceeds configured maximum"
                    ));
                }
                let final_path = artifact_path(store, job_id)?;
                let tmp_path = store
                    .dir
                    .join(format!(".{job_id}.{}.tmp", fresh_handle("write")?));
                write_private_file(&tmp_path, &bytes)?;
                fs::rename(tmp_path, final_path)?;
                self.enforce_store_budget()?;
                return Ok(());
            }
            job.results.pop();
            job.truncated = true;
        }
    }

    fn load(&self, job_id: &str) -> Result<Option<SearchJob>> {
        let Some(store) = self.persistence.as_ref() else {
            return Ok(None);
        };
        let path = artifact_path(store, job_id)?;
        if !path.exists() {
            return Ok(None);
        }
        let record: PersistedSearchJob = serde_json::from_slice(&fs::read(path)?)?;
        if now_ms().saturating_sub(record.created_ms) >= store.ttl_ms {
            self.delete_artifact(job_id)?;
            return Ok(None);
        }
        Ok(Some(record.into_job()))
    }

    fn delete_artifact(&self, job_id: &str) -> Result<()> {
        let Some(store) = self.persistence.as_ref() else {
            return Ok(());
        };
        let path = artifact_path(store, job_id)?;
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    fn cleanup_artifacts(&self, now: u64, ttl_ms: u64) -> Result<()> {
        let Some(store) = self.persistence.as_ref() else {
            return Ok(());
        };
        for entry in fs::read_dir(&store.dir)? {
            let path = entry?.path();
            if path.extension().and_then(|part| part.to_str()) != Some("json") {
                continue;
            }
            let expired = fs::read(&path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<PersistedSearchJob>(&bytes).ok())
                .is_some_and(|job| now.saturating_sub(job.created_ms) >= ttl_ms);
            if expired {
                let _ = fs::remove_file(path);
            }
        }
        Ok(())
    }

    fn enforce_store_budget(&self) -> Result<()> {
        let Some(store) = self.persistence.as_ref() else {
            return Ok(());
        };
        let mut artifacts = fs::read_dir(&store.dir)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let path = entry.path();
                let bytes = fs::read(&path).ok()?;
                let record = serde_json::from_slice::<PersistedSearchJob>(&bytes).ok()?;
                Some((record.created_ms, path, bytes.len() as u64))
            })
            .collect::<Vec<_>>();
        artifacts.sort_by_key(|(created_ms, path, _)| (*created_ms, path.clone()));
        let mut total = artifacts.iter().map(|(_, _, bytes)| *bytes).sum::<u64>();
        while artifacts.len() > MAX_JOBS || total > store.store_max_bytes {
            let (_, path, bytes) = artifacts.remove(0);
            fs::remove_file(path)?;
            total = total.saturating_sub(bytes);
        }
        Ok(())
    }
}

impl PersistedSearchJob {
    fn from_job(job: &SearchJob) -> Self {
        Self {
            created_ms: job.created_ms,
            host_id: job.host_id.clone(),
            verbose: job.verbose,
            limit: job.limit,
            pending_hosts: job.pending_hosts.clone(),
            host_status: job.host_status.clone(),
            results: job.results.clone(),
            truncated: job.truncated,
            cursors: job.cursors.clone(),
            lost: job.lost,
            durability_error: job.durability_error.clone(),
        }
    }
    fn into_job(self) -> SearchJob {
        let seen_results = self
            .results
            .iter()
            .map(|hit| (hit.host_id.clone(), hit.path.clone(), hit.line_number))
            .collect();
        SearchJob {
            created_ms: self.created_ms,
            host_id: self.host_id,
            verbose: self.verbose,
            limit: self.limit,
            pending_hosts: self.pending_hosts,
            host_status: self.host_status,
            results: self.results,
            seen_results,
            truncated: self.truncated,
            cursors: self.cursors,
            lost: self.lost,
            durability_error: self.durability_error,
        }
    }
}

fn artifact_path(store: &JobPersistence, job_id: &str) -> Result<PathBuf> {
    if !job_id.starts_with("job-")
        || !job_id
            .chars()
            .all(|char| char.is_ascii_alphanumeric() || char == '-')
    {
        return Err(anyhow::anyhow!("invalid search job id"));
    }
    Ok(store.dir.join(format!("{job_id}.json")))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_bookkeeping_is_bounded_and_artifacts_expire() {
        let temp = tempfile::tempdir().unwrap();
        let limits = LimitsConfig {
            search_job_ttl_ms: 1,
            ..Default::default()
        };
        let jobs = SearchJobs::persistent(temp.path().to_path_buf(), &limits).unwrap();
        let job_id = "job-deadbeef-1";
        let mut entries = jobs.inner.lock().unwrap();
        entries.insert(
            job_id.into(),
            SearchJob {
                created_ms: now_ms().saturating_sub(1_001),
                host_id: "A".into(),
                verbose: true,
                limit: 10,
                pending_hosts: BTreeSet::from(["A".into()]),
                host_status: BTreeMap::new(),
                results: Vec::new(),
                seen_results: BTreeSet::new(),
                truncated: false,
                cursors: (0..(MAX_CURSORS_PER_JOB + 8))
                    .map(|index| (format!("page-{index:04}"), index))
                    .collect(),
                lost: false,
                durability_error: None,
            },
        );
        let job = entries.get_mut(job_id).unwrap();
        jobs.persist(job_id, job).unwrap();
        drop(entries);

        let expired = jobs.status(job_id, None, None).unwrap();
        assert_eq!(expired["state"], "expired");
        let artifact = temp
            .path()
            .join(".grepmesh-jobs")
            .join(format!("{job_id}.json"));
        assert!(!artifact.exists());
    }

    #[test]
    fn polling_flood_keeps_only_the_cursor_cap() {
        let jobs = SearchJobs::default();
        let job_id = "job-deadbeef-2";
        let mut entries = jobs.inner.lock().unwrap();
        entries.insert(
            job_id.into(),
            SearchJob {
                created_ms: now_ms(),
                host_id: "A".into(),
                verbose: true,
                limit: 10,
                pending_hosts: BTreeSet::from(["A".into()]),
                host_status: BTreeMap::new(),
                results: Vec::new(),
                seen_results: BTreeSet::new(),
                truncated: false,
                cursors: BTreeMap::new(),
                lost: false,
                durability_error: None,
            },
        );
        drop(entries);
        for _ in 0..(MAX_CURSORS_PER_JOB + 32) {
            let status = jobs.status(job_id, None, None).unwrap();
            assert_eq!(status["state"], "running");
        }
        assert!(jobs.inner.lock().unwrap()[job_id].cursors.len() <= MAX_CURSORS_PER_JOB);
    }

    #[test]
    fn limit_is_stable_arrival_first_across_running_and_final_deltas() {
        let jobs = SearchJobs::default();
        let job_id = "job-deadbeef-3";
        let mut job = SearchJob {
            created_ms: now_ms(),
            host_id: "B".into(),
            verbose: true,
            limit: 1,
            pending_hosts: BTreeSet::from(["B".into(), "A".into()]),
            host_status: BTreeMap::new(),
            results: Vec::new(),
            seen_results: BTreeSet::new(),
            truncated: false,
            cursors: BTreeMap::new(),
            lost: false,
            durability_error: None,
        };
        SearchJobs::merge_result(
            &mut job,
            json!({"results": [{"host_id":"B","path":"/z","line_number":1,"column":1,"text":"early","context":[]}]}),
        );
        jobs.inner.lock().unwrap().insert(job_id.into(), job);
        let first = jobs.status(job_id, None, None).unwrap();
        assert_eq!(first["results"][0]["host_id"], "B");
        let cursor = first["cursor"].as_str().unwrap().to_string();
        let mut entries = jobs.inner.lock().unwrap();
        let job = entries.get_mut(job_id).unwrap();
        job.pending_hosts.clear();
        SearchJobs::merge_result(
            job,
            json!({"results": [{"host_id":"A","path":"/a","line_number":1,"column":1,"text":"late","context":[]}]}),
        );
        drop(entries);
        let final_delta = jobs.status(job_id, Some(&cursor), None).unwrap();
        assert_eq!(final_delta["state"], "complete");
        assert!(final_delta["results"].as_array().unwrap().is_empty());
        assert_eq!(jobs.inner.lock().unwrap()[job_id].results.len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn private_store_and_artifact_modes_ignore_umask() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let jobs =
            SearchJobs::persistent(temp.path().to_path_buf(), &LimitsConfig::default()).unwrap();
        let job_id = "job-deadbeef-4";
        let mut entries = jobs.inner.lock().unwrap();
        entries.insert(
            job_id.into(),
            SearchJob {
                created_ms: now_ms(),
                host_id: "A".into(),
                verbose: true,
                limit: 1,
                pending_hosts: BTreeSet::new(),
                host_status: BTreeMap::new(),
                results: Vec::new(),
                seen_results: BTreeSet::new(),
                truncated: false,
                cursors: BTreeMap::new(),
                lost: false,
                durability_error: None,
            },
        );
        jobs.persist(job_id, entries.get_mut(job_id).unwrap())
            .unwrap();
        drop(entries);
        let dir = temp.path().join(".grepmesh-jobs");
        let file = dir.join(format!("{job_id}.json"));
        assert_eq!(
            fs::metadata(dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(file).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

fn fresh_handle(prefix: &str) -> Result<String> {
    use std::io::Read;
    let mut bytes = [0u8; 24];
    fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(format!(
        "{prefix}-{}",
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))
}

#[cfg(unix)]
fn set_private_dir_permissions(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_dir_permissions(_path: &std::path::Path) -> Result<()> {
    Ok(())
}

fn write_private_file(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}
