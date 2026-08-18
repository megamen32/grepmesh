use crate::{
    backend::PerHostStatus,
    mcp::{HostsInput, MeshService, SearchArgs, ToolResult},
};
use anyhow::Result;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const JOB_TTL: Duration = Duration::from_secs(300);
const DEFAULT_PAGE_SIZE: usize = 32;
const MAX_PAGE_SIZE: usize = 64;
static HANDLE_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Default)]
pub struct SearchJobs {
    inner: Arc<Mutex<BTreeMap<String, SearchJob>>>,
}

struct SearchJob {
    started: Instant,
    partial: Value,
    result: Option<ToolResult>,
    error: Option<String>,
    cursors: BTreeMap<String, usize>,
}

impl SearchJobs {
    pub fn start(&self, service: MeshService, args: SearchArgs) -> String {
        let job_id = fresh_handle("job");
        if let Ok(mut jobs) = self.inner.lock() {
            Self::prune(&mut jobs);
            jobs.insert(
                job_id.clone(),
                SearchJob {
                    started: Instant::now(),
                    partial: running_data(&job_id),
                    result: None,
                    error: None,
                    cursors: BTreeMap::new(),
                },
            );
        }

        let jobs = self.clone();
        let completed_job_id = job_id.clone();
        let local_service = service.clone();
        let local_args = args.clone();
        tokio::spawn(async move {
            let mut raw_args = args;
            raw_args.verbose = true;
            let outcome = service.call_search(raw_args).await;
            if let Ok(mut jobs) = jobs.inner.lock() {
                if let Some(job) = jobs.get_mut(&completed_job_id) {
                    match outcome {
                        Ok(result) => job.result = Some(result),
                        Err(error) => job.error = Some(error.to_string()),
                    }
                }
            }
        });

        if requests_local_host(&local_service, local_args.hosts.as_ref()) {
            let jobs = self.clone();
            let partial_job_id = job_id.clone();
            let local = local_service.local.clone();
            tokio::spawn(async move {
                let limit = local_args
                    .limit
                    .unwrap_or(local.limits.max_results)
                    .min(local.limits.max_results);
                let context_lines = local_args
                    .context_lines
                    .unwrap_or(local.limits.context_lines);
                let outcome = local
                    .search_text_bounded(
                        &local_args.query,
                        limit,
                        context_lines,
                        local_args.mode,
                        local_args.path_globs,
                        local_args.roots,
                    )
                    .await;
                if let Ok(outcome) = outcome {
                    if let Ok(mut jobs) = jobs.inner.lock() {
                        if let Some(job) = jobs.get_mut(&partial_job_id) {
                            if job.result.is_none() && job.error.is_none() {
                                let mut partial = running_data(&partial_job_id);
                                partial["truncated"] = Value::Bool(outcome.truncated);
                                partial["results"] = serde_json::to_value(&outcome.hits)
                                    .unwrap_or_else(|_| Value::Array(vec![]));
                                partial["matches"] = partial["results"].clone();
                                partial["host_status"] = json!([PerHostStatus {
                                    host_id: local.host_id.clone(),
                                    ok: !outcome.partial,
                                    error: outcome.partial_error,
                                }]);
                                job.partial = partial;
                            }
                        }
                    }
                }
            });
        }
        job_id
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

    pub fn completed_result(&self, job_id: &str) -> Result<Option<Value>> {
        let mut jobs = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("search job lock poisoned"))?;
        Self::prune(&mut jobs);
        let job = jobs
            .get(job_id)
            .ok_or_else(|| anyhow::anyhow!("unknown or expired search job {job_id}"))?;
        if let Some(error) = &job.error {
            return Err(anyhow::anyhow!(error.clone()));
        }
        Ok(job.result.as_ref().map(|result| result.data.clone()))
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
        Self::prune(&mut jobs);
        let job = jobs
            .get_mut(job_id)
            .ok_or_else(|| anyhow::anyhow!("unknown or expired search job {job_id}"))?;
        if let Some(error) = &job.error {
            return Ok(json!({"state": "failed", "job_id": job_id, "error": error}));
        }
        let Some(result) = job.result.as_ref() else {
            return Ok(job.partial.clone());
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
        let all_results = result
            .data
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let end = start.saturating_add(page_size).min(all_results.len());
        let mut data = result.data.clone();
        let page = all_results[start..end].to_vec();
        data["results"] = Value::Array(page.clone());
        data["matches"] = Value::Array(page);
        data["state"] = Value::String("complete".into());
        data["job_id"] = Value::String(job_id.to_string());
        if end < all_results.len() {
            let next_cursor = fresh_handle("page");
            job.cursors.insert(next_cursor.clone(), end);
            data["cursor"] = Value::String(next_cursor);
        }
        Ok(data)
    }

    fn is_running(&self, job_id: &str) -> bool {
        self.inner
            .lock()
            .ok()
            .and_then(|jobs| {
                jobs.get(job_id)
                    .map(|job| job.result.is_none() && job.error.is_none())
            })
            .unwrap_or(false)
    }

    fn prune(jobs: &mut BTreeMap<String, SearchJob>) {
        let now = Instant::now();
        jobs.retain(|_, job| now.duration_since(job.started) < JOB_TTL);
    }
}

fn running_data(job_id: &str) -> Value {
    json!({
        "state": "running", "job_id": job_id, "partial": true,
        "truncated": false, "results": [], "matches": [], "host_status": []
    })
}

fn requests_local_host(service: &MeshService, hosts: Option<&HostsInput>) -> bool {
    let is_local = |host: &str| host == "local" || host == "*" || host == service.local.host_id;
    match hosts {
        None => true,
        Some(HostsInput::One(host)) => is_local(host),
        Some(HostsInput::Many(hosts)) => hosts.iter().any(|host| is_local(host)),
    }
}

fn fresh_handle(prefix: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = HANDLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{now:x}-{counter:x}")
}
