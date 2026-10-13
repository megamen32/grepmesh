//! Optional, private GPU OCR worker. Capability probes never initialize OCR.
use crate::{
    config::{ComputeConfig, OcrConfig},
    ocr::OcrEngine,
};
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    env, fs,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

const HARD_PAYLOAD_LIMIT: usize = 32 * 1024 * 1024;
const MIN_GPU_RESERVE_MB: u64 = 2560;
const GPU_ARENA_LIMIT: usize = 512 * 1024 * 1024;

// Native inference outlives an expired/disconnected HTTP waiter. Keep its
// successful output for the same content/type retry, rather than running OCR
// again and losing every result that takes longer than the waiting deadline.
// This is process-local, not a durable queue or a claim of completed indexing.
const RETAINED_RESULT_COUNT: usize = 64;
const RETAINED_RESULT_BYTES: usize = 64 * 1024 * 1024;
const RETAINED_RESULT_TTL: Duration = Duration::from_secs(2 * 60 * 60);

#[derive(Default)]
struct RetainedResults {
    rows: VecDeque<([u8; 32], Instant, Value, usize)>,
    bytes: usize,
}

impl RetainedResults {
    fn prune(&mut self, now: Instant) {
        while self
            .rows
            .front()
            .is_some_and(|row| now.saturating_duration_since(row.1) >= RETAINED_RESULT_TTL)
        {
            self.bytes -= self.rows.pop_front().unwrap().3;
        }
    }

    fn get(&mut self, key: &[u8; 32], now: Instant) -> Option<Value> {
        self.prune(now);
        self.rows
            .iter()
            .find(|row| &row.0 == key)
            .map(|row| row.2.clone())
    }

    fn insert(&mut self, key: [u8; 32], value: Value, now: Instant) {
        self.prune(now);
        let Ok(bytes) = serde_json::to_vec(&value).map(|v| v.len()) else {
            return;
        };
        if bytes > RETAINED_RESULT_BYTES {
            return;
        }
        if let Some(index) = self.rows.iter().position(|row| row.0 == key) {
            self.bytes -= self.rows.remove(index).unwrap().3;
        }
        while self.rows.len() >= RETAINED_RESULT_COUNT || self.bytes + bytes > RETAINED_RESULT_BYTES
        {
            self.bytes -= self.rows.pop_front().unwrap().3;
        }
        self.rows.push_back((key, now, value, bytes));
        self.bytes += bytes;
    }
}

fn retain_native_success(
    cache: &StdMutex<RetainedResults>,
    key: [u8; 32],
    value: Value,
) -> Result<Value> {
    cache
        .lock()
        .map_err(|_| anyhow!("OCR result cache unavailable"))?
        .insert(key, value.clone(), Instant::now());
    Ok(value)
}

fn content_key(bytes: &[u8], extension: &str) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(extension.as_bytes());
    hash.update([0]);
    hash.update(bytes);
    hash.finalize().into()
}

fn device_content_key(bytes: &[u8], extension: &str, provider: &str) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(provider.as_bytes());
    hash.update([0]);
    hash.update(content_key(bytes, extension));
    hash.finalize().into()
}

pub struct ComputeWorker {
    host_id: String,
    config: ComputeConfig,
    engine: Option<Arc<OcrEngine>>,
    cpu_engine: Option<Arc<OcrEngine>>,
    accelerator_environment_ready: bool,
    slots: Arc<Semaphore>,
    intake_slots: Arc<Semaphore>,
    probe_lock: Mutex<()>,
    timeout: Duration,
    max_output_bytes: usize,
    retained: StdMutex<RetainedResults>,
    retained_hits: AtomicU64,
    retained_late: AtomicU64,
    load: Mutex<Option<(Instant, crate::compute_load::HostLoad)>>,
    image_rates: StdMutex<VecDeque<(Instant, f64)>>,
    ratings: StdMutex<crate::compute_ratings::Ratings>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OcrExtractRequest {
    pub content_base64: String,
    pub input_type: String,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub execution_provider: Option<String>,
    #[serde(default)]
    pub task_class: Option<String>,
}

#[derive(Debug)]
struct Gpu {
    uuid: String,
    name: String,
    capability: String,
    total_bytes: u64,
    free_bytes: u64,
    utilization: u64,
}

impl ComputeWorker {
    pub fn new(
        host_id: String,
        config: ComputeConfig,
        mut ocr: OcrConfig,
        max_output_bytes: usize,
    ) -> Self {
        ocr.enabled = config.enabled;
        ocr.backend = "local".into();
        ocr.execution_provider = config.execution_provider.clone();
        ocr.gpu_device_id = config.gpu_device_id;
        ocr.gpu_mem_limit_bytes = ocr.gpu_mem_limit_bytes.min(GPU_ARENA_LIMIT);
        let timeout = Duration::from_millis(ocr.remote_timeout_ms.clamp(1_000, 300_000));
        let mut cpu_ocr = ocr.clone();
        cpu_ocr.execution_provider = "cpu".into();
        let engine = OcrEngine::new(ocr).map(Arc::new);
        let cpu_engine = if config.execution_provider == "cpu" {
            engine.as_ref().map(Arc::clone)
        } else {
            OcrEngine::new(cpu_ocr).map(Arc::new)
        };
        let accelerator_environment_ready = config.execution_provider == "cpu";
        let mut worker = Self {
            host_id,
            config,
            engine,
            cpu_engine,
            accelerator_environment_ready,
            slots: Arc::new(Semaphore::new(1)),
            intake_slots: Arc::new(Semaphore::new(1)),
            probe_lock: Mutex::new(()),
            timeout,
            max_output_bytes: max_output_bytes.clamp(1024, 1024 * 1024),
            retained: StdMutex::new(RetainedResults::default()),
            retained_hits: AtomicU64::new(0),
            retained_late: AtomicU64::new(0),
            load: Mutex::new(None),
            image_rates: StdMutex::new(VecDeque::new()),
            ratings: StdMutex::new(crate::compute_ratings::Ratings::default()),
        };
        // Only eligible accelerators establish the global provider. This does
        // not load models; explicitly chosen CPU sessions still opt out of it.
        if worker.config.execution_provider != "cpu" {
            worker.accelerator_environment_ready = if worker.eligibility_error().is_some() {
                true // Accelerator is permanently ineligible in this worker; CPU is independent.
            } else {
                worker
                    .engine
                    .as_ref()
                    .is_some_and(|e| e.prepare_accelerator_environment().is_ok())
            };
        }
        if worker.config.enabled && !worker.accelerator_environment_ready {
            tracing::warn!(stage = "provider_environment", provider = %worker.config.execution_provider,
                "OCR provider prerequisite is unavailable; no device admission promised");
        }
        worker
    }

    pub fn max_request_bytes(&self) -> usize {
        self.payload_limit()
            .div_ceil(3)
            .saturating_mul(4)
            .saturating_add(4096)
    }

    /// Shared across both listeners; acquire before buffering or JSON parsing.
    pub fn try_acquire_intake(&self) -> Result<OwnedSemaphorePermit> {
        Arc::clone(&self.intake_slots)
            .try_acquire_owned()
            .map_err(|_| anyhow!("GPU worker request intake is busy"))
    }

    fn payload_limit(&self) -> usize {
        self.config.max_payload_bytes.min(HARD_PAYLOAD_LIMIT)
    }

    fn eligibility_error(&self) -> Option<&'static str> {
        if !self.config.enabled {
            Some("OCR compute worker is disabled")
        } else if !matches!(
            self.config.execution_provider.as_str(),
            "cuda" | "cpu" | "coreml"
        ) {
            Some("unsupported OCR worker execution provider")
        } else if self.config.execution_provider == "cuda" && !cfg!(feature = "ocr-cuda") {
            Some("CUDA OCR support is absent from this build")
        } else if self.config.execution_provider == "coreml"
            && !cfg!(all(
                feature = "ocr-coreml",
                target_os = "macos",
                target_arch = "aarch64"
            ))
        {
            Some("Apple CoreML OCR support is absent from this build")
        } else if self.engine.is_none() {
            Some("OCR engine is unavailable")
        } else if self.config.execution_provider == "cuda" && self.config.gpu_device_id < 0 {
            Some("invalid GPU device index")
        } else {
            None
        }
    }

    fn admission(&self, gpu: &Gpu) -> &'static str {
        if self.slots.available_permits() == 0 {
            "busy"
        } else {
            self.gpu_admission(gpu)
        }
    }

    fn gpu_admission(&self, gpu: &Gpu) -> &'static str {
        if gpu.free_bytes
            < self
                .config
                // This configured value is the retained tenant reserve,
                // not the total free-memory admission threshold.
                .min_free_vram_mb
                .max(MIN_GPU_RESERVE_MB)
                .saturating_mul(1024 * 1024)
                // Detector and recognizer each own one bounded CUDA arena.
                // Keep the tenant reserve after their potential allocation.
                .saturating_add((GPU_ARENA_LIMIT as u64).saturating_mul(2))
        {
            "insufficient_vram"
        } else if gpu.utilization > self.config.max_gpu_utilization_percent.min(100) {
            "gpu_busy"
        } else {
            "available"
        }
    }

    pub async fn status(&self) -> Value {
        let mut result = self.capability_status().await;
        result["hardware_performance_score"] = result["performance_score"].clone();
        match self.host_load().await {
            Ok(load) => {
                result["host_admission"] = json!(load.admission());
                let utilization = load
                    .utilization()
                    .max(result["gpu_utilization_percent"].as_f64().unwrap_or(0.0))
                    .max(if self.config.execution_provider == "coreml" {
                        load.apple_gpu_utilization_percent.unwrap_or(100.0)
                    } else {
                        0.0
                    });
                result["utilization_percent"] = json!(utilization);
                if result["admission"] == "available" && load.admission() != "available" {
                    result["admission"] = json!(load.admission());
                }
                if self.config.execution_provider == "coreml" {
                    result["gpu_utilization_percent"] = json!(load.apple_gpu_utilization_percent);
                    if load.apple_gpu_utilization_percent.is_none() {
                        result["admission"] = json!("gpu_load_unavailable");
                    } else if load.apple_gpu_utilization_percent.unwrap()
                        > self.config.max_gpu_utilization_percent.min(100) as f64
                    {
                        result["admission"] = json!("gpu_busy");
                    }
                    // Architecture estimate only, replaced by observed OCR throughput.
                    result["hardware_performance_score"] =
                        json!(load.apple_gpu_core_count.unwrap_or(0) * 5);
                    result["performance_score"] = result["hardware_performance_score"].clone();
                }
                result["host_load"] = json!(load);
            }
            Err(_) => {
                result["host_admission"] = json!("load_unavailable");
                result["admission"] = json!("load_unavailable");
                result["utilization_percent"] = Value::Null;
            }
        }
        if let Ok(mut rates) = self.image_rates.lock() {
            while rates
                .front()
                .is_some_and(|(at, _)| at.elapsed() > Duration::from_secs(1800))
            {
                rates.pop_front();
            }
            result["performance_observations"] = json!(rates.len());
            if !rates.is_empty() {
                let mut sorted: Vec<_> = rates.iter().map(|(_, rate)| *rate).collect();
                sorted.sort_by(f64::total_cmp);
                result["performance_score"] = json!(sorted[sorted.len() / 2] * 100.0);
                result["performance_basis"] = json!("native_image_megapixels_per_second_x100");
                result["performance_score_approximate"] = json!(true);
                result["performance_sample_age_seconds"] =
                    json!(rates.back().unwrap().0.elapsed().as_secs());
            } else {
                result["performance_basis"] = json!("hardware_estimate");
            }
        }
        if let Ok(mut cache) = self.retained.lock() {
            cache.prune(Instant::now());
            result["retained_result_count"] = json!(cache.rows.len());
            result["retained_result_bytes"] = json!(cache.bytes);
        }
        result["retained_result_hits"] = json!(self.retained_hits.load(Ordering::Relaxed));
        result["retained_result_after_deadline_count"] =
            json!(self.retained_late.load(Ordering::Relaxed));
        // Hardware information remains diagnostic only. A cold rating is null.
        result["performance_score"] = Value::Null;
        result["performance_basis"] = json!("task_specific_native_observations");
        let now = Instant::now();
        result["performance_by_task"] = self
            .ratings
            .lock()
            .map(|mut rates| rates.snapshot(&self.config.execution_provider, now))
            .unwrap_or_else(|_| json!({}));
        let mut devices = Vec::new();
        let mut cpu = json!({"host_id": self.host_id, "backend": "cpu", "gpu_uuid": null,
            "busy_count": usize::from(self.slots.available_permits() == 0),
            "admission": "ineligible", "observed_at": observed_at(),
            "performance_score": null, "performance_basis": "task_specific_native_observations",
            "performance_by_task": self.ratings.lock().map(|mut r| r.snapshot("cpu", now)).unwrap_or_else(|_| json!({}))});
        if self.config.enabled && self.cpu_engine.is_some() {
            if let Some(load) = result.get("host_load") {
                cpu["host_load"] = load.clone();
                let utilization = load["cpu_utilization_percent"]
                    .as_f64()
                    .unwrap_or(100.0)
                    .max(
                        load["cgroup_cpu_utilization_percent"]
                            .as_f64()
                            .unwrap_or(0.0),
                    );
                cpu["utilization_percent"] = json!(utilization);
                cpu["admission"] = if self.slots.available_permits() == 0 {
                    json!("busy")
                } else if result["host_admission"] != "available" {
                    result["host_admission"].clone()
                } else {
                    json!("available")
                };
            } else {
                cpu["admission"] = json!("load_unavailable");
                cpu["utilization_percent"] = Value::Null;
            }
        }
        if self.config.enabled && self.cpu_engine.is_some() && !self.accelerator_environment_ready {
            cpu["admission"] = json!("provider_environment_unavailable");
        }
        devices.push(cpu);
        if self.config.execution_provider != "cpu" {
            let mut accelerator = result.clone();
            accelerator["utilization_percent"] = accelerator_utilization(&result);
            // Per-device status never recursively includes the device catalog.
            accelerator.as_object_mut().unwrap().remove("devices");
            devices.push(accelerator);
        }
        result["devices"] = json!(devices);
        result
    }

    async fn host_load(&self) -> Result<crate::compute_load::HostLoad> {
        let mut cached = self
            .load
            .try_lock()
            .map_err(|_| anyhow!("native load observation busy"))?;
        if let Some((at, value)) = cached
            .as_ref()
            .filter(|(at, _)| at.elapsed() < Duration::from_secs(1))
        {
            let _ = at;
            return Ok(value.clone());
        }
        let value = crate::compute_load::observe().await?;
        *cached = Some((Instant::now(), value.clone()));
        Ok(value)
    }

    async fn capability_status(&self) -> Value {
        if let Some(reason) = self.eligibility_error() {
            return self.unavailable("ineligible", reason);
        }
        if !self.accelerator_environment_ready {
            return self.unavailable(
                "provider_environment_unavailable",
                "global OCR provider prerequisite unavailable",
            );
        }
        if matches!(self.config.execution_provider.as_str(), "cpu" | "coreml") {
            return json!({"host_id": self.host_id, "backend": self.config.execution_provider, "gpu_uuid": null,
                "busy_count": usize::from(self.slots.available_permits() == 0),
                "admission": if self.slots.available_permits() == 0 { "busy" } else { "available" },
                "observed_at": observed_at(), "performance_score": std::thread::available_parallelism().map(|n| n.get() as f64 * 5.0).unwrap_or(5.0),
                "coreml_compute_units": if self.config.execution_provider == "coreml" { Some("CPUAndGPU") } else { None },
                "performance_score_approximate": true});
        }
        match self.probe_gpu(false).await {
            Ok(gpu) => json!({
                "host_id": self.host_id, "backend": "cuda", "gpu_uuid": gpu.uuid,
                "gpu_name": gpu.name, "compute_capability": gpu.capability,
                "gpu_total_bytes": gpu.total_bytes, "gpu_free_bytes": gpu.free_bytes,
                "gpu_utilization_percent": gpu.utilization,
                "busy_count": usize::from(self.slots.available_permits() == 0),
                "admission": self.admission(&gpu), "observed_at": observed_at(),
                "performance_score": performance_score(&gpu), "performance_score_approximate": true,
            }),
            Err(error) => self.unavailable("unavailable", &error.to_string()),
        }
    }

    fn unavailable(&self, admission: &str, reason: &str) -> Value {
        json!({"host_id": self.host_id, "backend": self.config.execution_provider, "gpu_uuid": null,
            "gpu_name": null, "compute_capability": null, "gpu_total_bytes": 0,
            "gpu_free_bytes": 0, "gpu_utilization_percent": 100,
            "busy_count": usize::from(self.slots.available_permits() == 0),
            "admission": admission, "reason": reason, "observed_at": observed_at(),
            "performance_score": 0, "performance_score_approximate": true})
    }

    async fn probe_gpu(&self, admitted_job: bool) -> Result<Gpu> {
        // Status polls never queue or multiply subprocesses. The sole job
        // already holding the inference permit may wait briefly for a poll;
        // a concurrent status request must not cancel admitted OCR work.
        let _probe = if admitted_job {
            tokio::time::timeout(Duration::from_secs(3), self.probe_lock.lock())
                .await
                .map_err(|_| anyhow!("GPU capability probe is busy"))?
        } else {
            self.probe_lock
                .try_lock()
                .map_err(|_| anyhow!("GPU capability probe is busy"))?
        };
        let mut command = tokio::process::Command::new("nvidia-smi");
        command
            .args([
                "--query-gpu=index,uuid,name,memory.total,memory.free,utilization.gpu,compute_cap",
                "--format=csv,noheader,nounits",
            ])
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(3), command.output())
            .await
            .map_err(|_| anyhow!("GPU capability probe timed out"))?
            .context("GPU capability probe could not start")?;
        if !output.status.success() || output.stdout.len() > 64 * 1024 {
            return Err(anyhow!("GPU capability probe failed"));
        }
        parse_gpu(
            &String::from_utf8(output.stdout).context("invalid GPU capability response")?,
            &visible_device(self.config.gpu_device_id)?,
        )
    }

    pub async fn extract(self: &Arc<Self>, request: OcrExtractRequest) -> Result<Value> {
        let provider = request
            .execution_provider
            .as_deref()
            .unwrap_or(&self.config.execution_provider)
            .to_owned();
        if provider != "cpu" && provider != self.config.execution_provider {
            return Err(anyhow!(
                "requested OCR execution provider is not configured on this host"
            ));
        }
        if provider != "cpu" {
            if let Some(reason) = self.eligibility_error() {
                return Err(anyhow!(reason));
            }
        } else if !self.accelerator_environment_ready {
            return Err(anyhow!(
                "CPU OCR provider environment prerequisite is unavailable"
            ));
        } else if !self.config.enabled || self.cpu_engine.is_none() {
            return Err(anyhow!("CPU OCR is disabled or unavailable"));
        }
        if request
            .task_class
            .as_ref()
            .is_some_and(|class| class.len() > 80)
        {
            return Err(anyhow!("OCR task class exceeds metadata bound"));
        }
        if !matches!(provider.as_str(), "cpu" | "cuda" | "coreml") {
            let reason = "unsupported OCR execution provider";
            return Err(anyhow!(reason));
        }
        let extension = validate_request(&request, self.payload_limit())?;
        let permit = Arc::clone(&self.slots)
            .try_acquire_owned()
            .map_err(|_| anyhow!("GPU worker is busy"))?;
        let started = Instant::now();
        let preparation_worker = Arc::clone(self);
        let preparation_type = extension.clone();
        let preparation_provider = provider.clone();
        let requested_class = request.task_class;
        // Both decoding and hashing belong to the same one-job admission.
        // The blocking task owns its permit even if the HTTP waiter goes away.
        let (bytes, key, permit, cached) = tokio::task::spawn_blocking(move || {
            let bytes = STANDARD
                .decode(request.content_base64)
                .context("invalid base64 OCR payload")?;
            if bytes.is_empty() || bytes.len() > preparation_worker.payload_limit() {
                return Err(anyhow!("OCR payload exceeds allowed size"));
            }
            let key = device_content_key(&bytes, &preparation_type, &preparation_provider);
            let cached = preparation_worker
                .retained
                .lock()
                .map_err(|_| anyhow!("OCR result cache unavailable"))?
                .get(&key, Instant::now());
            Ok::<_, anyhow::Error>((bytes, key, permit, cached))
        })
        .await
        .context("OCR input preparation failed")??;
        if let Some(value) = cached {
            if requested_class
                .as_deref()
                .is_some_and(|class| value["task_class"].as_str() != Some(class))
            {
                return Err(anyhow!("OCR task class differs from actual input"));
            }
            self.retained_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(value);
        }
        let load = self.host_load().await?;
        if load.admission() != "available" {
            return Err(anyhow!(
                "OCR worker rejected admission: {}",
                load.admission()
            ));
        }
        if provider == "coreml"
            && load
                .apple_gpu_utilization_percent
                .is_none_or(|n| n > self.config.max_gpu_utilization_percent.min(100) as f64)
        {
            return Err(anyhow!(
                "Apple GPU is busy or its native load is unavailable"
            ));
        }
        let gpu = if provider == "cuda" {
            let gpu = self.probe_gpu(true).await?;
            let admission = self.gpu_admission(&gpu);
            if admission != "available" {
                return Err(anyhow!("GPU worker rejected admission: {admission}"));
            }
            Some(gpu)
        } else {
            None
        };
        let worker = Arc::clone(self);
        // Prepare the required GPU environment before a first CPU override;
        // CPU sessions explicitly opt out, future accelerator sessions inherit it.
        let engine = Arc::clone(
            (if provider == "cpu" {
                &self.cpu_engine
            } else {
                &self.engine
            })
            .as_ref()
            .ok_or_else(|| anyhow!("OCR engine unavailable"))?,
        );
        // The permit belongs to the native job, not the waiting HTTP request.
        // Timeout/disconnection cannot admit an overlapping native inference.
        if started.elapsed() >= self.timeout {
            return Err(anyhow!("OCR deadline expired before native inference"));
        }
        let job = tokio::task::spawn_blocking(move || -> Result<Value> {
            let _permit = permit;
            // A queued blocking task must not start inference after its budget.
            if started.elapsed() >= worker.timeout {
                return Err(anyhow!("OCR deadline expired before native inference"));
            }
            fs::create_dir_all(&worker.config.temp_dir)
                .context("prepare worker temporary directory")?;
            let directory = tempfile::Builder::new()
                .prefix("ocr-")
                .tempdir_in(&worker.config.temp_dir)
                .context("create worker job directory")?;
            let path = directory.path().join(format!("input.{extension}"));
            fs::write(&path, bytes).context("store worker input")?;
            #[cfg(feature = "ocr")]
            let dimensions = if extension != "pdf" {
                image::image_dimensions(&path).ok()
            } else {
                None
            };
            #[cfg(not(feature = "ocr"))]
            let dimensions: Option<(u32, u32)> = None;
            let descriptor = mesh_core::placement::TaskDescriptor {
                input_type: extension.clone(),
                input_bytes: fs::metadata(&path)?.len(),
                width: dimensions.map(|v| v.0),
                height: dimensions.map(|v| v.1),
            };
            let task_class = descriptor.task_class();
            if requested_class
                .as_deref()
                .is_some_and(|class| task_class.as_deref() != Some(class))
            {
                return Err(anyhow!("OCR task class differs from actual input"));
            }
            #[cfg(feature = "ocr")]
            let image_pixels = if extension != "pdf" {
                image::image_dimensions(&path).ok().and_then(|(w, h)| {
                    let pixels = w as u64 * h as u64;
                    (pixels > 0 && pixels <= 64_000_000).then_some(pixels)
                })
            } else {
                None
            };
            let inference_started = Instant::now();
            #[cfg(feature = "ocr")]
            let warm_model = engine.native_initialized();
            let text = if extension == "pdf" {
                #[cfg(feature = "ocr")]
                let text = engine
                    .extract_pdf_in(&path, directory.path())
                    .map_err(|error| {
                        tracing::warn!(stage = "pdf_pipeline", provider = %provider,
                        input_bytes = descriptor.input_bytes,
                        elapsed_ms = inference_started.elapsed().as_millis() as u64,
                        "Native OCR PDF pipeline failed");
                        error
                    })?;
                #[cfg(not(feature = "ocr"))]
                let text = engine.extract_pdf(&path)?;
                text
            } else {
                engine.extract_image(&path).map_err(|error| {
                    tracing::warn!(stage = image_failure_stage(&error), provider = %provider,
                        input_bytes = descriptor.input_bytes,
                        elapsed_ms = inference_started.elapsed().as_millis() as u64,
                        "Native OCR image pipeline failed");
                    classify_image_failure(error)
                })?
            };
            let result = json!({"text": text, "host_id": worker.host_id, "gpu_uuid": gpu.as_ref().map(|gpu| &gpu.uuid),
                "gpu_name": gpu.as_ref().map(|gpu| &gpu.name), "backend": provider, "task_class": task_class});
            if serde_json::to_vec(&result)?.len() > worker.max_output_bytes {
                return Err(anyhow!("OCR output exceeds worker response limit"));
            }
            #[cfg(feature = "ocr")]
            if let Some(class) = task_class.as_deref() {
                if let Ok(mut ratings) = worker.ratings.lock() {
                    ratings.record(
                        &provider,
                        class,
                        inference_started.elapsed().as_secs_f64(),
                        warm_model,
                        observed_at(),
                        Instant::now(),
                    );
                }
            }
            #[cfg(feature = "ocr")]
            if let Some(pixels) = image_pixels.filter(|_| warm_model) {
                let rate = pixels as f64 / 1_000_000.0 / inference_started.elapsed().as_secs_f64();
                if rate.is_finite() && rate > 0.0 {
                    if let Ok(mut rates) = worker.image_rates.lock() {
                        if rates.len() == 16 {
                            rates.pop_front();
                        }
                        rates.push_back((Instant::now(), rate));
                    }
                }
            }
            let result = retain_native_success(&worker.retained, key, result)?;
            if started.elapsed() >= worker.timeout {
                worker.retained_late.fetch_add(1, Ordering::Relaxed);
            }
            Ok(result)
        });
        tokio::time::timeout(self.timeout.saturating_sub(started.elapsed()), job)
            .await
            .map_err(|_| {
                tracing::warn!(stage = "wait_deadline", provider = %self.config.execution_provider,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "OCR waiter expired; native job retains its host permit");
                anyhow!("GPU OCR deadline exceeded; worker remains busy until native job finishes")
            })?
            .context("GPU OCR job failed")?
    }
}

fn accelerator_utilization(status: &Value) -> Value {
    status
        .get("gpu_utilization_percent")
        .filter(|v| {
            v.as_f64()
                .is_some_and(|n| n.is_finite() && (0.0..=100.0).contains(&n))
        })
        .cloned()
        .unwrap_or(Value::Null)
}

fn image_failure_stage(error: &anyhow::Error) -> &'static str {
    // Fixed categories only. Error.message, file paths, payloads and OCR text
    // never enter the diagnostic. Opaque inference failures remain unattributed.
    #[cfg(feature = "ocr")]
    for source in error.chain() {
        if source.downcast_ref::<png::DecodingError>().is_some() {
            return "png_decode";
        }
        if source.downcast_ref::<image::ImageError>().is_some() {
            return "image_decode";
        }
    }
    let _ = error;
    "native_ocr"
}

fn classify_image_failure(error: anyhow::Error) -> anyhow::Error {
    // Pinned oar-ocr-core 0.9.2 preserves ImageError as the source of
    // OCRError::ImageLoad. Inspect that typed chain; never infer input validity
    // from a generic "load image" message or classify IO/resource/CUDA errors.
    #[cfg(feature = "ocr")]
    if error
        .chain()
        .filter_map(|source| source.downcast_ref::<png::DecodingError>())
        .any(|error| matches!(error, png::DecodingError::Format(_)))
    {
        return anyhow!(crate::compute_client::InvalidOcrInput);
    }
    #[cfg(feature = "ocr")]
    if error.chain().filter_map(|source| source.downcast_ref::<image::ImageError>()).any(|error| {
        matches!(error, image::ImageError::Decoding(_)) || matches!(error,
            image::ImageError::Unsupported(unsupported) if matches!(unsupported.kind(), image::error::UnsupportedErrorKind::Format(_)))
    }) {
        return anyhow!(crate::compute_client::InvalidOcrInput);
    }
    error
}

fn validate_request(request: &OcrExtractRequest, limit: usize) -> Result<String> {
    let extension = request.input_type.to_ascii_lowercase();
    if !matches!(
        extension.as_str(),
        "png" | "jpg" | "jpeg" | "webp" | "tif" | "tiff" | "bmp" | "gif" | "avif" | "pdf"
    ) {
        return Err(anyhow!("unsupported OCR input type"));
    }
    if request.content_base64.is_empty()
        || request.content_base64.len() > limit.div_ceil(3).saturating_mul(4)
    {
        return Err(anyhow!("encoded OCR payload exceeds allowed size"));
    }
    if let Some(filename) = &request.filename {
        if filename.len() > 255
            || filename.contains(['/', '\\'])
            || Path::new(filename)
                .extension()
                .and_then(|value| value.to_str())
                .map(str::to_ascii_lowercase)
                .as_deref()
                != Some(extension.as_str())
        {
            return Err(anyhow!(
                "filename must be a basename matching the allowed input type"
            ));
        }
    }
    Ok(extension)
}

fn visible_device(ordinal: i32) -> Result<String> {
    if ordinal < 0 {
        return Err(anyhow!("invalid CUDA device ordinal"));
    }
    match env::var("CUDA_VISIBLE_DEVICES") {
        Ok(visible) => visible
            .split(',')
            .nth(ordinal as usize)
            .map(str::trim)
            .filter(|device| device.parse::<u32>().is_ok() || device.starts_with("GPU-"))
            .map(str::to_string)
            .ok_or_else(|| anyhow!("CUDA device is hidden or uses unsupported visibility mapping")),
        Err(env::VarError::NotPresent) => Ok(ordinal.to_string()),
        Err(_) => Err(anyhow!("invalid CUDA visibility mapping")),
    }
}

fn parse_gpu(output: &str, device: &str) -> Result<Gpu> {
    for row in output.lines() {
        let fields: Vec<_> = row.split(',').map(str::trim).collect();
        if fields.len() != 7
            || !(fields[0] == device || (device.starts_with("GPU-") && fields[1] == device))
        {
            continue;
        }
        let mib = |value: &str| -> Result<u64> {
            Ok(value
                .parse::<u64>()
                .context("invalid GPU memory value")?
                .saturating_mul(1024 * 1024))
        };
        let utilization = fields[5]
            .parse::<u64>()
            .context("invalid GPU utilization")?;
        if utilization > 100
            || fields[1].is_empty()
            || !fields[6]
                .parse::<f64>()
                .is_ok_and(|value| value.is_finite() && value > 0.0)
        {
            return Err(anyhow!("invalid GPU capability values"));
        }
        return Ok(Gpu {
            uuid: fields[1].into(),
            name: fields[2].into(),
            capability: fields[6].into(),
            total_bytes: mib(fields[3])?,
            free_bytes: mib(fields[4])?,
            utilization,
        });
    }
    Err(anyhow!("configured GPU is absent from capability response"))
}

fn performance_score(gpu: &Gpu) -> f64 {
    // Approximate architecture capability only; no benchmark or inference in status.
    let name = gpu.name.to_ascii_lowercase();
    for (model, score) in [
        ("5090", 500.0),
        ("4090", 400.0),
        ("3090", 310.0),
        ("3080 ti", 300.0),
        ("3080ti", 300.0),
        ("3080", 280.0),
    ] {
        if name.contains(model) {
            return score;
        }
    }
    gpu.capability.parse::<f64>().unwrap_or(0.0) * 10.0
        + gpu.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
}

fn observed_at() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    #[test]
    fn per_device_gpu_load_never_substitutes_host_cpu_load() {
        assert_eq!(
            super::accelerator_utilization(
                &serde_json::json!({"utilization_percent":90,"gpu_utilization_percent":20})
            ),
            20
        );
        assert_eq!(
            super::accelerator_utilization(&serde_json::json!({"utilization_percent":90})),
            serde_json::Value::Null
        );
    }
    #[cfg(feature = "ocr")]
    #[tokio::test]
    async fn cpu_admission_cannot_promise_a_failed_provider_prerequisite() {
        let mut worker = super::ComputeWorker::new(
            "test".into(),
            crate::config::ComputeConfig {
                enabled: true,
                execution_provider: "cpu".into(),
                ..crate::config::ComputeConfig::default()
            },
            crate::config::OcrConfig::default(),
            4096,
        );
        assert!(worker.cpu_engine.is_some());
        worker.accelerator_environment_ready = false; // Local state regression only.
        let worker = std::sync::Arc::new(worker);
        assert_eq!(
            worker.status().await["devices"][0]["admission"],
            "provider_environment_unavailable"
        );
        let error = worker
            .extract(super::OcrExtractRequest {
                content_base64: "!".into(),
                input_type: "png".into(),
                filename: None,
                execution_provider: Some("cpu".into()),
                task_class: None,
            })
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("provider environment prerequisite"));
    }
    #[test]
    fn native_failure_diagnostic_never_projects_messages_or_paths() {
        let error = anyhow::anyhow!("Authorization: Bearer private /Users/private/input.png");
        assert_eq!(super::image_failure_stage(&error), "native_ocr");
        #[cfg(feature = "ocr")]
        assert_eq!(
            super::image_failure_stage(&anyhow::Error::new(png::DecodingError::LimitsExceeded)),
            "png_decode"
        );
    }
    use super::*;
    #[test]
    fn cpu_gpu_retained_receipts_are_separate_even_for_identical_content() {
        assert_ne!(
            device_content_key(b"same input", "png", "cpu"),
            device_content_key(b"same input", "png", "cuda")
        );
        assert_ne!(
            device_content_key(b"same input", "png", "cpu"),
            device_content_key(b"same input", "png", "coreml")
        );
        let request: OcrExtractRequest = serde_json::from_value(json!({"input_type":"png","content_base64":"AA==","execution_provider":"cpu","task_class":"ocr:image:png:pixels:4mp"})).unwrap();
        assert_eq!(request.execution_provider.as_deref(), Some("cpu"));
        assert!(serde_json::from_value::<OcrExtractRequest>(
            json!({"input_type":"png","content_base64":"AA==","host_id":"foreign"})
        )
        .is_err());
    }
    #[tokio::test]
    async fn unconfigured_device_is_rejected_before_decoding_or_any_model() {
        let worker = Arc::new(ComputeWorker::new(
            "worker".into(),
            ComputeConfig {
                enabled: true,
                execution_provider: "cpu".into(),
                ..Default::default()
            },
            OcrConfig::default(),
            128 * 1024,
        ));
        let error = worker
            .extract(OcrExtractRequest {
                content_base64: "!".into(),
                input_type: "png".into(),
                filename: None,
                execution_provider: Some("cuda".into()),
                task_class: None,
            })
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "requested OCR execution provider is not configured on this host"
        );
        assert_eq!(worker.slots.available_permits(), 1);
    }

    #[test]
    fn retained_results_match_content_and_type_without_extending_ttl() {
        let now = Instant::now();
        let mut cache = RetainedResults::default();
        let key = content_key(b"native output input", "pdf");
        let value = json!({"text":"completed OCR", "host_id":"worker"});
        cache.insert(key, value.clone(), now);
        assert_eq!(cache.get(&key, now + Duration::from_secs(90)), Some(value));
        assert_eq!(cache.get(&content_key(b"other input", "pdf"), now), None);
        assert_eq!(
            cache.get(&content_key(b"native output input", "png"), now),
            None
        );
        assert_eq!(cache.get(&key, now + RETAINED_RESULT_TTL), None);
        assert_eq!(cache.bytes, 0);
    }

    #[test]
    fn retained_results_evict_to_both_hard_bounds_and_replace_exact_key() {
        let now = Instant::now();
        let mut cache = RetainedResults::default();
        for i in 0..RETAINED_RESULT_COUNT + 1 {
            cache.insert(content_key(&i.to_le_bytes(), "png"), json!("ok"), now);
        }
        assert_eq!(cache.rows.len(), RETAINED_RESULT_COUNT);
        assert!(cache
            .get(&content_key(&0usize.to_le_bytes(), "png"), now)
            .is_none());
        let key = content_key(b"last", "pdf");
        cache.insert(key, json!("old"), now);
        cache.insert(key, json!("new"), now);
        assert_eq!(cache.rows.iter().filter(|row| row.0 == key).count(), 1);
        assert_eq!(
            cache.bytes,
            cache.rows.iter().map(|row| row.3).sum::<usize>()
        );
        // Exercise the byte bound cheaply with accounted entries: payload
        // serialization itself has its independent max_output_bytes fence.
        cache.rows.clear();
        cache.bytes = 0;
        cache
            .rows
            .push_back(([1; 32], now, json!("small"), RETAINED_RESULT_BYTES));
        cache.bytes = RETAINED_RESULT_BYTES;
        cache.insert([2; 32], json!("next"), now);
        assert_eq!(cache.rows.len(), 1);
        assert_eq!(cache.rows[0].0, [2; 32]);
        assert!(cache.bytes <= RETAINED_RESULT_BYTES);
    }

    #[tokio::test]
    async fn busy_native_slot_rejects_input_before_decode_and_hash() {
        let worker = Arc::new(ComputeWorker::new(
            "worker".into(),
            ComputeConfig {
                enabled: true,
                execution_provider: "cpu".into(),
                ..Default::default()
            },
            OcrConfig::default(),
            128 * 1024,
        ));
        let _permit = worker.slots.try_acquire().unwrap();
        let error = worker
            .extract(OcrExtractRequest {
                content_base64: "!!!!".into(),
                input_type: "png".into(),
                filename: None,
                execution_provider: None,
                task_class: None,
            })
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "GPU worker is busy");
        assert!(worker.retained.lock().unwrap().rows.is_empty());
    }

    #[tokio::test]
    async fn native_completion_survives_expired_http_waiter_without_second_job() {
        let cache = Arc::new(StdMutex::new(RetainedResults::default()));
        let retained = Arc::clone(&cache);
        let key = content_key(b"same admitted input", "pdf");
        let (finished, receipt) = tokio::sync::oneshot::channel();
        let job = tokio::task::spawn_blocking(move || {
            std::thread::sleep(Duration::from_millis(20));
            retain_native_success(&retained, key, json!({"text":"late native result"})).unwrap();
            finished.send(()).unwrap();
        });
        assert!(tokio::time::timeout(Duration::from_millis(1), job)
            .await
            .is_err());
        receipt.await.unwrap();
        assert_eq!(
            cache.lock().unwrap().get(&key, Instant::now()),
            Some(json!({"text":"late native result"}))
        );
    }

    #[cfg(feature = "ocr")]
    #[test]
    fn confirmed_decode_and_format_errors_are_terminal_but_io_and_limits_are_not() {
        use image::{
            error::{
                DecodingError, ImageFormatHint, LimitError, LimitErrorKind, UnsupportedError,
                UnsupportedErrorKind,
            },
            ImageError,
        };
        let errors = [
            ImageError::Decoding(DecodingError::from_format_hint(ImageFormatHint::Unknown)),
            ImageError::Unsupported(UnsupportedError::from_format_and_kind(
                ImageFormatHint::Unknown,
                UnsupportedErrorKind::Format(ImageFormatHint::Unknown),
            )),
        ];
        for error in errors {
            let error = anyhow::Error::new(error).context("load image private-input-path");
            assert!(classify_image_failure(error).is::<crate::compute_client::InvalidOcrInput>());
        }
        for error in [
            ImageError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "missing input",
            )),
            ImageError::Limits(LimitError::from_kind(LimitErrorKind::InsufficientMemory)),
        ] {
            let error = anyhow::Error::new(error).context("load image private-input-path");
            assert!(!classify_image_failure(error).is::<crate::compute_client::InvalidOcrInput>());
        }
        assert!(
            !classify_image_failure(anyhow!("CUDA provider unavailable"))
                .is::<crate::compute_client::InvalidOcrInput>()
        );
    }
    #[test]
    fn parser_selects_exact_device_and_converts_mib() {
        let gpu = parse_gpu("1, GPU-other, RTX, 12288, 4000, 0, 8.6\n0, GPU-selected, RTX 3080 Ti, 12288, 7000, 4, 8.6", "0").unwrap();
        assert_eq!(gpu.uuid, "GPU-selected");
        assert_eq!(gpu.free_bytes, 7000 * 1024 * 1024);
        assert!(parse_gpu("0, GPU-x, RTX, 12288, 7000, N/A, 8.6", "0").is_err());
        assert_eq!(
            parse_gpu("1, GPU-other, RTX, 12288, 4000, 0, 8.6", "GPU-other")
                .unwrap()
                .uuid,
            "GPU-other"
        );
    }
    #[test]
    fn rejects_paths_unknown_types_and_encoded_oversize() {
        let mut request = OcrExtractRequest {
            content_base64: "YWJj".into(),
            input_type: "png".into(),
            filename: Some("../../a.png".into()),
            execution_provider: None,
            task_class: None,
        };
        assert!(validate_request(&request, 32).is_err());
        request.filename = Some("a.png".into());
        assert!(validate_request(&request, 32).is_ok());
        assert!(validate_request(&request, 0).is_err());
        request.input_type = "txt".into();
        assert!(validate_request(&request, 32).is_err());
    }
    #[tokio::test]
    async fn disabled_worker_is_ineligible_without_probing() {
        let worker = ComputeWorker::new(
            "cpu-host".into(),
            ComputeConfig::default(),
            OcrConfig::default(),
            128 * 1024,
        );
        assert_eq!(worker.status().await["admission"], "ineligible");
    }

    #[test]
    fn intake_is_finite_and_independent_of_native_inference_slot() {
        let worker = ComputeWorker::new(
            "host".into(),
            ComputeConfig::default(),
            OcrConfig::default(),
            128 * 1024,
        );
        let intake = worker.try_acquire_intake().unwrap();
        assert!(worker.try_acquire_intake().is_err());
        assert!(worker.slots.try_acquire().is_ok());
        drop(intake);
        assert!(worker.try_acquire_intake().is_ok());
    }
}
