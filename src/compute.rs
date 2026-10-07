//! Optional, private GPU OCR worker. Capability probes never initialize OCR.
use crate::{
    config::{ComputeConfig, OcrConfig},
    ocr::OcrEngine,
};
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    env, fs,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

const HARD_PAYLOAD_LIMIT: usize = 32 * 1024 * 1024;
const MIN_GPU_RESERVE_MB: u64 = 2560;
const GPU_ARENA_LIMIT: usize = 512 * 1024 * 1024;

pub struct ComputeWorker {
    host_id: String,
    config: ComputeConfig,
    engine: Option<Arc<OcrEngine>>,
    slots: Arc<Semaphore>,
    intake_slots: Arc<Semaphore>,
    probe_lock: Mutex<()>,
    timeout: Duration,
    max_output_bytes: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OcrExtractRequest {
    pub content_base64: String,
    pub input_type: String,
    #[serde(default)]
    pub filename: Option<String>,
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
        ocr.execution_provider = "cuda".into();
        ocr.gpu_device_id = config.gpu_device_id;
        ocr.gpu_mem_limit_bytes = ocr.gpu_mem_limit_bytes.min(GPU_ARENA_LIMIT);
        let timeout = Duration::from_millis(ocr.remote_timeout_ms.clamp(1_000, 300_000));
        Self {
            host_id,
            config,
            engine: OcrEngine::new(ocr).map(Arc::new),
            slots: Arc::new(Semaphore::new(1)),
            intake_slots: Arc::new(Semaphore::new(1)),
            probe_lock: Mutex::new(()),
            timeout,
            max_output_bytes: max_output_bytes.clamp(1024, 1024 * 1024),
        }
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
            Some("GPU compute worker is disabled")
        } else if !cfg!(feature = "ocr-cuda") {
            Some("CUDA OCR support is absent from this build")
        } else if self.engine.is_none() {
            Some("OCR engine is unavailable")
        } else if self.config.gpu_device_id < 0 {
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
        if let Some(reason) = self.eligibility_error() {
            return self.unavailable("ineligible", reason);
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
        json!({"host_id": self.host_id, "backend": "cuda", "gpu_uuid": null,
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
        if let Some(reason) = self.eligibility_error() {
            return Err(anyhow!(reason));
        }
        let extension = validate_request(&request, self.payload_limit())?;
        let permit = Arc::clone(&self.slots)
            .try_acquire_owned()
            .map_err(|_| anyhow!("GPU worker is busy"))?;
        let gpu = self.probe_gpu(true).await?;
        let admission = self.gpu_admission(&gpu);
        if admission != "available" {
            return Err(anyhow!("GPU worker rejected admission: {admission}"));
        }
        let worker = Arc::clone(self);
        let engine = Arc::clone(
            self.engine
                .as_ref()
                .ok_or_else(|| anyhow!("OCR engine unavailable"))?,
        );
        // The permit belongs to the native job, not the waiting HTTP request.
        // Timeout/disconnection cannot admit an overlapping native inference.
        let job = tokio::task::spawn_blocking(move || -> Result<Value> {
            let _permit = permit;
            let bytes = STANDARD
                .decode(request.content_base64)
                .context("invalid base64 OCR payload")?;
            if bytes.is_empty() || bytes.len() > worker.payload_limit() {
                return Err(anyhow!("OCR payload exceeds allowed size"));
            }
            fs::create_dir_all(&worker.config.temp_dir)
                .context("prepare worker temporary directory")?;
            let directory = tempfile::Builder::new()
                .prefix("ocr-")
                .tempdir_in(&worker.config.temp_dir)
                .context("create worker job directory")?;
            let path = directory.path().join(format!("input.{extension}"));
            fs::write(&path, bytes).context("store worker input")?;
            let text = if extension == "pdf" {
                #[cfg(feature = "ocr")]
                let text = engine.extract_pdf_in(&path, directory.path())?;
                #[cfg(not(feature = "ocr"))]
                let text = engine.extract_pdf(&path)?;
                text
            } else {
                engine.extract_image(&path)?
            };
            let result = json!({"text": text, "host_id": worker.host_id, "gpu_uuid": gpu.uuid,
                "gpu_name": gpu.name, "backend": "cuda"});
            if serde_json::to_vec(&result)?.len() > worker.max_output_bytes {
                return Err(anyhow!("OCR output exceeds worker response limit"));
            }
            Ok(result)
        });
        tokio::time::timeout(self.timeout, job)
            .await
            .map_err(|_| {
                anyhow!("GPU OCR deadline exceeded; worker remains busy until native job finishes")
            })?
            .context("GPU OCR job failed")?
    }
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
    use super::*;
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
