use crate::compute_client::{mesh_input_limit, ComputeClient};
use crate::config::OcrConfig;
use anyhow::{anyhow, Context, Result};
use image::ImageFormat;
use oar_ocr::{
    core::config::{OrtExecutionProvider, OrtSessionConfig},
    oarocr::{OAROCRBuilder, OAROCR},
    utils::load_image,
};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    thread,
    time::{Duration, Instant},
};

const MAX_DEFERRED_PATHS: usize = 65_536;
const RETRY_COOLDOWN: Duration = Duration::from_secs(60);

pub struct OcrEngine {
    config: OcrConfig,
    pipeline: Mutex<Option<OcrRuntime>>,
    mesh: Mutex<Option<ComputeClient>>,
    deferred: Mutex<BTreeSet<PathBuf>>,
    last_retry: Mutex<Instant>,
    retry_cursor: Mutex<Option<PathBuf>>,
    deferred_overflow: AtomicBool,
}

struct OcrRuntime {
    ocr: OAROCR,
    preprocessing: rayon::ThreadPool,
}

impl OcrEngine {
    pub fn new(config: OcrConfig) -> Option<Self> {
        config.enabled.then_some(Self {
            config,
            pipeline: Mutex::new(None),
            mesh: Mutex::new(None),
            deferred: Mutex::new(BTreeSet::new()),
            last_retry: Mutex::new(
                Instant::now()
                    .checked_sub(RETRY_COOLDOWN)
                    .unwrap_or_else(Instant::now),
            ),
            retry_cursor: Mutex::new(None),
            deferred_overflow: AtomicBool::new(false),
        })
    }

    pub(crate) fn is_mesh(&self) -> bool {
        self.config.backend == "mesh"
    }

    pub(crate) fn deferred_count(&self) -> usize {
        self.deferred
            .lock()
            .map(|paths| paths.len())
            .unwrap_or(1)
            .saturating_add(usize::from(self.deferred_overflow.load(Ordering::Relaxed)))
    }

    pub(crate) fn is_deferred(&self, path: &Path) -> bool {
        self.deferred
            .lock()
            .map(|paths| paths.contains(path))
            .unwrap_or(true)
    }

    /// The index acknowledges successful durable writes, terminal exclusions,
    /// and file deletion through this method.
    pub(crate) fn forget_deferred(&self, path: &Path) {
        if let Ok(mut paths) = self.deferred.lock() {
            paths.remove(path);
        }
    }

    /// Retain failed extraction or persistence work without overwriting input.
    pub(crate) fn defer_path(&self, path: &Path) -> Result<()> {
        if !self.is_mesh() {
            return Ok(());
        }
        let mut paths = self
            .deferred
            .lock()
            .map_err(|_| anyhow!("deferred OCR queue lock is unavailable"))?;
        if paths.contains(path) {
            return Ok(());
        }
        if paths.len() >= MAX_DEFERRED_PATHS {
            self.deferred_overflow.store(true, Ordering::Relaxed);
            return Err(anyhow!(
                "deferred mesh OCR queue is full; input was not queued and requires reconciliation"
            ));
        }
        paths.insert(path.to_path_buf());
        Ok(())
    }

    /// Clearing this latch only authorizes a new full walk, which rediscovers
    /// overflowed inputs; it is never evidence of a completed reconciliation.
    pub(crate) fn clear_overflow_if_drained(&self) -> bool {
        let Ok(paths) = self.deferred.lock() else {
            return false;
        };
        paths.is_empty() && self.deferred_overflow.swap(false, Ordering::Relaxed)
    }

    /// Entries stay pending until the index acknowledges persistence or the
    /// input disappears.
    /// Rotate through the set so one rejected input cannot starve other files.
    pub(crate) fn take_retry_paths(&self, max: usize) -> Vec<PathBuf> {
        if !self.is_mesh() || max == 0 {
            return Vec::new();
        }
        let Ok(mut last_retry) = self.last_retry.lock() else {
            return Vec::new();
        };
        if last_retry.elapsed() < RETRY_COOLDOWN {
            return Vec::new();
        }
        let Ok(mut paths) = self.deferred.lock() else {
            return Vec::new();
        };
        let Ok(mut cursor) = self.retry_cursor.lock() else {
            return Vec::new();
        };
        let next = cursor
            .as_ref()
            .and_then(|previous| {
                paths
                    .range((
                        std::ops::Bound::Excluded(previous.clone()),
                        std::ops::Bound::Unbounded,
                    ))
                    .next()
            })
            .or_else(|| paths.iter().next())
            .cloned();
        let Some(path) = next else {
            return Vec::new();
        };
        *last_retry = Instant::now();
        *cursor = Some(path.clone());
        if matches!(path.try_exists(), Ok(false)) {
            paths.remove(&path);
            return Vec::new();
        }
        // The max parameter cannot multiply the shared project's workload.
        vec![path]
    }

    pub fn is_image(&self, path: &Path) -> bool {
        path.extension()
            .and_then(|ext| ext.to_str())
            .and_then(ImageFormat::from_extension)
            .is_some()
    }

    pub fn is_pdf(&self, path: &Path) -> bool {
        path.extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("pdf"))
    }

    pub fn max_image_bytes(&self) -> u64 {
        if self.is_mesh() {
            mesh_input_limit(self.config.max_image_bytes)
        } else {
            self.config.max_image_bytes
        }
    }

    pub fn should_ocr_pdf(&self, extracted: Option<&str>) -> bool {
        if !self.config.pdf_fallback {
            return false;
        }
        let chars = extracted
            .unwrap_or_default()
            .chars()
            .filter(|ch| ch.is_alphanumeric())
            .count();
        chars < self.config.min_pdf_text_chars
    }

    pub fn extract_image(&self, path: &Path) -> Result<String> {
        if self.config.backend == "mesh" {
            let input_type = path
                .extension()
                .and_then(|ext| ext.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            return self.extract_remote(path, &input_type);
        }
        if self.config.backend != "local" {
            return Err(anyhow!("unsupported OCR backend"));
        }
        let mut pipeline = self
            .pipeline
            .lock()
            .map_err(|_| anyhow!("OCR pipeline lock is unavailable"))?;
        if pipeline.is_none() {
            *pipeline = Some(OcrRuntime::new(&self.config)?);
        }
        let runtime = pipeline.as_ref().expect("OCR pipeline initialized");
        // Image decoding and nested Rayon OCR operations use this pool rather
        // than creating a global pool sized for every CPU on the host.
        let results = runtime.preprocessing.install(|| {
            let image =
                load_image(path).with_context(|| format!("load image {}", path.display()))?;
            runtime.ocr.predict(vec![image]).context("run OAR-OCR")
        })?;
        let mut lines = Vec::new();
        for page in results {
            for region in page.text_regions {
                if let Some(text) = region.text {
                    let text = text.trim();
                    if !text.is_empty() {
                        lines.push(text.to_string());
                    }
                }
            }
        }
        Ok(lines.join("\n"))
    }

    pub fn extract_pdf(&self, path: &Path) -> Result<String> {
        if self.config.backend == "mesh" {
            return self.extract_remote(path, "pdf");
        }
        self.extract_pdf_in(path, Path::new(".tmp/ocr"))
    }

    fn extract_remote(&self, path: &Path, input_type: &str) -> Result<String> {
        let result = (|| {
            let mut mesh = self
                .mesh
                .lock()
                .map_err(|_| anyhow!("mesh OCR lock is unavailable"))?;
            let client = mesh.get_or_insert_with(|| ComputeClient::new(self.config.clone()));
            client.extract(path, input_type)
        })();
        if matches!(path.try_exists(), Ok(false)) {
            self.forget_deferred(path);
        } else if result.is_err() {
            self.defer_path(path)?;
        }
        // A successful GPU result is not durable until SQLite accepts it.
        // Keep an existing deferred entry for the index's explicit ACK.
        result
    }

    /// GPU workers supply their protected input TempDir as the rendering root.
    pub(crate) fn extract_pdf_in(&self, path: &Path, temp_root: &Path) -> Result<String> {
        if self.config.backend == "mesh" {
            return self.extract_remote(path, "pdf");
        }
        if self.config.backend != "local" {
            return Err(anyhow!("unsupported OCR backend"));
        }
        fs::create_dir_all(temp_root).context("create PDF OCR temporary root")?;
        let dir = tempfile::tempdir_in(temp_root).context("create PDF OCR tempdir")?;
        let prefix = dir.path().join("page");
        let mut renderer = Command::new("pdftoppm")
            .arg("-png")
            .arg("-r")
            .arg(self.config.pdf_dpi.to_string())
            .arg("-f")
            .arg("1")
            .arg("-l")
            .arg(self.config.max_pdf_pages.to_string())
            .arg(path)
            .arg(&prefix)
            // Native rendering can produce arbitrary diagnostics. Discard
            // them rather than leave bounded workers blocked on a full pipe.
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("run pdftoppm for {}", path.display()))?;
        let deadline = Instant::now() + Duration::from_secs(90);
        let status = loop {
            match renderer.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => {}
                Err(error) => {
                    let _ = renderer.kill();
                    let _ = renderer.wait();
                    return Err(error).context("wait for PDF OCR renderer");
                }
            }
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                let _ = renderer.kill();
                let _ = renderer.wait();
                return Err(anyhow!("PDF OCR rendering exceeded its 90-second deadline"));
            };
            thread::sleep(remaining.min(Duration::from_millis(100)));
        };
        if !status.success() {
            return Err(anyhow!("PDF OCR renderer failed with status {status}"));
        }
        let mut pages: Vec<PathBuf> = fs::read_dir(dir.path())?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().is_some_and(|ext| ext == "png"))
            .collect();
        pages.sort();
        let mut markdown = String::new();
        for (index, page) in pages.iter().enumerate() {
            let text = self.extract_image(page)?;
            if text.trim().is_empty() {
                continue;
            }
            if !markdown.is_empty() {
                markdown.push_str("\n\n");
            }
            markdown.push_str(&format!("## Page {}\n\n{}", index + 1, text));
        }
        Ok(markdown)
    }
}

impl OcrRuntime {
    fn new(config: &OcrConfig) -> Result<Self> {
        let mut session = OrtSessionConfig::new()
            .with_intra_threads(1)
            .with_inter_threads(1)
            .with_parallel_execution(false)
            .add_config_entry("session.intra_op.allow_spinning", "0")
            .add_config_entry("session.inter_op.allow_spinning", "0");
        match config.execution_provider.as_str() {
            "cpu" => {}
            "cuda" => {
                if !cfg!(feature = "ocr-cuda") {
                    return Err(anyhow!("CUDA OCR support is disabled in this build"));
                }
                if config.gpu_device_id < 0 || config.gpu_mem_limit_bytes == 0 {
                    return Err(anyhow!("invalid CUDA OCR device or memory limit"));
                }
                session = session
                    .add_config_entry("session.disable_cpu_ep_fallback", "1")
                    .with_execution_providers(vec![OrtExecutionProvider::CUDA {
                        device_id: Some(config.gpu_device_id),
                        gpu_mem_limit: Some(config.gpu_mem_limit_bytes.min(512 * 1024 * 1024)),
                        arena_extend_strategy: Some("SameAsRequested".into()),
                        cudnn_conv_algo_search: Some("Default".into()),
                        cudnn_conv_use_max_workspace: Some(false),
                    }]);
            }
            _ => return Err(anyhow!("unsupported OCR execution provider")),
        }
        let preprocessing = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .context("initialize bounded OCR preprocessing pool")?;
        // Each model owns a session; neither may silently fall back to CPU
        // when the GPU worker explicitly requested CUDA execution.
        let ocr = OAROCRBuilder::new(
            config.det_model.as_str(),
            config.rec_model.as_str(),
            config.dict.as_str(),
        )
        .ort_session(session)
        .build()
        .context("initialize OAR-OCR pipeline")?;
        Ok(Self { ocr, preprocessing })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deferred_retry_is_serial_and_overflow_requires_a_new_walk() {
        let ocr = OcrEngine::new(OcrConfig {
            backend: "mesh".into(),
            ..OcrConfig::default()
        })
        .unwrap();
        ocr.deferred.lock().unwrap().insert(PathBuf::from("."));
        ocr.deferred.lock().unwrap().insert(PathBuf::from(".."));
        assert_eq!(ocr.take_retry_paths(10).len(), 1);
        assert!(ocr.take_retry_paths(10).is_empty());
        assert_eq!(ocr.deferred_count(), 2);
        ocr.deferred_overflow.store(true, Ordering::Relaxed);
        assert!(!ocr.clear_overflow_if_drained());
        ocr.deferred.lock().unwrap().clear();
        assert_eq!(ocr.deferred_count(), 1);
        assert!(ocr.clear_overflow_if_drained());
        assert_eq!(ocr.deferred_count(), 0);
    }

    #[test]
    fn mesh_errors_never_initialize_a_local_pipeline() {
        let ocr = OcrEngine::new(OcrConfig {
            backend: "mesh".into(),
            det_model: "missing-detector.onnx".into(),
            rec_model: "missing-recognizer.onnx".into(),
            ..OcrConfig::default()
        })
        .unwrap();
        assert!(ocr.extract_image(Path::new("missing-input.png")).is_err());
        assert!(ocr.extract_pdf(Path::new("missing-input.pdf")).is_err());
        assert!(ocr.pipeline.lock().unwrap().is_none());
    }

    #[cfg(not(feature = "ocr-cuda"))]
    #[test]
    fn cuda_request_fails_before_creating_local_worker_in_cpu_build() {
        let config = OcrConfig {
            execution_provider: "cuda".into(),
            ..OcrConfig::default()
        };
        assert!(OcrRuntime::new(&config).is_err());
    }

    #[test]
    fn detects_formats_supported_by_image_crate() {
        let ocr = OcrEngine::new(OcrConfig::default()).unwrap();
        for name in [
            "a.jpg", "a.jpeg", "a.png", "a.webp", "a.tif", "a.tiff", "a.bmp", "a.gif", "a.avif",
        ] {
            assert!(ocr.is_image(Path::new(name)), "missing {name}");
        }
        assert!(!ocr.is_image(Path::new("a.pdf")));
    }

    #[test]
    fn pdf_fallback_only_runs_for_thin_text_layer() {
        let ocr = OcrEngine::new(OcrConfig::default()).unwrap();
        assert!(ocr.should_ocr_pdf(None));
        assert!(ocr.should_ocr_pdf(Some("scan")));
        assert!(!ocr.should_ocr_pdf(Some(&"searchable text ".repeat(20))));
    }
}
