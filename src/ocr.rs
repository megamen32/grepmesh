use crate::compute_client::{mesh_input_limit, ComputeClient};
use crate::config::OcrConfig;
use crate::index::PersistentIndex;
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
    pending_store: Mutex<Option<PersistentIndex>>,
    pending_store_healthy: AtomicBool,
    pending_store_load_failed: AtomicBool,
    undurable: Mutex<BTreeSet<PathBuf>>,
    retry_fast: AtomicBool,
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
            pending_store: Mutex::new(None),
            pending_store_healthy: AtomicBool::new(true),
            pending_store_load_failed: AtomicBool::new(false),
            undurable: Mutex::new(BTreeSet::new()),
            retry_fast: AtomicBool::new(false),
        })
    }

    pub(crate) fn attach_pending_store(&self, store: PersistentIndex) -> Result<(), String> {
        if !self.is_mesh() {
            return Ok(());
        }
        *self
            .pending_store
            .lock()
            .map_err(|_| "pending store lock unavailable")? = Some(store.clone());
        let loaded = store.load_pending_ocr();
        match loaded {
            Ok((paths, overflow)) => {
                self.deferred
                    .lock()
                    .map_err(|_| "pending queue lock unavailable")?
                    .extend(paths);
                self.deferred_overflow.store(overflow, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                self.pending_store_load_failed
                    .store(true, Ordering::Relaxed);
                self.pending_store_healthy.store(false, Ordering::Relaxed);
                self.deferred_overflow.store(true, Ordering::Relaxed);
                Err(error)
            }
        }
    }

    pub(crate) fn retry_poll_interval(&self) -> Duration {
        if self.retry_fast.load(Ordering::Relaxed) {
            Duration::from_secs(1)
        } else {
            Duration::from_secs(30)
        }
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
            if !paths.contains(path) {
                return;
            }
            let store = self.pending_store.lock();
            let Ok(store) = store else {
                return;
            };
            if let Some(store) = store.as_ref() {
                if store.ack_pending_ocr(path).is_err() {
                    return;
                }
            }
            paths.remove(path);
            if let Ok(mut undurable) = self.undurable.lock() {
                undurable.remove(path);
                self.pending_store_healthy.store(
                    undurable.is_empty() && !self.pending_store_load_failed.load(Ordering::Relaxed),
                    Ordering::Relaxed,
                );
            }
            self.retry_fast.store(true, Ordering::Relaxed);
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
        self.retry_fast.store(false, Ordering::Relaxed);
        if !paths.contains(path) && paths.len() >= MAX_DEFERRED_PATHS {
            self.deferred_overflow.store(true, Ordering::Relaxed);
            if let Some(store) = self
                .pending_store
                .lock()
                .map_err(|_| anyhow!("pending store lock unavailable"))?
                .as_ref()
            {
                store.set_ocr_overflow(true).map_err(anyhow::Error::msg)?;
            }
            return Err(anyhow!(
                "deferred mesh OCR queue is full; input was not queued and requires reconciliation"
            ));
        }
        paths.insert(path.to_path_buf());
        if let Some(store) = self
            .pending_store
            .lock()
            .map_err(|_| anyhow!("pending store lock unavailable"))?
            .as_ref()
        {
            let mut undurable = self
                .undurable
                .lock()
                .map_err(|_| anyhow!("undurable OCR queue lock unavailable"))?;
            if let Err(error) = store.defer_ocr(path) {
                undurable.insert(path.to_path_buf());
                self.pending_store_healthy.store(false, Ordering::Relaxed);
                self.deferred_overflow.store(true, Ordering::Relaxed);
                // Best effort durable recovery latch; the in-memory failure
                // remains blocking even when the database rejects this too.
                let _ = store.set_ocr_overflow(true);
                return Err(anyhow::Error::msg(error));
            }
            undurable.remove(path);
            self.pending_store_healthy.store(
                undurable.is_empty() && !self.pending_store_load_failed.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
        }
        Ok(())
    }

    /// Clearing this latch only authorizes a new full walk, which rediscovers
    /// overflowed inputs; it is never evidence of a completed reconciliation.
    pub(crate) fn clear_overflow_if_drained(&self) -> bool {
        let Ok(paths) = self.deferred.lock() else {
            return false;
        };
        if !paths.is_empty()
            || !self.pending_store_healthy.load(Ordering::Relaxed)
            || !self.deferred_overflow.load(Ordering::Relaxed)
        {
            return false;
        }
        let Ok(store) = self.pending_store.lock() else {
            return false;
        };
        if store
            .as_ref()
            .is_some_and(|store| store.set_ocr_overflow(false).is_err())
        {
            return false;
        }
        self.deferred_overflow.swap(false, Ordering::Relaxed)
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
        let cooldown = if self.retry_fast.load(Ordering::Relaxed) {
            Duration::from_secs(1)
        } else {
            RETRY_COOLDOWN
        };
        if last_retry.elapsed() < cooldown {
            return Vec::new();
        }
        let Ok(paths) = self.deferred.lock() else {
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
        self.retry_fast.store(false, Ordering::Relaxed);
        *cursor = Some(path.clone());
        // Even missing inputs reach the index's durable terminal ACK, which
        // also removes stale searchable text before clearing this queue.
        // The max parameter cannot multiply the shared project's workload.
        vec![path]
    }

    pub fn is_image(&self, path: &Path) -> bool {
        if self.is_mesh() {
            return remote_image_type(path).is_some();
        }
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
            let input_type = remote_image_type(path)
                .ok_or_else(|| anyhow!("unsupported mesh OCR image format"))?;
            return self.extract_remote(path, input_type);
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
        let result = match result {
            Err(error) if error.is::<crate::compute_client::InvalidOcrInput>() => {
                // A confirmed undecodable image is terminal. The index still
                // acknowledges an empty result only after its SQLite commit.
                Ok(String::new())
            }
            result => result,
        };
        if result.is_err() {
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

fn remote_image_type(path: &Path) -> Option<&'static str> {
    match path
        .extension()
        .and_then(|ext| ext.to_str())
        .and_then(ImageFormat::from_extension)?
    {
        ImageFormat::Png => Some("png"),
        ImageFormat::Jpeg => Some("jpg"),
        ImageFormat::WebP => Some("webp"),
        ImageFormat::Tiff => Some("tiff"),
        ImageFormat::Bmp => Some("bmp"),
        ImageFormat::Gif => Some("gif"),
        ImageFormat::Avif => Some("avif"),
        _ => None,
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
            "cpu" => {
                session = session.with_execution_providers(vec![OrtExecutionProvider::CPU]);
            }
            "cuda" => {
                if !cfg!(feature = "ocr-cuda") {
                    return Err(anyhow!("CUDA OCR support is disabled in this build"));
                }
                if config.gpu_device_id < 0 || config.gpu_mem_limit_bytes == 0 {
                    return Err(anyhow!("invalid CUDA OCR device or memory limit"));
                }
                #[cfg(feature = "ocr-cuda")]
                {
                    require_cuda_environment(config)?;
                }
                // Inherit the strict environment provider on both actual model
                // commits. OAR's explicit CUDA dispatch permits silent fallback.
            }
            _ => return Err(anyhow!("unsupported OCR execution provider")),
        }
        let preprocessing = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .context("initialize bounded OCR preprocessing pool")?;
        // Both models request the same required, validated CUDA provider.
        let ocr = OAROCRBuilder::new(
            config.det_model.as_str(),
            config.rec_model.as_str(),
            config.dict.as_str(),
        )
        .ort_session(session)
        .image_batch_size(1)
        .region_batch_size(1)
        .build()
        .map_err(|error| anyhow!("initialize OAR-OCR pipeline: {error}"))?;
        Ok(Self { ocr, preprocessing })
    }
}

#[cfg(feature = "ocr-cuda")]
fn require_cuda_environment(config: &OcrConfig) -> Result<()> {
    // Configure before the first ORT session. Each real OAR model commit then
    // registers this required provider, failing instead of skipping CUDA.
    static ENVIRONMENT: std::sync::OnceLock<(i32, usize, bool)> = std::sync::OnceLock::new();
    let memory_limit = config.gpu_mem_limit_bytes.min(512 * 1024 * 1024);
    let (device, memory, committed) = ENVIRONMENT.get_or_init(|| {
        let provider = ort::ep::CUDA::default()
            .with_device_id(config.gpu_device_id)
            .with_memory_limit(memory_limit)
            .with_arena_extend_strategy(ort::ep::ArenaExtendStrategy::SameAsRequested)
            .with_conv_algorithm_search(ort::ep::cuda::ConvAlgorithmSearch::Default)
            .with_conv_max_workspace(false)
            .build()
            .error_on_failure();
        (
            config.gpu_device_id,
            memory_limit,
            ort::init().with_execution_providers([provider]).commit(),
        )
    });
    if *device != config.gpu_device_id || *memory != memory_limit {
        return Err(anyhow!("CUDA environment device or memory budget differs"));
    }
    if !committed {
        return Err(anyhow!("ORT was already configured without required CUDA"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn committed_retry_drains_serially_and_failure_restores_backoff() {
        let ocr = OcrEngine::new(OcrConfig {
            backend: "mesh".into(),
            ..OcrConfig::default()
        })
        .unwrap();
        for name in ["a.png", "b.png", "c.png"] {
            ocr.defer_path(Path::new(name)).unwrap();
        }
        let first = ocr.take_retry_paths(10);
        assert_eq!(first, vec![PathBuf::from("a.png")]);
        ocr.forget_deferred(&first[0]);
        assert_eq!(ocr.retry_poll_interval(), Duration::from_secs(1));
        *ocr.last_retry.lock().unwrap() = Instant::now() - Duration::from_millis(1100);
        assert_eq!(ocr.take_retry_paths(10), vec![PathBuf::from("b.png")]);
        // No durable ACK: the same positive failure backoff remains active.
        assert_eq!(ocr.retry_poll_interval(), Duration::from_secs(30));
        assert!(ocr.take_retry_paths(10).is_empty());
        assert_eq!(ocr.deferred_count(), 2);
    }

    #[test]
    fn mesh_image_policy_matches_worker_types_and_normalizes_jpeg_aliases() {
        let ocr = OcrEngine::new(OcrConfig {
            backend: "mesh".into(),
            ..OcrConfig::default()
        })
        .unwrap();
        assert!(ocr.is_image(Path::new("image.jpeg")));
        assert_eq!(remote_image_type(Path::new("image.jpeg")), Some("jpg"));
        assert!(!ocr.is_image(Path::new("application.ico")));
        assert!(!ocr.is_image(Path::new("texture.tga")));
    }

    #[test]
    fn pending_engine_work_survives_recreation_until_durable_ack() {
        let root = Path::new(".tmp/ocr-tests");
        fs::create_dir_all(root).unwrap();
        let dir = tempfile::tempdir_in(root).unwrap();
        let store = PersistentIndex::open(dir.path().join("index.sqlite")).unwrap();
        let path = dir.path().join("input.png");
        let config = OcrConfig {
            backend: "mesh".into(),
            ..OcrConfig::default()
        };
        let first = OcrEngine::new(config.clone()).unwrap();
        first.attach_pending_store(store.clone()).unwrap();
        first.defer_path(&path).unwrap();
        drop(first);
        let second = OcrEngine::new(config).unwrap();
        second.attach_pending_store(store.clone()).unwrap();
        assert!(second.is_deferred(&path));
        // Missing inputs still reach the index's SQL deletion/ACK path.
        assert_eq!(second.take_retry_paths(1), vec![path.clone()]);
        second.forget_deferred(&path);
        assert!(store.load_pending_ocr().unwrap().0.is_empty());
    }

    #[test]
    fn failed_enqueue_retries_durability_and_blocks_completion_until_repaired() {
        let root = Path::new(".tmp/ocr-tests");
        fs::create_dir_all(root).unwrap();
        let dir = tempfile::tempdir_in(root).unwrap();
        let database = dir.path().join("index.sqlite");
        let store = PersistentIndex::open(database.clone()).unwrap();
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection.execute_batch("CREATE TRIGGER enqueue_failure BEFORE INSERT ON grepmesh_pending_ocr BEGIN SELECT RAISE(ABORT, 'controlled enqueue failure'); END;").unwrap();
        let config = OcrConfig {
            backend: "mesh".into(),
            ..OcrConfig::default()
        };
        let engine = OcrEngine::new(config.clone()).unwrap();
        engine.attach_pending_store(store.clone()).unwrap();
        let path = dir.path().join("input.png");
        assert!(engine.defer_path(&path).is_err());
        assert!(engine.is_deferred(&path));
        assert!(!engine.pending_store_healthy.load(Ordering::Relaxed));
        assert!(store.ocr_recovery_full_scan_required().unwrap());
        assert!(!engine.clear_overflow_if_drained());
        connection
            .execute_batch("DROP TRIGGER enqueue_failure;")
            .unwrap();
        // Existing memory entries must still retry their durable insert.
        engine.defer_path(&path).unwrap();
        assert!(engine.pending_store_healthy.load(Ordering::Relaxed));
        drop(engine);
        let reopened = OcrEngine::new(config).unwrap();
        reopened.attach_pending_store(store.clone()).unwrap();
        assert!(reopened.is_deferred(&path));
        reopened.forget_deferred(&path);
        assert!(reopened.clear_overflow_if_drained());
        assert!(store.ocr_recovery_full_scan_required().unwrap());
    }

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
