use crate::compute_client::{mesh_input_limit, ComputeClient, MAX_PARALLEL_OCR};
use crate::config::OcrConfig;
use crate::index::PersistentIndex;
use anyhow::{anyhow, Context, Result};
use image::ImageFormat;
use oar_ocr::{
    core::config::{OrtExecutionProvider, OrtSessionConfig},
    domain::tasks::TextDetectionConfig,
    oarocr::{OAROCRBuilder, OAROCR},
    utils::load_image,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

const MAX_DEFERRED_PATHS: usize = 65_536;
const RETRY_COOLDOWN: Duration = Duration::from_secs(60);

fn gpu_detection_config(config: &OcrConfig) -> Option<TextDetectionConfig> {
    matches!(config.execution_provider.as_str(), "cuda" | "coreml").then(|| TextDetectionConfig {
        // The default 960-side detector exhausted the unchanged 512 MiB ORT
        // arena on ordinary queued screenshots. Bound detector activations;
        // recognition still crops the original-resolution image.
        limit_side_len: Some(512),
        limit_type: Some(oar_ocr::processors::LimitType::Max),
        max_side_len: Some(512),
        // Preserve OAR's general-text preset while overriding its resize only.
        unclip_ratio: 2.0,
        ..TextDetectionConfig::default()
    })
}

pub struct OcrEngine {
    config: OcrConfig,
    pipeline: Mutex<Option<OcrRuntime>>,
    mesh: Mutex<Vec<ComputeClient>>,
    deferred: Mutex<BTreeSet<PathBuf>>,
    active_paths: Mutex<BTreeSet<PathBuf>>,
    last_retry: Mutex<Instant>,
    retry_cursor: Mutex<Option<PathBuf>>,
    retry_after: Mutex<BTreeMap<PathBuf, Instant>>,
    metadata_retry_cursor: Mutex<Option<PathBuf>>,
    deferred_overflow: AtomicBool,
    pending_store: Mutex<Option<PersistentIndex>>,
    pending_store_healthy: AtomicBool,
    pending_store_load_failed: AtomicBool,
    undurable: Mutex<BTreeSet<PathBuf>>,
    retry_fast: AtomicBool,
}

pub(crate) struct RetryPathLease {
    engine: Arc<OcrEngine>,
    path: PathBuf,
}
impl Drop for RetryPathLease {
    fn drop(&mut self) {
        if let Ok(mut paths) = self.engine.active_paths.lock() {
            paths.remove(&self.path);
        }
    }
}

struct MeshClientLease<'a> {
    pool: &'a Mutex<Vec<ComputeClient>>,
    client: Option<ComputeClient>,
}
impl Drop for MeshClientLease<'_> {
    fn drop(&mut self) {
        if let (Some(client), Ok(mut pool)) = (self.client.take(), self.pool.lock()) {
            pool.push(client);
        }
    }
}

struct OcrRuntime {
    ocr: OAROCR,
    preprocessing: rayon::ThreadPool,
}

impl OcrEngine {
    /// Prepare only the preferred global provider before a CPU override;
    /// constructing model sessions remains lazy and within worker admission.
    pub(crate) fn prepare_accelerator_environment(&self) -> Result<()> {
        match self.config.execution_provider.as_str() {
            "cuda" => {
                #[cfg(feature = "ocr-cuda")]
                {
                    return require_cuda_environment(&self.config);
                }
                #[cfg(not(feature = "ocr-cuda"))]
                {
                    Err(anyhow!("CUDA support is absent from this build"))
                }
            }
            "coreml" => {
                #[cfg(all(feature = "ocr-coreml", target_os = "macos", target_arch = "aarch64"))]
                {
                    return require_coreml_environment();
                }
                #[cfg(not(all(
                    feature = "ocr-coreml",
                    target_os = "macos",
                    target_arch = "aarch64"
                )))]
                {
                    Err(anyhow!("CoreML support is absent from this build"))
                }
            }
            "cpu" => Ok(()),
            _ => Err(anyhow!("unsupported OCR execution provider")),
        }
    }
    pub(crate) fn native_initialized(&self) -> bool {
        self.pipeline
            .lock()
            .map(|pipeline| pipeline.is_some())
            .unwrap_or(false)
    }
    pub fn new(config: OcrConfig) -> Option<Self> {
        let claimed = Arc::new(Mutex::new(BTreeSet::new()));
        let clients = (0..MAX_PARALLEL_OCR)
            .map(|_| ComputeClient::with_shared_workers(config.clone(), Arc::clone(&claimed)))
            .collect();
        config.enabled.then_some(Self {
            config,
            pipeline: Mutex::new(None),
            mesh: Mutex::new(clients),
            deferred: Mutex::new(BTreeSet::new()),
            active_paths: Mutex::new(BTreeSet::new()),
            last_retry: Mutex::new(
                Instant::now()
                    .checked_sub(RETRY_COOLDOWN)
                    .unwrap_or_else(Instant::now),
            ),
            retry_cursor: Mutex::new(None),
            retry_after: Mutex::new(BTreeMap::new()),
            metadata_retry_cursor: Mutex::new(None),
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
        let Ok(paths) = self.deferred.lock() else {
            return Duration::from_secs(30);
        };
        let Ok(after) = self.retry_after.lock() else {
            return Duration::from_secs(30);
        };
        let now = Instant::now();
        if paths
            .iter()
            .any(|path| after.get(path).is_none_or(|at| *at <= now))
        {
            Duration::from_secs(1)
        } else {
            after
                .values()
                .map(|at| at.saturating_duration_since(now))
                .min()
                .unwrap_or(Duration::from_secs(30))
                .clamp(Duration::from_secs(1), Duration::from_secs(30))
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

    pub(crate) fn is_in_flight(&self, path: &Path) -> bool {
        self.active_paths
            .lock()
            .map(|paths| paths.contains(path))
            .unwrap_or(true)
    }
    pub(crate) fn claim_retry(self: &Arc<Self>, path: &Path) -> Option<RetryPathLease> {
        if !self.active_paths.lock().ok()?.insert(path.to_path_buf()) {
            return None;
        }
        Some(RetryPathLease {
            engine: Arc::clone(self),
            path: path.to_path_buf(),
        })
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
            if let Ok(mut after) = self.retry_after.lock() {
                after.remove(path);
            }
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
        self.take_retry_paths_excluding(max.min(1), &BTreeSet::new())
    }

    pub(crate) fn take_retry_paths_excluding(
        &self,
        max: usize,
        in_flight: &BTreeSet<PathBuf>,
    ) -> Vec<PathBuf> {
        if !self.is_mesh() || max == 0 {
            return Vec::new();
        }
        let Ok(mut last_retry) = self.last_retry.lock() else {
            return Vec::new();
        };
        // Only throttle dispatch observation itself. Unrelated files must not
        // inherit another input's resource/transport retry delay.
        if last_retry.elapsed() < Duration::from_secs(1) {
            return Vec::new();
        }
        let Ok(paths) = self.deferred.lock() else {
            return Vec::new();
        };
        let Ok(mut cursor) = self.retry_cursor.lock() else {
            return Vec::new();
        };
        let ordered: Vec<_> = cursor
            .as_ref()
            .map(|previous| {
                paths
                    .range((
                        std::ops::Bound::Excluded(previous.clone()),
                        std::ops::Bound::Unbounded,
                    ))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let Ok(mut after) = self.retry_after.lock() else {
            return Vec::new();
        };
        let now = Instant::now();
        let mut unique = BTreeSet::new();
        let next: Vec<_> = ordered
            .into_iter()
            .chain(paths.iter())
            .filter(|path| {
                !in_flight.contains(*path)
                    && after.get(*path).is_none_or(|at| *at <= now)
                    && unique.insert((*path).clone())
            })
            .take(max.min(MAX_PARALLEL_OCR))
            .cloned()
            .collect();
        if let Some(path) = next.last() {
            *last_retry = Instant::now();
            self.retry_fast.store(false, Ordering::Relaxed);
            for selected in &next {
                after.insert(selected.clone(), now + RETRY_COOLDOWN);
            }
            *cursor = Some(path.clone());
        }
        next
    }

    pub(crate) fn metadata_retry_paths(
        &self,
        max: usize,
        accept: impl Fn(&Path) -> bool,
    ) -> Vec<PathBuf> {
        if !self.is_mesh() || max == 0 {
            return Vec::new();
        }
        let Ok(paths) = self.deferred.lock() else {
            return Vec::new();
        };
        let Ok(mut cursor) = self.metadata_retry_cursor.lock() else {
            return Vec::new();
        };
        let ordered: Box<dyn Iterator<Item = &PathBuf> + '_> =
            if let Some(previous) = cursor.as_ref() {
                Box::new(
                    paths
                        .range::<PathBuf, _>((
                            std::ops::Bound::Excluded(previous.clone()),
                            std::ops::Bound::Unbounded,
                        ))
                        .chain(paths.range::<PathBuf, _>((
                            std::ops::Bound::Unbounded,
                            std::ops::Bound::Included(previous.clone()),
                        ))),
                )
            } else {
                Box::new(paths.iter())
            };
        let selected = ordered
            .filter(|path| accept(path))
            .take(max.min(64))
            .cloned()
            .collect::<Vec<_>>();
        if let Some(last) = selected.last() {
            *cursor = Some(last.clone());
        }
        selected
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
        let deadline = Instant::now()
            + Duration::from_millis(self.config.remote_timeout_ms.clamp(1_000, 300_000));
        let mut located: Vec<(f32, f32, f32, f32, String)> = Vec::new();
        let mut text_bytes = 0usize;
        let tiled = runtime.preprocessing.install(|| {
            crate::ocr_png::stream_tiles(path, deadline, |image, x, y| {
                for page in runtime
                    .ocr
                    .predict(vec![image])
                    .context("run tiled OAR-OCR")?
                {
                    for region in page.text_regions {
                        let Some(text) = region
                            .text
                            .as_deref()
                            .map(str::trim)
                            .filter(|text| !text.is_empty())
                        else {
                            continue;
                        };
                        if region.bounding_box.points.is_empty()
                            || region
                                .bounding_box
                                .points
                                .iter()
                                .any(|p| !p.x.is_finite() || !p.y.is_finite())
                        {
                            return Err(anyhow!("tiled OCR text geometry is invalid"));
                        }
                        let points = &region.bounding_box.points;
                        let left =
                            points.iter().map(|p| p.x).fold(f32::INFINITY, f32::min) + x as f32;
                        let top =
                            points.iter().map(|p| p.y).fold(f32::INFINITY, f32::min) + y as f32;
                        let right =
                            points.iter().map(|p| p.x).fold(f32::NEG_INFINITY, f32::max) + x as f32;
                        let bottom =
                            points.iter().map(|p| p.y).fold(f32::NEG_INFINITY, f32::max) + y as f32;
                        // Deduplicate only the same text at overlapping global geometry.
                        // Repeated text at another document position remains separate.
                        let duplicate = located.iter().any(|(l, t, r, b, old)| {
                            old == text
                                && left < *r
                                && right > *l
                                && top < *b
                                && bottom > *t
                                && ((top + bottom) - (t + b)).abs()
                                    < (bottom - top).min(b - t).max(1.0) * 0.5
                                && ((left + right) - (l + r)).abs()
                                    < (right - left).min(r - l).max(1.0) * 0.5
                        });
                        if !duplicate {
                            text_bytes = text_bytes
                                .checked_add(text.len() + 1)
                                .ok_or_else(|| anyhow!("tiled OCR text limit exceeded"))?;
                            if text_bytes > 1024 * 1024 || located.len() >= 8192 {
                                return Err(anyhow!("tiled OCR text limit exceeded"));
                            }
                            located.push((left, top, right, bottom, text.to_string()));
                        }
                    }
                }
                Ok(())
            })
        })?;
        if tiled.is_some() {
            located.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.total_cmp(&b.0)));
            return Ok(located
                .into_iter()
                .map(|(_, _, _, _, text)| text)
                .collect::<Vec<_>>()
                .join("\n"));
        }
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
        // A PDF retained after a prior parser failure may still have a native
        // text layer. Re-check it before spending a worker slot on raster OCR.
        if let Some(text) = native_pdf_text(path, self.config.min_pdf_text_chars) {
            return Ok(text);
        }
        if self.config.backend == "mesh" {
            return self.extract_remote(path, "pdf");
        }
        self.extract_pdf_in(path, Path::new(".tmp/ocr"))
    }

    pub(crate) fn skip_recognition_for_version(
        &self,
        path: &Path,
        size: u64,
        mtime_ns: u128,
    ) -> Result<bool> {
        let store = self
            .pending_store
            .lock()
            .map_err(|_| anyhow!("OCR failure ledger lock unavailable"))?
            .clone();
        match store {
            Some(store) => Ok(store
                .ocr_terminal_empty(path, size, mtime_ns)
                .map_err(anyhow::Error::msg)?
                || store
                    .ocr_failure_count(path, size, mtime_ns)
                    .map_err(anyhow::Error::msg)?
                    >= crate::compute_client::MAX_RECOGNITION_FAILURES),
            None => Ok(false),
        }
    }

    fn extract_remote(&self, path: &Path, input_type: &str) -> Result<String> {
        use crate::compute_client::{OcrRecognitionFailure, MAX_RECOGNITION_FAILURES};
        let metadata = fs::metadata(path)?;
        let size = metadata.len();
        let mtime_ns = crate::index::metadata_mtime_ns(&metadata);
        let store = self
            .pending_store
            .lock()
            .map_err(|_| anyhow!("OCR failure ledger lock unavailable"))?
            .clone();
        let failures = match &store {
            Some(store) => store
                .ocr_failure_count(path, size, mtime_ns)
                .map_err(anyhow::Error::msg)?,
            None => 0,
        };
        if failures >= MAX_RECOGNITION_FAILURES
            || store
                .as_ref()
                .map(|store| store.ocr_terminal_empty(path, size, mtime_ns))
                .transpose()
                .map_err(anyhow::Error::msg)?
                .unwrap_or(false)
        {
            return Ok(String::new());
        }
        let result = (|| {
            let mut lease = MeshClientLease {
                pool: &self.mesh,
                client: Some(
                    self.mesh
                        .lock()
                        .map_err(|_| anyhow!("mesh OCR lock unavailable"))?
                        .pop()
                        .ok_or_else(|| anyhow!("mesh OCR client slots busy"))?,
                ),
            };
            lease
                .client
                .as_mut()
                .expect("client lease")
                .extract_with_budget(path, input_type, MAX_RECOGNITION_FAILURES - failures)
        })();
        let result = match result {
            Err(error)
                if error.is::<crate::compute_client::InvalidOcrInput>()
                    || error.is::<OcrRecognitionFailure>() =>
            {
                let attempts = error
                    .downcast_ref::<OcrRecognitionFailure>()
                    .map(|failure| failure.attempts)
                    .unwrap_or(1);
                let stable = fs::metadata(path)
                    .map(|current| {
                        current.len() == size
                            && crate::index::metadata_mtime_ns(&current) == mtime_ns
                    })
                    .unwrap_or(false);
                if stable {
                    let total = match &store {
                        Some(store) => store
                            .record_ocr_failures(path, size, mtime_ns, attempts)
                            .map_err(anyhow::Error::msg)?,
                        None => failures.saturating_add(attempts),
                    };
                    if total >= MAX_RECOGNITION_FAILURES {
                        tracing::info!(input = %path.display(), failures = total, "mesh OCR version skipped after processing failures");
                        Ok(String::new())
                    } else {
                        Err(error)
                    }
                } else {
                    Err(error)
                }
            }
            result => result,
        };
        if result.is_err() {
            self.defer_path(path)?;
            if result.as_ref().is_err_and(|error| {
                error.is::<OcrRecognitionFailure>()
                    || error.is::<crate::compute_client::InvalidOcrInput>()
            }) {
                // A finished input failure has a finite generation budget;
                // continue useful work without an outage backoff.
                if let Ok(mut after) = self.retry_after.lock() {
                    after.insert(path.to_path_buf(), Instant::now() + Duration::from_secs(1));
                }
            }
        }
        // The index owns durable acknowledgement and clears failures only
        // in the transaction committing a successful document.
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

// Real Poppler extraction; missing Poppler or insufficient text retains OCR.
// The temporary file is unlinked, output is bounded, and an expired subprocess
// is killed and reaped before returning. No private text reaches diagnostics.
fn native_pdf_text(path: &Path, minimum_chars: usize) -> Option<String> {
    use std::io::{Read, Seek, SeekFrom};
    const MAX_TEXT_BYTES: u64 = 4 * 1024 * 1024;
    let mut output = tempfile::tempfile().ok()?;
    let writer = output.try_clone().ok()?;
    let mut child = Command::new("pdftotext")
        .arg("-layout")
        .arg(path)
        .arg("-")
        .stdin(Stdio::null())
        .stdout(writer)
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if output
            .metadata()
            .ok()
            .is_none_or(|m| m.len() > MAX_TEXT_BYTES)
            || Instant::now() >= deadline
        {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        match child.try_wait() {
            Ok(Some(status)) if status.success() => break,
            Ok(Some(_)) => return None,
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            Ok(None) => thread::sleep(Duration::from_millis(25)),
        }
    }
    output.seek(SeekFrom::Start(0)).ok()?;
    let mut text = String::new();
    output
        .take(MAX_TEXT_BYTES + 1)
        .read_to_string(&mut text)
        .ok()?;
    if text.len() as u64 > MAX_TEXT_BYTES
        || text.trim().is_empty()
        || text.chars().filter(|c| !c.is_whitespace()).count() < minimum_chars
    {
        return None;
    }
    Some(text)
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
            "coreml" => {
                if !cfg!(all(
                    feature = "ocr-coreml",
                    target_os = "macos",
                    target_arch = "aarch64"
                )) {
                    return Err(anyhow!(
                        "Apple CoreML OCR support is absent from this build"
                    ));
                }
                #[cfg(all(feature = "ocr-coreml", target_os = "macos", target_arch = "aarch64"))]
                require_coreml_environment()?;
                if std::env::var("GREPMESH_COREML_DIAGNOSTICS").as_deref() == Ok("1") {
                    session = session.with_log_severity_level(0);
                }
                // Required CoreML EP registration, CPU/GPU compute units.
                // This does not claim that every model operator runs on GPU.
            }
            _ => return Err(anyhow!("unsupported OCR execution provider")),
        }
        let preprocessing = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .context("initialize bounded OCR preprocessing pool")?;
        // Both models request the same required, validated CUDA provider.
        let mut builder = OAROCRBuilder::new(
            config.det_model.as_str(),
            config.rec_model.as_str(),
            config.dict.as_str(),
        )
        .ort_session(session)
        .image_batch_size(1)
        .region_batch_size(1);
        if let Some(detection) = gpu_detection_config(config) {
            builder = builder.text_detection_config(detection);
        }
        let ocr = builder
            .build()
            .map_err(|error| anyhow!("initialize OAR-OCR pipeline: {error}"))?;
        Ok(Self { ocr, preprocessing })
    }
}

#[cfg(all(feature = "ocr-coreml", target_os = "macos", target_arch = "aarch64"))]
fn require_coreml_environment() -> Result<()> {
    static ENVIRONMENT: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let committed = *ENVIRONMENT.get_or_init(|| {
        let provider = ort::ep::CoreML::default()
            .with_compute_units(ort::ep::coreml::ComputeUnits::CPUAndGPU)
            .with_model_format(ort::ep::coreml::ModelFormat::MLProgram)
            .with_profile_compute_plan(true)
            .build()
            .error_on_failure();
        ort::init().with_execution_providers([provider]).commit()
    });
    if !committed {
        return Err(anyhow!(
            "ORT was already configured without required CoreML"
        ));
    }
    Ok(())
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
    fn parallel_retry_is_bounded_skips_inflight_and_never_acknowledges_dispatch() {
        let engine = OcrEngine::new(OcrConfig {
            backend: "mesh".into(),
            ..Default::default()
        })
        .unwrap();
        for name in ["a.png", "b.png", "c.png", "d.png", "e.png"] {
            engine.defer_path(Path::new(name)).unwrap();
        }
        let inflight = BTreeSet::from([PathBuf::from("b.png")]);
        let batch = engine.take_retry_paths_excluding(99, &inflight);
        assert_eq!(
            batch,
            vec![
                PathBuf::from("a.png"),
                PathBuf::from("c.png"),
                PathBuf::from("d.png")
            ]
        );
        assert_eq!(engine.deferred_count(), 5, "dispatch is not a durable ACK");
        engine.retry_fast.store(true, Ordering::Relaxed);
        *engine.last_retry.lock().unwrap() = Instant::now() - Duration::from_secs(2);
        let active = batch.into_iter().chain(inflight).collect();
        assert_eq!(
            engine.take_retry_paths_excluding(3, &active),
            vec![PathBuf::from("e.png")]
        );
    }

    #[test]
    fn energy_recovery_gpu_detector_respects_existing_arena_budget() {
        let config = OcrConfig {
            execution_provider: "cuda".into(),
            ..OcrConfig::default()
        };
        let detection = gpu_detection_config(&config).expect("CUDA needs a bounded detector shape");
        assert_eq!(detection.limit_side_len, Some(512));
        assert_eq!(detection.max_side_len, Some(512));
        assert!(matches!(
            detection.limit_type,
            Some(oar_ocr::processors::LimitType::Max)
        ));
        assert_eq!(config.gpu_mem_limit_bytes, 512 * 1024 * 1024);
        assert!(gpu_detection_config(&OcrConfig::default()).is_none());
    }

    #[test]
    fn energy_recovery_metadata_batch_is_bounded_without_gpu_retry() {
        let ocr = OcrEngine::new(OcrConfig {
            backend: "mesh".into(),
            ..OcrConfig::default()
        })
        .unwrap();
        ocr.defer_path(Path::new("a-ordinary.png")).unwrap();
        for i in 0..70 {
            ocr.defer_path(Path::new(&format!("generated/.tmpbin/{i:03}.png")))
                .unwrap();
        }
        let previous = *ocr.last_retry.lock().unwrap();
        let paths =
            ocr.metadata_retry_paths(200, |path| path.to_string_lossy().contains("/.tmpbin/"));
        assert_eq!(paths.len(), 64);
        assert!(paths
            .iter()
            .all(|path| path.to_string_lossy().contains("/.tmpbin/")));
        assert_eq!(
            *ocr.last_retry.lock().unwrap(),
            previous,
            "metadata selection must not consume GPU cooldown"
        );
        assert_eq!(ocr.deferred_count(), 71, "selection must not ACK");
    }

    #[test]
    fn failed_input_backoff_does_not_delay_other_queue_files() {
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
        // b remains pending and cooling down, while c can use a free worker.
        assert_eq!(ocr.retry_poll_interval(), Duration::from_secs(1));
        assert!(ocr.take_retry_paths(10).is_empty());
        *ocr.last_retry.lock().unwrap() = Instant::now() - Duration::from_millis(1100);
        assert_eq!(ocr.take_retry_paths(10), vec![PathBuf::from("c.png")]);
        *ocr.last_retry.lock().unwrap() = Instant::now() - Duration::from_millis(1100);
        assert!(ocr.take_retry_paths(10).is_empty());
        assert!(ocr.retry_poll_interval() > Duration::from_secs(1));
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
