use crate::config::OcrConfig;
use anyhow::{anyhow, Context, Result};
use image::ImageFormat;
use oar_ocr::{
    core::config::OrtSessionConfig,
    oarocr::{OAROCRBuilder, OAROCR},
    utils::load_image,
};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Mutex,
};

pub struct OcrEngine {
    config: OcrConfig,
    pipeline: Mutex<Option<OcrRuntime>>,
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
        })
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
        self.config.max_image_bytes
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
        let mut pipeline = self
            .pipeline
            .lock()
            .map_err(|_| anyhow!("OCR pipeline lock is unavailable"))?;
        if pipeline.is_none() {
            let preprocessing = rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .context("initialize bounded OCR preprocessing pool")?;
            let ocr = OAROCRBuilder::new(
                self.config.det_model.as_str(),
                self.config.rec_model.as_str(),
                self.config.dict.as_str(),
            )
            // Detection and recognition each own an ORT session. Host
            // parallelism ignores the shared service's two-CPU budget;
            // worker pools then compete with each other and spin while
            // throttled. OCR already runs serially under this mutex.
            .ort_session(
                OrtSessionConfig::new()
                    .with_intra_threads(1)
                    .with_inter_threads(1)
                    .with_parallel_execution(false)
                    .add_config_entry("session.intra_op.allow_spinning", "0")
                    .add_config_entry("session.inter_op.allow_spinning", "0"),
            )
            .build()
            .context("initialize OAR-OCR pipeline")?;
            *pipeline = Some(OcrRuntime { ocr, preprocessing });
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
        let dir = tempfile::tempdir().context("create PDF OCR tempdir")?;
        let prefix = dir.path().join("page");
        let output = Command::new("pdftoppm")
            .arg("-png")
            .arg("-r")
            .arg(self.config.pdf_dpi.to_string())
            .arg("-f")
            .arg("1")
            .arg("-l")
            .arg(self.config.max_pdf_pages.to_string())
            .arg(path)
            .arg(&prefix)
            .output()
            .with_context(|| format!("run pdftoppm for {}", path.display()))?;
        if !output.status.success() {
            return Err(anyhow!(
                "pdftoppm failed for {}: {}",
                path.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            ));
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

#[cfg(test)]
mod tests {
    use super::*;

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
