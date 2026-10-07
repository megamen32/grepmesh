use crate::config::OcrConfig;
use anyhow::{anyhow, Result};
use std::path::Path;

/// Lightweight build used on hosts where the local index and OCR are disabled.
pub struct OcrEngine;

impl OcrEngine {
    pub(crate) fn defer_path(&self, _path: &Path) -> Result<()> {
        Ok(())
    }
    pub(crate) fn is_deferred(&self, _path: &Path) -> bool {
        false
    }
    pub(crate) fn forget_deferred(&self, _path: &Path) {}
    pub(crate) fn is_mesh(&self) -> bool {
        false
    }
    pub(crate) fn deferred_count(&self) -> usize {
        0
    }
    pub(crate) fn take_retry_paths(&self, _max: usize) -> Vec<std::path::PathBuf> {
        Vec::new()
    }
    pub(crate) fn clear_overflow_if_drained(&self) -> bool {
        false
    }
    pub fn new(_config: OcrConfig) -> Option<Self> {
        None
    }

    pub fn is_image(&self, _path: &Path) -> bool {
        false
    }

    pub fn is_pdf(&self, _path: &Path) -> bool {
        false
    }

    pub fn max_image_bytes(&self) -> u64 {
        0
    }

    pub fn should_ocr_pdf(&self, _extracted: Option<&str>) -> bool {
        false
    }

    pub fn extract_image(&self, _path: &Path) -> Result<String> {
        Err(anyhow!("OCR support is disabled in this build"))
    }

    pub fn extract_pdf(&self, _path: &Path) -> Result<String> {
        Err(anyhow!("OCR support is disabled in this build"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_build_never_constructs_an_ocr_engine() {
        assert!(OcrEngine::new(OcrConfig::default()).is_none());
    }
}
