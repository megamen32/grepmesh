use crate::config::OcrConfig;
use anyhow::{anyhow, Result};
use std::path::Path;

/// Lightweight build used on hosts where the local index and OCR are disabled.
pub struct OcrEngine;

impl OcrEngine {
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
