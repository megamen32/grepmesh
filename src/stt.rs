use crate::config::SttConfig;
use anyhow::{anyhow, Context, Result};
use bzip2::read::BzDecoder;
use sherpa_onnx::{OfflineRecognizer, OfflineRecognizerConfig, OfflineTransducerModelConfig, Wave};
use std::{
    fs, io,
    path::{Path, PathBuf},
    process::Command,
    sync::Mutex,
};
use tar::Archive;

const PARAKEET_PACKAGE: &str = "sherpa-onnx-nemo-parakeet-tdt-0.6b-v3-int8";
const PARAKEET_URL: &str = "https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/sherpa-onnx-nemo-parakeet-tdt-0.6b-v3-int8.tar.bz2";
const MEDIA_EXTENSIONS: &[&str] = &[
    "wav", "mp3", "m4a", "aac", "flac", "ogg", "opus", "wma", "mp4", "mkv", "mov", "webm", "avi",
    "m4v",
];

pub struct SttEngine {
    config: SttConfig,
    recognizer: Mutex<Option<OfflineRecognizer>>,
}

impl SttEngine {
    pub fn new(config: SttConfig) -> Option<Self> {
        config.enabled.then_some(Self {
            config,
            recognizer: Mutex::new(None),
        })
    }

    pub fn is_media(&self, path: &Path) -> bool {
        path.extension()
            .and_then(|value| value.to_str())
            .map(|value| MEDIA_EXTENSIONS.contains(&value.to_ascii_lowercase().as_str()))
            .unwrap_or(false)
    }

    pub fn max_media_bytes(&self) -> u64 {
        self.config.max_media_bytes
    }

    pub fn transcribe(&self, path: &Path) -> Result<String> {
        let model_dir = self.ensure_model()?;
        let wav = self.prepare_wav(path)?;
        let wave_path = wav.as_deref().unwrap_or(path);
        let wave = Wave::read(&wave_path.to_string_lossy())
            .ok_or_else(|| anyhow!("cannot decode WAV {}", wave_path.display()))?;

        let mut guard = self
            .recognizer
            .lock()
            .map_err(|_| anyhow!("STT recognizer lock is unavailable"))?;
        if guard.is_none() {
            *guard = Some(create_parakeet_recognizer(&model_dir)?);
        }
        let recognizer = guard.as_ref().expect("recognizer initialized");
        let stream = recognizer.create_stream();
        stream.accept_waveform(wave.sample_rate(), wave.samples());
        recognizer.decode(&stream);
        let result = stream
            .get_result()
            .ok_or_else(|| anyhow!("Parakeet returned no result"))?;
        let text = result.text.trim();
        if text.is_empty() {
            return Ok(String::new());
        }

        let mut output = format!("# Transcript\n\n{text}\n");
        if let Some(timestamps) = result.timestamps {
            if timestamps.len() == result.tokens.len() && !timestamps.is_empty() {
                output.push_str("\n# Timestamps\n\n");
                for (timestamp, token) in timestamps.iter().zip(result.tokens.iter()) {
                    output.push_str(&format!("[{}] {}\n", format_timestamp(*timestamp), token));
                }
            }
        }
        Ok(output)
    }

    fn effective_model_dir(&self) -> Result<PathBuf> {
        if !matches!(self.config.backend.as_str(), "auto" | "parakeet") {
            return Err(anyhow!("unsupported STT backend {}", self.config.backend));
        }
        if !matches!(
            self.config.model.as_str(),
            "auto" | "parakeet-tdt-0.6b-v3-int8"
        ) {
            return Err(anyhow!("unsupported STT model {}", self.config.model));
        }
        let base = if let Some(path) = self.config.model_dir.clone() {
            path
        } else {
            let home = std::env::var_os("HOME").ok_or_else(|| anyhow!("HOME is not set"))?;
            PathBuf::from(home).join(".cache/grepmesh/models")
        };
        Ok(base.join(PARAKEET_PACKAGE))
    }

    fn ensure_model(&self) -> Result<PathBuf> {
        let model_dir = self.effective_model_dir()?;
        if model_files_ready(&model_dir) {
            return Ok(model_dir);
        }
        if !self.config.auto_download {
            return Err(anyhow!(
                "Parakeet model is missing at {} and auto_download is disabled",
                model_dir.display()
            ));
        }
        let base = model_dir
            .parent()
            .ok_or_else(|| anyhow!("invalid model directory"))?;
        fs::create_dir_all(base).with_context(|| format!("create {}", base.display()))?;
        let archive_path = base.join(format!("{PARAKEET_PACKAGE}.tar.bz2.part"));
        tracing::info!(url = PARAKEET_URL, path = %model_dir.display(), "downloading STT model");
        let response = ureq::get(PARAKEET_URL)
            .call()
            .map_err(|error| anyhow!("download Parakeet model: {error}"))?;
        let mut reader = response.into_reader();
        let mut file = fs::File::create(&archive_path)
            .with_context(|| format!("create {}", archive_path.display()))?;
        io::copy(&mut reader, &mut file).context("write Parakeet model archive")?;
        drop(file);
        let archive = fs::File::open(&archive_path)?;
        let decoder = BzDecoder::new(archive);
        Archive::new(decoder)
            .unpack(base)
            .with_context(|| format!("extract Parakeet model into {}", base.display()))?;
        let _ = fs::remove_file(&archive_path);
        if !model_files_ready(&model_dir) {
            return Err(anyhow!("downloaded Parakeet package is incomplete"));
        }
        Ok(model_dir)
    }

    fn prepare_wav(&self, path: &Path) -> Result<Option<tempfile::TempPath>> {
        let temp = tempfile::Builder::new().suffix(".wav").tempfile()?;
        let output = Command::new("ffmpeg")
            .arg("-v")
            .arg("error")
            .arg("-y")
            .arg("-i")
            .arg(path)
            .arg("-vn")
            .arg("-ac")
            .arg("1")
            .arg("-ar")
            .arg("16000")
            .arg("-c:a")
            .arg("pcm_s16le")
            .arg(temp.path())
            .output();
        match output {
            Ok(output) if output.status.success() => Ok(Some(temp.into_temp_path())),
            Ok(output) => Err(anyhow!(
                "ffmpeg failed for {}: {}",
                path.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            )),
            Err(_error)
                if path
                    .extension()
                    .and_then(|value| value.to_str())
                    .is_some_and(|value| value.eq_ignore_ascii_case("wav")) =>
            {
                drop(temp);
                Ok(None)
            }
            Err(error) => Err(anyhow!(
                "ffmpeg is required to transcribe {}: {error}",
                path.display()
            )),
        }
    }
}

fn create_parakeet_recognizer(model_dir: &Path) -> Result<OfflineRecognizer> {
    let mut config = OfflineRecognizerConfig::default();
    config.model_config.transducer = OfflineTransducerModelConfig {
        encoder: Some(
            model_dir
                .join("encoder.int8.onnx")
                .to_string_lossy()
                .into_owned(),
        ),
        decoder: Some(
            model_dir
                .join("decoder.int8.onnx")
                .to_string_lossy()
                .into_owned(),
        ),
        joiner: Some(
            model_dir
                .join("joiner.int8.onnx")
                .to_string_lossy()
                .into_owned(),
        ),
    };
    config.model_config.tokens = Some(model_dir.join("tokens.txt").to_string_lossy().into_owned());
    config.model_config.model_type = Some("nemo_transducer".to_string());
    OfflineRecognizer::create(&config)
        .ok_or_else(|| anyhow!("cannot initialize Parakeet recognizer"))
}

fn model_files_ready(dir: &Path) -> bool {
    [
        "encoder.int8.onnx",
        "decoder.int8.onnx",
        "joiner.int8.onnx",
        "tokens.txt",
    ]
    .iter()
    .all(|name| dir.join(name).is_file())
}

fn format_timestamp(seconds: f32) -> String {
    let millis = (seconds.max(0.0) * 1000.0).round() as u64;
    let hours = millis / 3_600_000;
    let minutes = (millis / 60_000) % 60;
    let seconds = (millis / 1_000) % 60;
    let millis = millis % 1_000;
    format!("{hours:02}:{minutes:02}:{seconds:02}.{millis:03}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_selects_parakeet_and_recognizes_media_extensions_without_download() {
        let engine = SttEngine::new(SttConfig {
            enabled: true,
            auto_download: false,
            ..Default::default()
        })
        .unwrap();
        assert!(engine.is_media(Path::new("meeting.mp4")));
        assert!(engine.is_media(Path::new("voice.M4A")));
        assert!(!engine.is_media(Path::new("notes.txt")));
        assert!(engine
            .effective_model_dir()
            .unwrap()
            .ends_with(PARAKEET_PACKAGE));
    }

    #[test]
    fn timestamps_are_search_friendly_and_stable() {
        assert_eq!(format_timestamp(65.432), "00:01:05.432");
    }
}
