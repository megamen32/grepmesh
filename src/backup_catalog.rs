//! Fixture-only backup catalog metadata for the local Console.
//!
//! This module deliberately does not know how to authenticate to, list, or
//! restore an object store. The configured fixture is a manifest of aggregate
//! backup metadata; [`BackupCatalogAvailability`] is the separate, browser-safe
//! view of that manifest.

use serde::{Deserialize, Serialize};
use std::fs;

pub use crate::config::BackupCatalogConfig;

/// The four states the Console can show for the independent backup catalog.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackupCatalogState {
    Unconfigured,
    Unavailable,
    Stale,
    Available,
}

/// On-disk fixture format. It contains aggregate backup metadata only.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BackupManifest {
    pub schema_version: u32,
    pub generated_at_ms: u64,
    #[serde(default)]
    pub backups: Vec<BackupManifestEntry>,
}

/// One manifest row; no object path, object content, or credentials are
/// represented by this fixture contract.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BackupManifestEntry {
    pub completed_at_ms: u64,
    pub object_count: u64,
    pub byte_count: u64,
}

/// Browser-safe aggregate availability. It is intentionally not a search or
/// restore response and does not serialize fixture locations or raw entries.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BackupCatalogAvailability {
    pub state: BackupCatalogState,
    pub provider_alias: Option<String>,
    pub prefix: Option<String>,
    pub generated_at_ms: Option<u64>,
    pub backup_count: usize,
    pub object_count: u64,
    pub byte_count: u64,
    pub newest_backup_at_ms: Option<u64>,
}

impl BackupCatalogAvailability {
    fn unconfigured() -> Self {
        Self {
            state: BackupCatalogState::Unconfigured,
            provider_alias: None,
            prefix: None,
            generated_at_ms: None,
            backup_count: 0,
            object_count: 0,
            byte_count: 0,
            newest_backup_at_ms: None,
        }
    }

    fn unavailable(config: &BackupCatalogConfig) -> Self {
        Self {
            state: BackupCatalogState::Unavailable,
            provider_alias: config.provider_alias.clone(),
            prefix: config.prefix.clone(),
            generated_at_ms: None,
            backup_count: 0,
            object_count: 0,
            byte_count: 0,
            newest_backup_at_ms: None,
        }
    }

    fn from_manifest(config: &BackupCatalogConfig, manifest: BackupManifest, now_ms: u64) -> Self {
        let object_count = manifest
            .backups
            .iter()
            .map(|entry| entry.object_count)
            .sum();
        let byte_count = manifest.backups.iter().map(|entry| entry.byte_count).sum();
        let newest_backup_at_ms = manifest
            .backups
            .iter()
            .map(|entry| entry.completed_at_ms)
            .max();
        let state = if now_ms.saturating_sub(manifest.generated_at_ms)
            > config.effective_stale_after_ms()
        {
            BackupCatalogState::Stale
        } else {
            BackupCatalogState::Available
        };

        Self {
            state,
            provider_alias: config.provider_alias.clone(),
            prefix: config.prefix.clone(),
            generated_at_ms: Some(manifest.generated_at_ms),
            backup_count: manifest.backups.len(),
            object_count,
            byte_count,
            newest_backup_at_ms,
        }
    }
}

/// Read one local JSON fixture and reduce it to browser-safe metadata.
///
/// This never opens an S3/Yandex connection, restores data, or searches backup
/// contents. A missing configuration is `unconfigured`; an unreadable or
/// malformed fixture is `unavailable`.
pub fn read_fixture_availability(
    config: Option<&BackupCatalogConfig>,
    now_ms: u64,
) -> BackupCatalogAvailability {
    let Some(config) = config else {
        return BackupCatalogAvailability::unconfigured();
    };
    let Some(path) = config.fixture_path.as_ref() else {
        return BackupCatalogAvailability::unconfigured();
    };

    let manifest = fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<BackupManifest>(&bytes).ok());
    match manifest {
        Some(manifest) => BackupCatalogAvailability::from_manifest(config, manifest, now_ms),
        None => BackupCatalogAvailability::unavailable(config),
    }
}
