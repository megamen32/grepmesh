use grepmesh::{
    backup_catalog::{read_fixture_availability, BackupCatalogState},
    config::BackupCatalogConfig,
};
use serde_json::Value;
use std::path::PathBuf;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/backup_catalog")
        .join(name)
}

fn configured(name: &str) -> BackupCatalogConfig {
    BackupCatalogConfig {
        provider_alias: Some("archive-fixture".into()),
        prefix: Some("grepmesh/archives".into()),
        fixture_path: Some(fixture(name)),
        stale_after_ms: 60_000,
    }
}

#[test]
fn absent_backup_configuration_is_explicitly_unconfigured() {
    let response = read_fixture_availability(None, 1_723_840_000_000);

    assert_eq!(response.state, BackupCatalogState::Unconfigured);
    assert_eq!(response.backup_count, 0);
    assert_eq!(response.object_count, 0);
    assert_eq!(response.byte_count, 0);
    assert_eq!(response.generated_at_ms, None);
}

#[test]
fn configured_catalog_without_a_fixture_is_still_unconfigured() {
    let response = read_fixture_availability(
        Some(&BackupCatalogConfig {
            provider_alias: Some("archive-fixture".into()),
            prefix: Some("grepmesh/archives".into()),
            fixture_path: None,
            stale_after_ms: 60_000,
        }),
        1_723_840_000_000,
    );

    assert_eq!(response.state, BackupCatalogState::Unconfigured);
    assert_eq!(response.provider_alias, None);
    assert_eq!(response.prefix, None);
}

#[test]
fn missing_or_malformed_fixture_is_unavailable_without_a_read_error() {
    let missing = BackupCatalogConfig {
        fixture_path: Some(fixture("missing.json")),
        ..configured("available.json")
    };
    let malformed = configured("malformed.json");

    for config in [&missing, &malformed] {
        let response = read_fixture_availability(Some(config), 1_723_840_000_000);
        assert_eq!(response.state, BackupCatalogState::Unavailable);
        assert_eq!(response.provider_alias.as_deref(), Some("archive-fixture"));
        assert_eq!(response.backup_count, 0);
        assert_eq!(response.generated_at_ms, None);
    }
}

#[test]
fn fixture_metadata_maps_to_available_and_stale_independently_of_live_search() {
    let config = configured("available.json");
    let available = read_fixture_availability(Some(&config), 1_723_840_030_000);
    let stale = read_fixture_availability(Some(&config), 1_723_840_060_001);

    assert_eq!(available.state, BackupCatalogState::Available);
    assert_eq!(stale.state, BackupCatalogState::Stale);
    assert_eq!(available.backup_count, 2);
    assert_eq!(available.object_count, 10);
    assert_eq!(available.byte_count, 4_600);
    assert_eq!(available.newest_backup_at_ms, Some(1_723_839_500_000));
}

#[test]
fn serialized_response_contains_only_browser_safe_catalog_metadata() {
    let config = configured("available.json");
    let response = read_fixture_availability(Some(&config), 1_723_840_030_000);
    let json = serde_json::to_value(response).unwrap();
    let object = json.as_object().unwrap();

    for forbidden in [
        "fixture_path",
        "path",
        "backups",
        "results",
        "host_id",
        "partial",
        "truncated",
        "token",
        "secret",
        "credential",
        "password",
        "access_key",
        "authorization",
    ] {
        assert!(
            !object.contains_key(forbidden),
            "response unexpectedly exposed {forbidden}"
        );
    }
    assert_eq!(object["state"], Value::String("available".into()));
    assert_eq!(
        object["provider_alias"],
        Value::String("archive-fixture".into())
    );
}
