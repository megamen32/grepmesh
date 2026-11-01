use grepmesh::{
    backend::{LocalBackend, SearchMode},
    mcp::{HostsInput, MeshService, SearchArgs},
    topology::Topology,
};
use std::collections::BTreeMap;
use std::fs;

#[tokio::test]
async fn no_index_search_uses_rg_and_returns_a_match() {
    let root = tempfile::tempdir().unwrap();
    fs::write(root.path().join("canary.txt"), "NO_INDEX_CANARY_TOKEN\n").unwrap();
    let service = MeshService::new(
        LocalBackend::from_config(
            "A",
            root.path(),
            Default::default(),
            BTreeMap::new(),
            vec![],
            None,
        ),
        Topology::new("A", vec![]),
    );

    let result = service
        .call_search({
            let mut args: SearchArgs = serde_json::from_value(serde_json::json!({"query":"fixture"})).unwrap();
            args.rank = true;
            args.query = "NO_INDEX_CANARY_TOKEN".into();
            args.verbose = false;
            args.hosts = Some(HostsInput::One("local".into()));
            args.request_id = Some("no-index-rg-canary".into());
            args.origin_host = None;
            args.hop_count = None;
            args.limit = Some(10);
            args.context_lines = Some(0);
            args.mode = SearchMode::Literal;
            args.path_globs = vec![];
            args.roots = vec![];
            args
        })
        .await
        .unwrap();

    assert!(!result.partial);
    assert_eq!(result.data["results"].as_array().unwrap().len(), 1);
    assert_eq!(result.data["host_status"][0]["state"], "ok");
}

#[tokio::test]
async fn no_index_rg_spawn_failure_is_a_concrete_partial_mcp_result() {
    let temp = tempfile::tempdir().unwrap();
    let unavailable_root = temp.path().join("missing-allowed-root");
    let service = MeshService::new(
        LocalBackend::from_config(
            "A",
            &unavailable_root,
            Default::default(),
            BTreeMap::new(),
            vec![],
            None,
        ),
        Topology::new("A", vec![]),
    );

    let result = service
        .call_search({
            let mut args: SearchArgs = serde_json::from_value(serde_json::json!({"query":"fixture"})).unwrap();
            args.rank = true;
            args.query = "NO_INDEX_FALLBACK_TOKEN".into();
            args.verbose = false;
            args.hosts = Some(HostsInput::One("local".into()));
            args.request_id = Some("no-index-rg-spawn-failure".into());
            args.origin_host = None;
            args.hop_count = None;
            args.limit = Some(10);
            args.context_lines = Some(0);
            args.mode = SearchMode::Literal;
            args.path_globs = vec![];
            args.roots = vec![];
            args
        })
        .await
        .unwrap();

    assert!(result.partial);
    assert!(result.data["results"].as_array().unwrap().is_empty());
    let status = result
        .host_status
        .iter()
        .find(|status| status.host_id == "A")
        .unwrap();
    assert!(!status.ok);
    assert_eq!(result.data["host_status"][0]["state"], "failed");
    let error = status.error.as_deref().unwrap();
    assert!(error.contains("start rg search in"), "{error}");
    assert_ne!(error, "run rg search");
}
