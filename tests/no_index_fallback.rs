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
        .call_search(SearchArgs {
            query: "NO_INDEX_CANARY_TOKEN".into(),
            verbose: false,
            hosts: Some(HostsInput::One("local".into())),
            request_id: Some("no-index-rg-canary".into()),
            origin_host: None,
            hop_count: None,
            limit: Some(10),
            context_lines: Some(0),
            mode: SearchMode::Literal,
            path_globs: vec![],
            roots: vec![],
        })
        .await
        .unwrap();

    assert!(!result.partial);
    assert_eq!(result.data["results"].as_array().unwrap().len(), 1);
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
        .call_search(SearchArgs {
            query: "NO_INDEX_FALLBACK_TOKEN".into(),
            verbose: false,
            hosts: Some(HostsInput::One("local".into())),
            request_id: Some("no-index-rg-spawn-failure".into()),
            origin_host: None,
            hop_count: None,
            limit: Some(10),
            context_lines: Some(0),
            mode: SearchMode::Literal,
            path_globs: vec![],
            roots: vec![],
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
    let error = status.error.as_deref().unwrap();
    assert!(error.contains("start rg search in"), "{error}");
    assert_ne!(error, "run rg search");
}
