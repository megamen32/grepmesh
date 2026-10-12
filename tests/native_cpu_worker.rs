//! Focused integration: real registered Mac M1 MCP and native CPU OCR.
//! Expected 15s, maximum 90s after cached setup. No mocked process/network.
use grepmesh::{compute_client::ComputeClient, config::OcrConfig, topology::PeerConfig};
use std::{env, path::Path};

#[test]
#[ignore = "requires the operator's real protected Mac M1 mesh route and image"]
fn native_mac_cpu_worker_extracts_real_image() {
    let host = env::var("GREPMESH_NATIVE_HOST").expect("real worker host");
    let url = env::var("GREPMESH_NATIVE_URL").expect("real registered worker route");
    let input = env::var("GREPMESH_NATIVE_INPUT").expect("real PNG");
    let token_env = env::var("GREPMESH_NATIVE_TOKEN_ENV").expect("protected token variable name");
    let origin = env::var("GREPMESH_NATIVE_ORIGIN").expect("existing trusted relay origin");
    let mut client = ComputeClient::new(OcrConfig {
        backend: "mesh".into(),
        mesh_peers: vec![PeerConfig {
            host_id: host,
            local_url: url.clone(),
            routable_url: url.clone(),
            gptadmin_proxy_url: None,
            gptadmin_relay_url: Some(url),
        }],
        mesh_topology_cache_path: None,
        mesh_relay_origin: Some(origin),
        mesh_relay_token_env: Some(token_env),
        remote_timeout_ms: 90_000,
        ..OcrConfig::default()
    });
    let text = client.extract(Path::new(&input), "png").expect("actual native mesh OCR");
    let lowered = text.to_lowercase();
    assert!(lowered.contains("mesh") && lowered.contains("cpu") && lowered.contains("october"), "actual image markers missing");
    println!("real registered Mac CPU OCR returned {} characters", text.chars().count());
}
