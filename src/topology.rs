use serde::{Deserialize, Serialize};

use crate::topology_cache::{CacheFreshness, TopologySnapshot};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PeerConfig {
    pub host_id: String,
    pub local_url: String,
    pub routable_url: String,
    /// Optional loopback HTTP CONNECT endpoint backed by an approved GPTAdmin
    /// Network Tunnel capability. It is used only after a direct TCP connect
    /// failure; it never replaces the routable peer URL.
    #[serde(default)]
    pub gptadmin_proxy_url: Option<String>,
    /// Credential-free URL of an existing GPTAdmin child MCP endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gptadmin_relay_url: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_registry_keeps_static_hosts_and_direct_route_with_relay_fallback() {
        let direct = PeerConfig {
            host_id: "B".into(),
            local_url: "http://b/mcp".into(),
            routable_url: "http://b/mcp".into(),
            gptadmin_proxy_url: None,
            gptadmin_relay_url: None,
        };
        let missing = PeerConfig {
            host_id: "C".into(),
            ..direct.clone()
        };
        let relay = PeerConfig {
            host_id: "B".into(),
            local_url: "http://hub/server/b/mcp".into(),
            routable_url: "http://hub/server/b/mcp".into(),
            gptadmin_proxy_url: None,
            gptadmin_relay_url: Some("http://hub/server/b/mcp".into()),
        };
        let mut topology = Topology::new("A", vec![relay]);
        topology.retain_known_peers(&[direct.clone(), missing.clone()]);
        assert_eq!(
            topology.peer("B").unwrap().routable_url,
            direct.routable_url
        );
        assert!(topology.peer("B").unwrap().gptadmin_relay_url.is_some());
        assert!(topology.peer("C").is_some());
        let mut empty = Topology::new("A", vec![]);
        empty.retain_known_peers(&[direct, missing]);
        assert_eq!(empty.peers.len(), 2);
    }
}

#[derive(Debug, Clone)]
pub struct Topology {
    pub local_host_id: String,
    pub peers: Vec<PeerConfig>,
    pub cache_freshness: Option<CacheFreshness>,
    pub generation: u64,
    pub last_refresh_error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopologyStatus {
    pub freshness: Option<CacheFreshness>,
    pub generation: u64,
    pub last_refresh_error: Option<String>,
}

impl Topology {
    pub fn new(local_host_id: impl Into<String>, peers: Vec<PeerConfig>) -> Self {
        Self {
            local_host_id: local_host_id.into(),
            peers,
            cache_freshness: None,
            generation: 0,
            last_refresh_error: None,
        }
    }

    pub fn from_snapshot(snapshot: TopologySnapshot, now_ms: u64) -> anyhow::Result<Self> {
        snapshot.validate()?;
        let freshness = snapshot.freshness_at(now_ms);
        let peers = snapshot
            .peers
            .into_iter()
            .map(|node| PeerConfig {
                host_id: node.host_id,
                local_url: node.local_url,
                routable_url: node.routable_url,
                gptadmin_proxy_url: None,
                gptadmin_relay_url: node.gptadmin_relay_url,
            })
            .collect();
        Ok(Self {
            local_host_id: snapshot.local_host_id,
            peers,
            cache_freshness: Some(freshness),
            generation: snapshot.generation,
            last_refresh_error: snapshot.last_refresh_error,
        })
    }

    pub fn with_cache_error(mut self, error: impl Into<String>) -> Self {
        self.last_refresh_error = Some(error.into());
        self
    }

    /// Discovery can be partial. Keep known direct routes, adding the Hub as
    /// a fallback, and retain hosts absent from the latest registry response.
    pub fn retain_known_peers(&mut self, known: &[PeerConfig]) {
        for previous in known {
            if previous.host_id == self.local_host_id {
                continue;
            }
            if let Some(peer) = self
                .peers
                .iter_mut()
                .find(|p| p.host_id == previous.host_id)
            {
                if peer.gptadmin_relay_url.as_deref() == Some(peer.routable_url.as_str())
                    && previous.gptadmin_relay_url.as_deref()
                        != Some(previous.routable_url.as_str())
                {
                    peer.routable_url = previous.routable_url.clone();
                    peer.local_url = previous.local_url.clone();
                }
                if peer.gptadmin_proxy_url.is_none() {
                    peer.gptadmin_proxy_url = previous.gptadmin_proxy_url.clone();
                }
                if peer.gptadmin_relay_url.is_none() {
                    peer.gptadmin_relay_url = previous.gptadmin_relay_url.clone();
                }
            } else {
                self.peers.push(previous.clone());
            }
        }
    }

    pub fn status(&self) -> TopologyStatus {
        TopologyStatus {
            freshness: self.cache_freshness,
            generation: self.generation,
            last_refresh_error: self.last_refresh_error.clone(),
        }
    }

    pub fn peer(&self, host_id: &str) -> Option<&PeerConfig> {
        self.peers.iter().find(|peer| peer.host_id == host_id)
    }

    pub fn known_host_ids(&self) -> impl Iterator<Item = String> + '_ {
        std::iter::once(self.local_host_id.clone())
            .chain(self.peers.iter().map(|peer| peer.host_id.clone()))
    }
}
