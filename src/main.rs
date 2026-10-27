use clap::Parser;
use grepmesh::server::run_server;
use grepmesh::{config::AppConfig, index::PersistentIndex};
use std::path::PathBuf;
#[cfg(target_os = "linux")]
use std::{
    io::Write,
    process::{Command, Stdio},
};

#[cfg(target_os = "linux")]
fn verify_cache_compaction_resources() -> anyhow::Result<()> {
    let mut child = Command::new("python3")
        .args(["-", "--label", "grepmesh-cache-compaction"])
        .stdin(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("resource guard stdin unavailable"))?
        .write_all(include_str!("../deploy/paired_resource_guard.py").as_bytes())?;
    anyhow::ensure!(
        child.wait()?.success(),
        "cache compaction resource guard is closed"
    );
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn verify_cache_compaction_resources() -> anyhow::Result<()> {
    anyhow::bail!("cache-body compaction is available only on Linux hosts")
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    /// Compact one bounded batch of verified duplicate extraction-cache bodies and exit.
    #[arg(long)]
    compact_cache_bodies: bool,
    /// Maximum candidate rows in this one maintenance batch (hard-capped by the index).
    #[arg(long, default_value_t = 64)]
    compaction_batch_rows: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let args = Args::parse();
    let config = AppConfig::from_path(&args.config)?;
    if args.compact_cache_bodies {
        verify_cache_compaction_resources()?;
        let index_path = config
            .index_path
            .ok_or_else(|| anyhow::anyhow!("cache-body compaction requires index_path"))?;
        let index =
            PersistentIndex::open_for_body_compaction(index_path).map_err(anyhow::Error::msg)?;
        let batch = index
            .compact_duplicate_cache_bodies_batch(args.compaction_batch_rows)
            .map_err(anyhow::Error::msg)?;
        println!(
            "cache-body batch: scanned={} cleared={} cleared_body_bytes={} preserved_mismatches={} preserved_oversized={} complete={}",
            batch.scanned_rows,
            batch.cleared_rows,
            batch.cleared_body_bytes,
            batch.preserved_mismatches,
            batch.preserved_oversized,
            batch.complete,
        );
        return Ok(());
    }
    run_server(config).await
}
