pub mod backend;
pub mod backup_catalog;
pub mod compute;
mod compute_ratings;
pub mod compute_client;
mod compute_load;
pub mod config;
pub mod console;
pub mod gptadmin;
pub mod index;
pub mod jobs;
pub mod mcp;
#[cfg(feature = "ocr")]
pub mod ocr;
#[cfg(feature = "ocr")]
mod ocr_png;
#[cfg(not(feature = "ocr"))]
#[path = "ocr_disabled.rs"]
pub mod ocr;
pub mod server;
pub mod stt;
pub mod topology;
pub mod topology_cache;
pub mod userio;
