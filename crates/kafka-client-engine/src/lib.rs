//! Execution ownership between deterministic client policy and `kafka-driver`.

#![forbid(unsafe_code)]

mod config;
mod engine;

pub use config::EngineConfig;
pub use engine::Engine;
