//! Execution ownership between deterministic client policy and `kafka-driver`.

#![forbid(unsafe_code)]

mod config;
mod engine;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "wire materialization precedes the blocked driver submission seam"
    )
)]
mod protocol;

pub use config::EngineConfig;
pub use engine::Engine;
