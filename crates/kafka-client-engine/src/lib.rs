//! Execution ownership between deterministic client policy and `kafka-driver`.

#![forbid(unsafe_code)]

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "clock and timer ownership precede the integrated engine host"
    )
)]
mod clock;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "completion ownership precedes the integrated engine host"
    )
)]
mod completion;
mod config;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the embedded owner precedes the integrated engine host"
    )
)]
mod driver;
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
