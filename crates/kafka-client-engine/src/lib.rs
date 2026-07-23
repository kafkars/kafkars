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
mod delivery;
mod delivery_error;
mod delivery_observer;
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
        reason = "bounded producer ownership precedes integrated host interpretation"
    )
)]
mod producer;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "wire materialization precedes the blocked driver submission seam"
    )
)]
mod protocol;

pub use config::EngineConfig;
pub use delivery::{
    ProducerDeliveryFailure, ProducerDeliveryFailureKind, ProducerDeliveryStatus,
    ProducerRecordMetadata,
};
pub use delivery_error::{ProducerDeliveryError, ProducerObserverError};
pub use delivery_observer::{ProducerDeliveryObserver, ProducerDeliveryResult};
pub use engine::Engine;

#[cfg(test)]
mod delivery_error_test;
#[cfg(test)]
mod delivery_observer_test;
#[cfg(test)]
mod delivery_test;
