//! Execution ownership between deterministic client policy and `kafka-driver`.

#![forbid(unsafe_code)]

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "integrated host does not yet expose every owner metric"
    )
)]
mod clock;
mod completion;
mod config;
mod delivery;
mod delivery_error;
mod delivery_observer;
mod driver;
mod engine;
mod engine_host;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "first host slice does not yet drive queued admission"
    )
)]
mod producer;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "real driver Produce admission remains blocked")
)]
mod protocol;

pub use config::{EngineConfig, EngineProducerLimits};
pub use delivery::{
    ProducerDeliveryFailure, ProducerDeliveryFailureKind, ProducerDeliveryStatus,
    ProducerRecordMetadata,
};
pub use delivery_error::{ProducerDeliveryError, ProducerObserverError};
pub use delivery_observer::{ProducerDeliveryObserver, ProducerDeliveryResult};
pub use engine::Engine;
pub use engine_host::{
    EngineShutdownError, EngineShutdownErrorKind, EngineStartError, EngineStartErrorKind,
};
pub use producer::{
    ProducerAcceptedFault, ProducerAcceptedFaultKind, ProducerHandle, ProducerOperationId,
    ProducerSendOptions, ProducerTrySendAccepted, ProducerTrySendError, ProducerTrySendErrorKind,
    PublicProducerHeader as ProducerHeader, PublicProducerRecord as ProducerRecord,
};

#[cfg(test)]
mod config_test;
#[cfg(test)]
mod delivery_error_test;
#[cfg(test)]
mod delivery_observer_test;
#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod engine_test;
