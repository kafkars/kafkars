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
mod flush_error;
mod flush_observer;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "driver handoff does not yet expose every producer mechanism"
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
pub use flush_error::ProducerFlushError;
pub use flush_observer::{ProducerFlushObserver, ProducerFlushResult};
pub use producer::{
    ProducerAcceptedFault, ProducerAcceptedFaultKind, ProducerCancelAccepted, ProducerCancelError,
    ProducerCancelErrorKind, ProducerCancelFault, ProducerCancelFaultKind,
    ProducerCancellationOutcome, ProducerHandle, ProducerSendCapture, ProducerSendCaptureError,
    ProducerSendCaptureErrorKind, ProducerSendOptions, ProducerTryCloseAccepted,
    ProducerTryCloseError, ProducerTryCloseErrorKind, ProducerTryFlushAccepted,
    ProducerTryFlushError, ProducerTryFlushErrorKind, ProducerTrySendAccepted,
    ProducerTrySendError, ProducerTrySendErrorKind, PublicProducerHeader as ProducerHeader,
    PublicProducerRecord as ProducerRecord,
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
#[cfg(test)]
mod flush_error_test;
#[cfg(test)]
mod flush_observer_test;
