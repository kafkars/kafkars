//! Runtime-neutral observation of bounded driver operational metrics.

mod calls;
mod error;
#[cfg(test)]
mod error_test;
mod failures;
mod latency;
mod mailbox;
mod observer;
#[cfg(test)]
mod observer_test;
mod producer;
#[cfg(test)]
mod producer_test;
mod snapshot;

pub use calls::EngineCallMetrics;
pub use error::{
    EngineMetricsAdmissionError, EngineMetricsAdmissionErrorKind, EngineMetricsObserverError,
    EngineMetricsObserverErrorKind,
};
pub use failures::EngineFailureMetrics;
pub use latency::{EngineLatencyMetric, EngineLatencyMetrics};
pub use mailbox::EngineMailboxMetrics;
pub use observer::EngineMetricsObserver;
pub use producer::EngineProducerMetrics;
pub use snapshot::EngineMetricsSnapshot;
