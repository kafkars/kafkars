//! Producer-boundary metrics admission error classification.

use crate::producer::ingress::ProducerShardLockError;

use super::{EngineMetricsAdmissionError, EngineMetricsAdmissionErrorKind};

#[test]
fn producer_contention_is_metrics_backpressure_without_driver_ownership() {
    let error = EngineMetricsAdmissionError::from_producer(ProducerShardLockError::Contended);

    assert_eq!(error.kind(), EngineMetricsAdmissionErrorKind::Capacity);
    assert!(error.to_string().contains("temporarily contended"));
    assert!(std::error::Error::source(&error).is_none());
}

#[test]
fn producer_poisoning_is_host_unavailable_without_driver_ownership() {
    let error = EngineMetricsAdmissionError::from_producer(ProducerShardLockError::Poisoned);

    assert_eq!(
        error.kind(),
        EngineMetricsAdmissionErrorKind::HostUnavailable
    );
    assert!(error.to_string().contains("ownership is unavailable"));
    assert!(std::error::Error::source(&error).is_none());
}
