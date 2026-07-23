//! Producer flush ingress ownership classification scenarios.

use kafka_client_core::Moment;

use super::flush_outcome::{ProducerPortFlushError, classify_flush};
use crate::producer::{
    flush::{FlushAdmissionFailure, FlushRejectionReason},
    host_limits_test::{start, valid_limits},
};

#[test]
fn healthy_flush_admission_retains_its_observer() {
    let mut host = start(valid_limits());
    let accepted = classify_flush(host.try_admit_flush(Moment::from_tick(0)))
        .unwrap_or_else(|error| panic!("flush classification failed: {error:?}"));
    let (observer, flush_id, fault) = accepted.into_parts();

    assert!(flush_id.is_some());
    assert!(fault.is_ok());
    assert_eq!(observer.wait(), Ok(()));
}

#[test]
fn pre_acceptance_rejection_stays_observer_free() {
    assert!(matches!(
        classify_flush(Err(FlushAdmissionFailure::Rejected(
            FlushRejectionReason::Closed
        ))),
        Err(ProducerPortFlushError::Rejected(
            FlushRejectionReason::Closed
        ))
    ));
}
