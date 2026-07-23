//! Producer close result and error translation scenarios.

use kafka_client_core::{FlushLedgerError, Moment};

use super::{ProducerTryCloseAccepted, ProducerTryCloseError, ProducerTryCloseErrorKind};
use crate::producer::{
    flush::FlushRejectionReason,
    host_limits_test::{start, valid_limits},
    ingress::{ProducerPortFlushAccepted, ProducerPortFlushError},
};

#[test]
fn accepted_close_reuses_the_flush_barrier_observer() {
    let mut host = start(valid_limits());
    let admitted = host
        .try_admit_close(Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("close should be accepted: {error:?}"));
    let port = ProducerPortFlushAccepted::from_admitted_for_test(admitted);
    let accepted = ProducerTryCloseAccepted::from_port(port);

    assert!(accepted.fault().is_none());
    assert_eq!(accepted.into_observer().wait(), Ok(()));
}

#[test]
fn close_preserves_shared_capacity_and_identity_rejections() {
    let capacity = ProducerTryCloseError::from_port(ProducerPortFlushError::Rejected(
        FlushRejectionReason::Core(FlushLedgerError::Capacity),
    ));
    let identity = ProducerTryCloseError::from_port(ProducerPortFlushError::Rejected(
        FlushRejectionReason::Core(FlushLedgerError::IdentityExhausted),
    ));

    assert_eq!(
        capacity.kind(),
        ProducerTryCloseErrorKind::CompletionCapacity
    );
    assert_eq!(
        identity.kind(),
        ProducerTryCloseErrorKind::LocalIdentityExhausted
    );
    assert!(
        capacity
            .to_string()
            .starts_with("producer try_close failed")
    );
}

#[test]
fn close_boundary_clock_failure_is_explicit() {
    assert_eq!(
        ProducerTryCloseError::moment_unrepresentable().kind(),
        ProducerTryCloseErrorKind::MomentUnrepresentable
    );
}
