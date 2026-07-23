//! Public producer flush admission error translation scenarios.

use kafka_client_core::FlushLedgerError;

use super::flush_error::{ProducerTryFlushError, ProducerTryFlushErrorKind};
use crate::producer::{flush::FlushRejectionReason, ingress::ProducerPortFlushError};

#[test]
fn bounded_capacity_and_closed_state_remain_distinct() {
    let capacity = ProducerTryFlushError::from_port(ProducerPortFlushError::Rejected(
        FlushRejectionReason::Core(FlushLedgerError::Capacity),
    ));
    let closed = ProducerTryFlushError::from_port(ProducerPortFlushError::Rejected(
        FlushRejectionReason::Closed,
    ));

    assert_eq!(
        capacity.kind(),
        ProducerTryFlushErrorKind::CompletionCapacity
    );
    assert_eq!(closed.kind(), ProducerTryFlushErrorKind::Closed);
}

#[test]
fn boundary_clock_failure_is_explicit() {
    let error = ProducerTryFlushError::moment_unrepresentable();
    assert_eq!(
        error.kind(),
        ProducerTryFlushErrorKind::MomentUnrepresentable
    );
}
