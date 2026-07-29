//! Exact partition-transaction abort terminal value scenarios.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::{
    AbortPartitionTransactionBrokerError, AbortPartitionTransactionFailure,
    AbortPartitionTransactionFailureKind, AbortPartitionTransactionTerminal,
};

#[test]
fn broker_error_preserves_future_signed_code_and_observed_delivery() {
    let error = AbortPartitionTransactionBrokerError::new(
        NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("nonzero")),
    );

    assert_eq!(error.code(), -31_999);
    assert_eq!(error.delivery(), DeliveryStatus::PossiblySent);
    assert_eq!(
        AbortPartitionTransactionTerminal::BrokerRejected(error),
        AbortPartitionTransactionTerminal::BrokerRejected(error)
    );
}

#[test]
fn mechanism_failure_preserves_kind_and_authoritative_delivery() {
    let failure = AbortPartitionTransactionFailure::new(
        AbortPartitionTransactionFailureKind::Transport,
        DeliveryStatus::NotSent,
    );

    assert_eq!(
        failure.kind(),
        AbortPartitionTransactionFailureKind::Transport
    );
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
}
