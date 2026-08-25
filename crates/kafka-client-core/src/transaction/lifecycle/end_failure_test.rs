//! Transaction-end failure ownership and signed-code preservation tests.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::{
    TransactionEndBrokerFailureKind, TransactionEndFailure, TransactionEndFailureKind,
    TransactionEndMode,
};

#[test]
fn local_failure_preserves_intent_cause_and_delivery_without_a_broker_code() {
    let failure = TransactionEndFailure::local(
        TransactionEndMode::Commit,
        TransactionEndFailureKind::Transport,
        DeliveryStatus::PossiblySent,
    );

    assert_eq!(failure.mode(), TransactionEndMode::Commit);
    assert_eq!(failure.kind(), TransactionEndFailureKind::Transport);
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    assert_eq!(failure.broker_code(), None);
}

#[test]
fn broker_failure_preserves_abort_intent_category_and_signed_code() {
    let code = NonZeroI16::new(-731).unwrap_or_else(|| panic!("nonzero test code"));
    let failure = TransactionEndFailure::broker(
        TransactionEndMode::Abort,
        TransactionEndBrokerFailureKind::Rejected,
        DeliveryStatus::PossiblySent,
        code,
    );

    assert_eq!(failure.mode(), TransactionEndMode::Abort);
    assert_eq!(
        failure.kind(),
        TransactionEndFailureKind::Broker(TransactionEndBrokerFailureKind::Rejected)
    );
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    assert_eq!(failure.broker_code(), Some(-731));
}

#[test]
fn delivery_floor_only_weakens_certainty_and_preserves_exact_failure() {
    let code = NonZeroI16::new(-731).unwrap_or_else(|| panic!("nonzero test code"));
    let failure = TransactionEndFailure::broker(
        TransactionEndMode::Abort,
        TransactionEndBrokerFailureKind::Rejected,
        DeliveryStatus::NotSent,
        code,
    )
    .with_delivery_floor(DeliveryStatus::PossiblySent)
    .with_delivery_floor(DeliveryStatus::NotSent);

    assert_eq!(failure.mode(), TransactionEndMode::Abort);
    assert_eq!(
        failure.kind(),
        TransactionEndFailureKind::Broker(TransactionEndBrokerFailureKind::Rejected)
    );
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    assert_eq!(failure.broker_code(), Some(-731));
}
