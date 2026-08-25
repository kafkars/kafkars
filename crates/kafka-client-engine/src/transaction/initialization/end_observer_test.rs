//! Public engine translation preserves exact transaction-end failure facts.

use core::num::NonZeroI16;

use kafka_client_core::{
    DeliveryStatus, TransactionEndBrokerFailureKind, TransactionEndFailure as CoreFailure,
    TransactionEndFailureKind as CoreFailureKind, TransactionEndMode, TransactionLifecycleTerminal,
};

use super::{
    TransactionEndDeliveryStatus, TransactionEndFailureKind, TransactionEndIntent,
    TransactionEndOutcome, end_observer::translate_terminal,
};

#[test]
fn exact_broker_failure_crosses_core_to_public_engine_without_fencing_invention() {
    let failure = CoreFailure::broker(
        TransactionEndMode::Abort,
        TransactionEndBrokerFailureKind::Rejected,
        DeliveryStatus::PossiblySent,
        NonZeroI16::new(-731).unwrap_or_else(|| panic!("nonzero code")),
    );
    let TransactionEndOutcome::Failed(failure) = translate_terminal(
        TransactionLifecycleTerminal::Failed(failure),
        TransactionEndIntent::Abort,
    ) else {
        panic!("failed core terminal remains failed")
    };

    assert_eq!(failure.kind(), TransactionEndFailureKind::Broker);
    assert_eq!(failure.intent(), TransactionEndIntent::Abort);
    assert_eq!(
        failure.delivery(),
        TransactionEndDeliveryStatus::PossiblySent
    );
    assert_eq!(failure.broker_code(), Some(-731));
}

#[test]
fn lifecycle_fatal_and_transport_failure_remain_distinct_nonfenced_causes() {
    let TransactionEndOutcome::Failed(lifecycle) = translate_terminal(
        TransactionLifecycleTerminal::Fatal,
        TransactionEndIntent::Commit,
    ) else {
        panic!("lifecycle fatal remains failed")
    };
    assert_eq!(lifecycle.kind(), TransactionEndFailureKind::Lifecycle);
    assert_eq!(lifecycle.delivery(), TransactionEndDeliveryStatus::NotSent);

    let transport = CoreFailure::local(
        TransactionEndMode::Commit,
        CoreFailureKind::Transport,
        DeliveryStatus::PossiblySent,
    );
    let TransactionEndOutcome::Failed(transport) = translate_terminal(
        TransactionLifecycleTerminal::Failed(transport),
        TransactionEndIntent::Commit,
    ) else {
        panic!("transport terminal remains failed")
    };
    assert_eq!(transport.kind(), TransactionEndFailureKind::Transport);
    assert_eq!(transport.broker_code(), None);
}
