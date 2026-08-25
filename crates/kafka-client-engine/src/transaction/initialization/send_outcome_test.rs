//! Exact transactional send success translation and correlation scenarios.

use std::sync::Arc;

use kafka_client_core::{
    DeliveryStatus, ProducerBatchSuccess, TransactionLifecycleInput, TransactionLifecycleMachine,
    TransactionSendId, TransactionalOwnerId,
};

use super::{
    TransactionSendConsequence, TransactionSendDeliveryStatus, TransactionSendFailureKind,
    TransactionSendOutcome, send_outcome::translate_send_terminal,
};
use crate::transaction::send::{
    InternalTransactionPartitioningFailure, InternalTransactionSendFailure,
    InternalTransactionSendFailureKind, TransactionSendTerminal,
};

#[test]
fn success_translation_retains_route_and_all_broker_metadata() {
    let epoch = epoch();
    let send_id = TransactionSendId::from_raw(9);
    let outcome = translate_send_terminal(
        TransactionSendTerminal::Succeeded {
            epoch,
            send_id,
            partition: kafka_client_core::PartitionIndex::from_raw(3),
            success: ProducerBatchSuccess::new(41, Some(55), Some(7)),
            last_offset: 41,
        },
        epoch,
        send_id,
        Arc::from("orders"),
        Some([9; 16]),
        Some(3),
    )
    .unwrap_or_else(|| panic!("exact terminal correlation"));
    let TransactionSendOutcome::Succeeded(metadata) = outcome else {
        panic!("success terminal must remain successful")
    };

    assert_eq!(metadata.topic(), "orders");
    assert_eq!(metadata.topic_uuid(), Some([9; 16]));
    assert_eq!(metadata.partition(), 3);
    assert_eq!(metadata.offset(), 41);
    assert_eq!(metadata.last_offset(), 41);
    assert_eq!(metadata.timestamp(), Some(55));
    assert_eq!(metadata.leader_epoch(), Some(7));
}

#[test]
fn success_translation_rejects_the_wrong_send_identity() {
    let epoch = epoch();
    assert!(
        translate_send_terminal(
            TransactionSendTerminal::Succeeded {
                epoch,
                send_id: TransactionSendId::from_raw(9),
                partition: kafka_client_core::PartitionIndex::from_raw(3),
                success: ProducerBatchSuccess::new(41, None, None),
                last_offset: 41,
            },
            epoch,
            TransactionSendId::from_raw(10),
            Arc::from("orders"),
            None,
            Some(3),
        )
        .is_none()
    );
}

#[test]
fn topic_identity_mismatch_remains_not_sent_abort_required_and_nonfenced() {
    let epoch = epoch();
    let send_id = TransactionSendId::from_raw(9);
    let outcome = translate_send_terminal(
        TransactionSendTerminal::AbortRequired {
            epoch,
            send_id,
            failure: InternalTransactionSendFailure::new(
                InternalTransactionSendFailureKind::Partitioning(
                    InternalTransactionPartitioningFailure::TopicIdentityMismatch,
                ),
                DeliveryStatus::NotSent,
            ),
        },
        epoch,
        send_id,
        Arc::from("orders"),
        None,
        None,
    )
    .unwrap_or_else(|| panic!("exact identity failure correlation"));
    let TransactionSendOutcome::Failed(failure) = outcome else {
        panic!("identity mismatch must remain failed")
    };

    assert_eq!(failure.kind(), TransactionSendFailureKind::Identity);
    assert_eq!(failure.delivery(), TransactionSendDeliveryStatus::NotSent);
    assert_eq!(
        failure.consequence(),
        TransactionSendConsequence::AbortRequired
    );
    assert_eq!(failure.broker_code(), None);
}

fn epoch() -> kafka_client_core::TransactionEpoch {
    let owner_id = TransactionalOwnerId::from_raw(7);
    let mut lifecycle = TransactionLifecycleMachine::new(owner_id);
    lifecycle
        .apply(owner_id, TransactionLifecycleInput::Begin)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    lifecycle
        .active_epoch()
        .unwrap_or_else(|| panic!("active epoch"))
}
