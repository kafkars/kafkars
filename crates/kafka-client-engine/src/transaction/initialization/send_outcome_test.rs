//! Exact transactional send success translation and correlation scenarios.

use std::sync::Arc;

use kafka_client_core::{
    ProducerBatchSuccess, TransactionLifecycleInput, TransactionLifecycleMachine,
    TransactionSendId, TransactionalOwnerId,
};

use super::{TransactionSendOutcome, send_outcome::translate_send_terminal};
use crate::transaction::send::TransactionSendTerminal;

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
        },
        epoch,
        send_id,
        Arc::from("orders"),
        Some(3),
    )
    .unwrap_or_else(|| panic!("exact terminal correlation"));
    let TransactionSendOutcome::Succeeded(metadata) = outcome else {
        panic!("success terminal must remain successful")
    };

    assert_eq!(metadata.topic(), "orders");
    assert_eq!(metadata.partition(), 3);
    assert_eq!(metadata.offset(), 41);
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
            },
            epoch,
            TransactionSendId::from_raw(10),
            Arc::from("orders"),
            Some(3),
        )
        .is_none()
    );
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
