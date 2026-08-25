//! Homogeneous transactional batch metadata and one-terminal scenarios.

use std::sync::Arc;

use kafka_client_core::{
    ProducerBatchSuccess, TransactionLifecycleInput, TransactionLifecycleMachine,
    TransactionSendId, TransactionalOwnerId,
};

use super::{
    TransactionBatchSendOutcome, send_batch_outcome::batch_outcome,
    send_outcome::translate_send_terminal,
};
use crate::transaction::send::TransactionSendTerminal;

#[test]
fn one_batch_terminal_retains_base_metadata_and_exact_record_count() {
    let owner_id = TransactionalOwnerId::from_raw(7);
    let mut lifecycle = TransactionLifecycleMachine::new(owner_id);
    lifecycle
        .apply(owner_id, TransactionLifecycleInput::Begin)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    let epoch = lifecycle
        .active_epoch()
        .unwrap_or_else(|| panic!("active epoch"));
    let send_id = TransactionSendId::from_raw(9);
    let outcome = translate_send_terminal(
        TransactionSendTerminal::Succeeded {
            epoch,
            send_id,
            partition: kafka_client_core::PartitionIndex::from_raw(3),
            success: ProducerBatchSuccess::new(41, Some(55), Some(7)),
            last_offset: 43,
        },
        epoch,
        send_id,
        Arc::from("orders"),
        Some([9; 16]),
        Some(3),
    )
    .unwrap_or_else(|| panic!("exact terminal correlation"));

    let TransactionBatchSendOutcome::Succeeded(metadata) = batch_outcome(outcome, 3) else {
        panic!("batch success remains one successful terminal")
    };
    assert_eq!(metadata.topic(), "orders");
    assert_eq!(metadata.topic_uuid(), Some([9; 16]));
    assert_eq!(metadata.partition(), 3);
    assert_eq!(metadata.base_offset(), 41);
    assert_eq!(metadata.last_offset(), 43);
    assert_eq!(metadata.record_count(), 3);
    assert_eq!(metadata.timestamp(), Some(55));
    assert_eq!(metadata.leader_epoch(), Some(7));
}
