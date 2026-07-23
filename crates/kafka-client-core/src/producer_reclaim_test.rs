//! Scenarios for split core and engine terminal-completion ownership.

use crate::{
    BatchId, ByteCount, CompletionLedgerError, Deadline, ExplicitRecord, Moment, OperationId,
    PartitionIndex, PayloadId, ProducerInput, ProducerMachine, ProducerMachineError, TopicId,
};

#[test]
fn completion_capacity_cannot_be_reclaimed_before_terminal_decision() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let operation_id = OperationId::from_raw(1);
    let batch_id = BatchId::from_raw(1);
    producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(10),
            record: ExplicitRecord::new(
                PayloadId::from_raw(40),
                TopicId::from_raw(4),
                PartitionIndex::from_raw(0),
                ByteCount::new(32),
            ),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));

    assert_eq!(
        producer.apply(ProducerInput::CompletionReclaimed { operation_id }),
        Err(ProducerMachineError::Completion(
            CompletionLedgerError::NotReady
        ))
    );
    producer
        .apply(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id,
            accumulator_bytes: ByteCount::new(32),
            now: Moment::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("deadline settlement failed: {error}"));
    let reclaimed = producer
        .apply(ProducerInput::CompletionReclaimed { operation_id })
        .unwrap_or_else(|error| panic!("reclaim failed: {error}"));
    assert!(reclaimed.effects().is_empty());
    assert_eq!(producer.completion_slots(), 0);
}
