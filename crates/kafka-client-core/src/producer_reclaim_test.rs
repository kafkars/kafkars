//! Scenarios for split core and engine terminal-completion ownership.

use crate::{
    BatchId, ByteCount, CompletionLedgerError, Deadline, ExplicitRecord, Moment, OperationId,
    PartitionIndex, PayloadId, ProducerInput, ProducerMachine, ProducerMachineError,
    ProducerTransition, TopicId,
};

#[test]
fn engine_cannot_reclaim_completion_capacity_before_terminal_effect() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let record = ExplicitRecord::new(
        PayloadId::from_raw(40),
        TopicId::from_raw(4),
        PartitionIndex::from_raw(0),
        ByteCount::new(32),
    );
    let admitted = producer.apply(ProducerInput::AdmitExplicit {
        now: Moment::from_tick(1),
        deadline: Deadline::from_tick(10),
        record,
    });
    assert!(admitted.is_ok());
    let operation_id = OperationId::from_raw(1);

    assert_eq!(
        producer.apply(ProducerInput::CompletionReclaimed { operation_id }),
        Err(ProducerMachineError::Completion(
            CompletionLedgerError::NotReady
        ))
    );
    assert_eq!(producer.retained_bytes(), ByteCount::new(32));
    assert_eq!(producer.completion_slots(), 1);

    assert!(
        producer
            .apply(ProducerInput::BatchReady {
                operation_id,
                batch_id: BatchId::from_raw(8),
                now: Moment::from_tick(10),
            })
            .is_ok()
    );
    assert_eq!(
        producer.apply(ProducerInput::CompletionReclaimed { operation_id }),
        Ok(ProducerTransition::None)
    );
    assert_eq!(producer.completion_slots(), 0);
}
