//! Virtual execution-history scenarios across definitely-unsent retry.

use kafka_client_core::{
    AcknowledgementPolicy, BatchExecutionGeneration, BatchExecutionId, BatchId, ByteCount,
    CompressionPolicy, Deadline, ExplicitRecord, OperationId, PartitionIndex, PayloadId,
    ProducerEffect, TopicId,
};

use crate::{SimulationError, state::VirtualProducerState};

#[test]
fn retry_retains_attempt_history_and_materializes_only_the_replacement() {
    let mut state = prepared_state();
    let first = execution(1);
    let second = execution(2);
    state
        .interpret(materialize(first))
        .unwrap_or_else(|error| panic!("initial materialization failed: {error}"));
    submit_and_accept(&mut state, first);

    state
        .interpret(ProducerEffect::RetryBatchExecution {
            previous: first,
            replacement: second,
        })
        .unwrap_or_else(|error| panic!("retry replacement failed: {error}"));
    assert_eq!(
        state.interpret(materialize(first)),
        Err(SimulationError::BatchExecutionMismatch {
            expected: Some(second),
            actual: first,
        })
    );

    state
        .interpret(materialize(second))
        .unwrap_or_else(|error| panic!("replacement materialization failed: {error}"));
    submit_and_accept(&mut state, second);

    assert_eq!(state.submission_count(), 2);
    assert_eq!(
        state.submitted_members(first),
        Some([OperationId::from_raw(1)].as_slice())
    );
    assert_eq!(
        state.submitted_members(second),
        Some([OperationId::from_raw(1)].as_slice())
    );
}

#[test]
fn retry_rejects_generation_skips_without_mutating_the_current_attempt() {
    let mut state = prepared_state();
    let first = execution(1);
    state
        .interpret(materialize(first))
        .unwrap_or_else(|error| panic!("initial materialization failed: {error}"));
    submit_and_accept(&mut state, first);
    let skipped = execution(3);

    assert_eq!(
        state.interpret(ProducerEffect::RetryBatchExecution {
            previous: first,
            replacement: skipped,
        }),
        Err(SimulationError::BatchExecutionMismatch {
            expected: Some(execution(2)),
            actual: skipped,
        })
    );
    assert_eq!(state.submission_count(), 1);
}

fn prepared_state() -> VirtualProducerState {
    let mut state = VirtualProducerState::default();
    let payload_id = PayloadId::from_raw(1);
    state
        .retain_payload(payload_id, ByteCount::new(8))
        .unwrap_or_else(|error| panic!("payload retention failed: {error}"));
    state
        .interpret(ProducerEffect::AccumulateExplicit {
            operation_id: OperationId::from_raw(1),
            batch_id: BatchId::from_raw(1),
            deadline: Deadline::from_tick(20),
            record: ExplicitRecord::new(
                payload_id,
                TopicId::from_raw(2),
                PartitionIndex::from_raw(3),
                ByteCount::new(8),
            ),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    state
}

fn submit_and_accept(state: &mut VirtualProducerState, execution: BatchExecutionId) {
    state
        .interpret(ProducerEffect::SubmitProduce {
            execution,
            deadline_operation_id: OperationId::from_raw(1),
            deadline: Deadline::from_tick(20),
            topic_id: TopicId::from_raw(2),
            partition: PartitionIndex::from_raw(3),
            acknowledgements: AcknowledgementPolicy::All,
        })
        .unwrap_or_else(|error| panic!("submission failed: {error}"));
    state
        .driver_accepted(execution)
        .unwrap_or_else(|error| panic!("driver acceptance failed: {error}"));
}

fn materialize(execution: BatchExecutionId) -> ProducerEffect {
    ProducerEffect::MaterializeBatch {
        execution,
        compression: CompressionPolicy::Uncompressed,
    }
}

fn execution(generation: u64) -> BatchExecutionId {
    let generation = BatchExecutionGeneration::try_from_raw(generation)
        .unwrap_or_else(|| panic!("test generation is nonzero"));
    BatchExecutionId::new(BatchId::from_raw(1), generation)
}
