//! Producer-machine scenarios for bounded admission-sequence flush barriers.

use crate::{
    AdmissionRejection, AdmissionSequence, BatchExecutionGeneration, BatchExecutionId, BatchId,
    ByteCount, Deadline, ExplicitRecord, FlushId, FlushLedgerError, Moment, OperationId,
    PartitionIndex, PayloadId, ProducerEffect, ProducerInput, ProducerMachine,
    ProducerMachineError, TopicId,
};

const TOPIC: TopicId = TopicId::from_raw(7);
const BYTES: ByteCount = ByteCount::new(11);

fn execution(batch_id: BatchId) -> BatchExecutionId {
    BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial())
}

#[test]
fn flush_includes_exactly_operations_accepted_before_its_barrier() {
    let mut producer = ProducerMachine::new(ByteCount::new(128), 4);
    let (first, first_batch) = admit(&mut producer, 1, 0);
    let (flush_id, barrier) = request_flush(&mut producer);
    assert_eq!(flush_id, FlushId::from_raw(1));
    assert_eq!(barrier.get(), 2);

    let (_later, _later_batch) = admit(&mut producer, 2, 1);
    let terminal = fail_materialization(&mut producer, first, first_batch);

    assert_eq!(
        terminal.effects().last(),
        Some(&ProducerEffect::CompleteFlush { flush_id })
    );
    assert_eq!(producer.flush_slots(), 1);
    assert_eq!(producer.completion_slots(), 2);
}

#[test]
fn overlapping_flushes_settle_in_barrier_order() {
    let mut producer = ProducerMachine::new(ByteCount::new(128), 4);
    let (first, first_batch) = admit(&mut producer, 1, 0);
    let (first_flush, first_barrier) = request_flush(&mut producer);
    let (second, second_batch) = admit(&mut producer, 2, 1);
    let (second_flush, second_barrier) = request_flush(&mut producer);
    assert!(first_barrier < second_barrier);

    let later_terminal = fail_materialization(&mut producer, second, second_batch);
    assert!(
        !later_terminal
            .effects()
            .iter()
            .any(|effect| matches!(effect, ProducerEffect::CompleteFlush { .. }))
    );
    let earlier_terminal = fail_materialization(&mut producer, first, first_batch);

    assert_eq!(
        &earlier_terminal.effects()[earlier_terminal.effects().len() - 2..],
        [
            ProducerEffect::CompleteFlush {
                flush_id: first_flush,
            },
            ProducerEffect::CompleteFlush {
                flush_id: second_flush,
            },
        ]
    );
}

#[test]
fn terminal_decision_orders_flush_after_record_completion_effect() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    let (operation_id, batch_id) = admit(&mut producer, 1, 0);
    let (flush_id, _barrier) = request_flush(&mut producer);

    let terminal = fail_materialization(&mut producer, operation_id, batch_id);

    assert_eq!(
        terminal.effects(),
        [
            ProducerEffect::ReleaseBatch { batch_id },
            ProducerEffect::ReleasePayload {
                payload_id: PayloadId::from_raw(1),
                retained_bytes: BYTES,
            },
            ProducerEffect::Complete {
                operation_id,
                completion: crate::ProducerCompletion::Failed(
                    crate::ProducerFailure::materialization_failed(),
                ),
            },
            ProducerEffect::CompleteFlush { flush_id },
        ]
    );
}

#[test]
fn flush_capacity_and_identity_are_bounded_until_reclamation() {
    let mut producer = ProducerMachine::with_batch_policy_and_flush_capacity(
        ByteCount::new(64),
        1,
        crate::ProducerBatchPolicy::single_record(),
        1,
    );
    let (first, barrier) = request_flush(&mut producer);
    assert_eq!(barrier.get(), 1);
    assert_eq!(
        producer.apply(ProducerInput::FlushRequested),
        Err(ProducerMachineError::Flush(FlushLedgerError::Capacity))
    );
    producer
        .apply(ProducerInput::FlushCompletionReclaimed { flush_id: first })
        .unwrap_or_else(|error| panic!("completed flush reclaim failed: {error}"));
    let (second, _barrier) = request_flush(&mut producer);
    assert_eq!(second, FlushId::from_raw(2));

    producer
        .apply(ProducerInput::FlushCompletionReclaimed { flush_id: second })
        .unwrap_or_else(|error| panic!("second flush reclaim failed: {error}"));
    producer.flushes.exhaust_identity();
    assert_eq!(
        producer.apply(ProducerInput::FlushRequested),
        Err(ProducerMachineError::Flush(
            FlushLedgerError::IdentityExhausted
        ))
    );
}

#[test]
fn pending_flush_cannot_be_reclaimed() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let (_operation_id, _batch_id) = admit(&mut producer, 1, 0);
    let (flush_id, _barrier) = request_flush(&mut producer);

    assert_eq!(
        producer.apply(ProducerInput::FlushCompletionReclaimed { flush_id }),
        Err(ProducerMachineError::Flush(FlushLedgerError::NotCompleted))
    );
}

#[test]
fn close_and_execution_loss_settle_flush_deterministically() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    let (_operation_id, _batch_id) = admit(&mut producer, 1, 0);
    let (flush_id, _barrier) = request_flush(&mut producer);
    producer.close_admission();
    assert_eq!(
        producer.apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(100),
            record: record(2, 1),
        }),
        Err(ProducerMachineError::Admission(AdmissionRejection::Closed))
    );

    let terminal = producer
        .apply(ProducerInput::ExecutionUnavailable)
        .unwrap_or_else(|error| panic!("execution loss failed: {error}"));

    assert_eq!(
        terminal.effects().last(),
        Some(&ProducerEffect::CompleteFlush { flush_id })
    );
    let (closed_flush, _barrier) = request_flush(&mut producer);
    assert_eq!(closed_flush, FlushId::from_raw(2));
}

fn request_flush(producer: &mut ProducerMachine) -> (FlushId, AdmissionSequence) {
    let transition = producer
        .apply(ProducerInput::FlushRequested)
        .unwrap_or_else(|error| panic!("flush request failed: {error}"));
    let Some(ProducerEffect::AcceptFlush { flush_id, barrier }) = transition.effects().first()
    else {
        panic!("flush request did not reserve its completion")
    };
    (*flush_id, *barrier)
}

fn admit(producer: &mut ProducerMachine, payload: u64, partition: u32) -> (OperationId, BatchId) {
    let transition = producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(100),
            record: record(payload, partition),
        })
        .unwrap_or_else(|error| panic!("record admission failed: {error}"));
    let Some(ProducerEffect::AccumulateExplicit {
        operation_id,
        batch_id,
        ..
    }) = transition.effects().first()
    else {
        panic!("record admission did not request accumulation")
    };
    (*operation_id, *batch_id)
}

fn fail_materialization(
    producer: &mut ProducerMachine,
    operation_id: OperationId,
    batch_id: BatchId,
) -> crate::ProducerTransition {
    producer
        .apply(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id,
            accumulator_bytes: BYTES,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    producer
        .apply(ProducerInput::BatchMaterializationFailed {
            execution: execution(batch_id),
        })
        .unwrap_or_else(|error| panic!("materialization failure failed: {error}"))
}

const fn record(payload: u64, partition: u32) -> ExplicitRecord {
    ExplicitRecord::new(
        PayloadId::from_raw(payload),
        TOPIC,
        PartitionIndex::from_raw(partition),
        BYTES,
    )
}
