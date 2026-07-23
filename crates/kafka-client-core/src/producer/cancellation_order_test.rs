//! Cancellation orders included barriers before replacement execution.

use crate::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, ByteCount, Deadline, ExplicitRecord,
    FlushId, Moment, OperationId, PartitionIndex, PayloadId, ProducerBatchPolicy, ProducerEffect,
    ProducerInput, ProducerMachine, ProducerMachineError, TopicId,
};

const RETAINED: ByteCount = ByteCount::new(8);

#[test]
fn included_flush_completes_before_excluded_survivor_rematerializes() {
    let mut producer = producer(2, 2);
    let (included, batch_id) = admit(&mut producer, 1);
    producer
        .apply(ProducerInput::FlushRequested)
        .unwrap_or_else(|error| panic!("flush failed: {error}"));
    let (survivor, same_batch) = admit(&mut producer, 2);
    assert_eq!(same_batch, batch_id);
    accumulate(&mut producer, included, batch_id);
    accumulate(&mut producer, survivor, batch_id);

    let transition = cancel(&mut producer, included);

    assert!(matches!(
        transition.effects(),
        [
            ProducerEffect::ReviseBatchExecution {
                previous,
                replacement: Some(replacement),
                removed_operation_id,
            },
            ProducerEffect::ReleasePayload { payload_id, .. },
            ProducerEffect::Complete {
                operation_id: completed,
                ..
            },
            ProducerEffect::CompleteFlush { flush_id },
            ProducerEffect::MaterializeBatch {
                execution: materialized,
                ..
            },
        ] if previous.batch_id() == batch_id
            && previous.generation() == generation(1)
            && replacement.batch_id() == batch_id
            && replacement.generation() == generation(2)
            && *payload_id == PayloadId::from_raw(1)
            && *removed_operation_id == included
            && *completed == included
            && *flush_id == FlushId::from_raw(1)
            && *materialized == *replacement
    ));
    let capacity = producer
        .transition_effect_capacity()
        .unwrap_or_else(|| panic!("test transition capacity must be representable"));
    assert!(transition.effects().len() <= capacity);
}

#[test]
fn successive_cancellations_advance_exact_generations_and_fence_old_facts() {
    let mut producer = producer(3, 3);
    let (survivor, batch_id) = admit(&mut producer, 1);
    let (second, _) = admit(&mut producer, 2);
    let (third, _) = admit(&mut producer, 3);
    for operation_id in [survivor, second, third] {
        accumulate(&mut producer, operation_id, batch_id);
    }
    let first = execution(batch_id, 1);
    let second_generation = execution(batch_id, 2);
    let third_generation = execution(batch_id, 3);

    assert_revision(&cancel(&mut producer, third), first, second_generation);
    assert_revision(
        &cancel(&mut producer, second),
        second_generation,
        third_generation,
    );

    for stale in [first, second_generation] {
        for input in [
            ProducerInput::BatchMaterialized {
                execution: stale,
                now: Moment::from_tick(2),
            },
            ProducerInput::BatchMaterializationFailed { execution: stale },
            ProducerInput::DriverRejected { execution: stale },
        ] {
            assert!(
                producer
                    .apply(input)
                    .is_ok_and(|transition| transition.effects().is_empty())
            );
        }
        assert_eq!(
            producer.apply(ProducerInput::DriverAccepted { execution: stale }),
            Err(ProducerMachineError::StaleDriverAcceptance {
                reported: stale,
                current: Some(third_generation),
            })
        );
    }
}

fn assert_revision(
    transition: &crate::ProducerTransition,
    previous: BatchExecutionId,
    replacement: BatchExecutionId,
) {
    assert!(matches!(
        transition.effects().first(),
        Some(ProducerEffect::ReviseBatchExecution {
            previous: actual,
            replacement: Some(next),
            ..
        }) if *actual == previous && *next == replacement
    ));
}

fn producer(max_records: usize, completion_capacity: usize) -> ProducerMachine {
    let policy = ProducerBatchPolicy::try_new(max_records, ByteCount::new(1_024), 20)
        .unwrap_or_else(|error| panic!("policy invalid: {error}"));
    ProducerMachine::with_batch_policy(ByteCount::new(64), completion_capacity, policy)
}

fn admit(producer: &mut ProducerMachine, payload: u64) -> (OperationId, BatchId) {
    let transition = producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(100),
            record: ExplicitRecord::new(
                PayloadId::from_raw(payload),
                TopicId::from_raw(7),
                PartitionIndex::from_raw(0),
                RETAINED,
            ),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    match transition.effects().first() {
        Some(ProducerEffect::AccumulateExplicit {
            operation_id,
            batch_id,
            ..
        }) => (*operation_id, *batch_id),
        effect => panic!("unexpected admission effect: {effect:?}"),
    }
}

fn accumulate(producer: &mut ProducerMachine, operation_id: OperationId, batch_id: BatchId) {
    producer
        .apply(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id,
            accumulator_bytes: RETAINED,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
}

fn cancel(producer: &mut ProducerMachine, operation_id: OperationId) -> crate::ProducerTransition {
    producer
        .apply(ProducerInput::CancelRequested { operation_id })
        .unwrap_or_else(|error| panic!("cancellation failed: {error}"))
}

fn execution(batch_id: BatchId, value: u64) -> BatchExecutionId {
    BatchExecutionId::new(batch_id, generation(value))
}

fn generation(value: u64) -> BatchExecutionGeneration {
    BatchExecutionGeneration::try_from_raw(value)
        .unwrap_or_else(|| panic!("generation must be nonzero"))
}
