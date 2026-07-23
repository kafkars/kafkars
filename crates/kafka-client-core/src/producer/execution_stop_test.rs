//! Scenarios for bounded settlement after permanent execution loss.

use crate::{
    AdmissionRejection, BatchExecutionGeneration, BatchExecutionId, BatchId, ByteCount, Deadline,
    DeliveryStatus, ExplicitRecord, Moment, OperationId, PartitionIndex, PayloadId,
    ProducerBatchPolicy, ProducerCompletion, ProducerEffect, ProducerFailureKind, ProducerInput,
    ProducerMachine, ProducerMachineError, ProducerOperationState, TopicId,
};

const TOPIC: TopicId = TopicId::from_raw(7);
const BYTES: ByteCount = ByteCount::new(11);

fn execution(batch_id: BatchId) -> BatchExecutionId {
    BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial())
}

fn record(payload: u64, partition: u32) -> ExplicitRecord {
    ExplicitRecord::new(
        PayloadId::from_raw(payload),
        TOPIC,
        PartitionIndex::from_raw(partition),
        BYTES,
    )
}

pub(super) fn producer(capacity: usize) -> ProducerMachine {
    let policy = ProducerBatchPolicy::try_new(2, ByteCount::new(1_024), 50)
        .unwrap_or_else(|error| panic!("valid test policy: {error}"));
    ProducerMachine::with_batch_policy(ByteCount::new(256), capacity, policy)
}

pub(super) fn admit(
    producer: &mut ProducerMachine,
    payload: u64,
    partition: u32,
) -> (OperationId, BatchId) {
    let transition = producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(100),
            record: record(payload, partition),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    let Some(ProducerEffect::AccumulateExplicit {
        operation_id,
        batch_id,
        ..
    }) = transition.effects().first()
    else {
        panic!("admission did not request accumulation")
    };
    (*operation_id, *batch_id)
}

fn accumulate(producer: &mut ProducerMachine, operation_id: OperationId, batch_id: BatchId) {
    producer
        .apply(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id,
            accumulator_bytes: BYTES,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
}

fn ready_pair(
    producer: &mut ProducerMachine,
    first_payload: u64,
    partition: u32,
) -> ([OperationId; 2], BatchId) {
    let (first, batch_id) = admit(producer, first_payload, partition);
    accumulate(producer, first, batch_id);
    let (second, second_batch) = admit(producer, first_payload + 1, partition);
    assert_eq!(second_batch, batch_id);
    accumulate(producer, second, batch_id);
    ([first, second], batch_id)
}

fn assert_resource_release_prefix(
    effects: &[ProducerEffect],
    open_batch: BatchId,
    materializing_batch: BatchId,
    awaiting_batch: BatchId,
    submitted_batch: BatchId,
) {
    assert_eq!(
        effects[..5],
        [
            ProducerEffect::CancelBatchTimer {
                batch_id: open_batch,
                generation: crate::BatchTimerGeneration::from_raw(1),
            },
            ProducerEffect::ReleaseBatch {
                batch_id: open_batch,
            },
            ProducerEffect::ReleaseBatch {
                batch_id: materializing_batch,
            },
            ProducerEffect::ReleaseBatch {
                batch_id: awaiting_batch,
            },
            ProducerEffect::ReleaseBatch {
                batch_id: submitted_batch,
            },
        ]
    );
}

#[test]
fn execution_loss_settles_every_batch_stage_in_release_before_complete_order() {
    let mut producer = producer(7);
    let (open, open_batch) = admit(&mut producer, 1, 0);
    let (materializing, materializing_batch) = ready_pair(&mut producer, 2, 1);
    let (awaiting_driver, awaiting_batch) = ready_pair(&mut producer, 4, 2);
    producer
        .apply(ProducerInput::BatchMaterialized {
            execution: execution(awaiting_batch),
            now: Moment::from_tick(2),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    let (submitted, submitted_batch) = ready_pair(&mut producer, 6, 3);
    producer
        .apply(ProducerInput::BatchMaterialized {
            execution: execution(submitted_batch),
            now: Moment::from_tick(2),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    producer
        .apply(ProducerInput::DriverAccepted {
            execution: execution(submitted_batch),
        })
        .unwrap_or_else(|error| panic!("driver acceptance failed: {error}"));

    let terminal = producer
        .apply(ProducerInput::ExecutionUnavailable)
        .unwrap_or_else(|error| panic!("execution settlement failed: {error}"));
    let all_operations = [
        open,
        materializing[0],
        materializing[1],
        awaiting_driver[0],
        awaiting_driver[1],
        submitted[0],
        submitted[1],
    ];
    assert_resource_release_prefix(
        terminal.effects(),
        open_batch,
        materializing_batch,
        awaiting_batch,
        submitted_batch,
    );
    assert_eq!(terminal.effects().len(), 19);
    for (effect, operation_id) in terminal.effects()[5..12].iter().zip(all_operations) {
        assert!(matches!(
            effect,
            ProducerEffect::ReleasePayload {
                payload_id,
                retained_bytes: BYTES,
            } if payload_id.get() == operation_id.get()
        ));
    }
    for (index, (effect, operation_id)) in terminal.effects()[12..]
        .iter()
        .zip(all_operations)
        .enumerate()
    {
        let expected_delivery = if index < 5 {
            DeliveryStatus::NotSent
        } else {
            DeliveryStatus::PossiblySent
        };
        assert!(matches!(
            effect,
            ProducerEffect::Complete {
                operation_id: actual,
                completion: ProducerCompletion::Failed(failure),
            } if *actual == operation_id
                && failure.kind() == ProducerFailureKind::ExecutionUnavailable
                && failure.delivery() == expected_delivery
        ));
        assert_eq!(
            producer
                .operation(operation_id)
                .map(crate::ProducerOperation::state),
            Some(ProducerOperationState::Completed)
        );
    }
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
    assert_eq!(producer.completion_slots(), 7);
    assert!(!producer.admission_is_open());
    assert!(
        producer
            .apply(ProducerInput::ExecutionUnavailable)
            .is_ok_and(|transition| transition.effects().is_empty())
    );

    for operation_id in all_operations {
        producer
            .apply(ProducerInput::CompletionReclaimed { operation_id })
            .unwrap_or_else(|error| panic!("completion reclaim failed: {error}"));
    }
    assert_eq!(producer.completion_slots(), 0);
    assert_eq!(
        producer.apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(3),
            deadline: Deadline::from_tick(100),
            record: record(8, 4),
        }),
        Err(ProducerMachineError::Admission(AdmissionRejection::Closed))
    );
}

#[test]
fn empty_execution_loss_is_idempotent_and_closes_admission() {
    let mut producer = producer(2);
    for _ in 0..2 {
        assert!(
            producer
                .apply(ProducerInput::ExecutionUnavailable)
                .is_ok_and(|transition| transition.effects().is_empty())
        );
    }
    assert!(!producer.admission_is_open());
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
    assert_eq!(producer.completion_slots(), 0);
}

#[test]
fn execution_loss_does_not_repeat_an_unreclaimed_terminal() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    let (completed, completed_batch) = admit(&mut producer, 1, 0);
    accumulate(&mut producer, completed, completed_batch);
    producer
        .apply(ProducerInput::BatchMaterializationFailed {
            execution: execution(completed_batch),
        })
        .unwrap_or_else(|error| panic!("terminal setup failed: {error}"));
    let (active, active_batch) = admit(&mut producer, 2, 1);

    let terminal = producer
        .apply(ProducerInput::ExecutionUnavailable)
        .unwrap_or_else(|error| panic!("execution settlement failed: {error}"));
    assert_eq!(terminal.effects().len(), 4);
    assert!(terminal.effects().iter().all(|effect| !matches!(
        effect,
        ProducerEffect::Complete {
            operation_id,
            ..
        } if *operation_id == completed
    )));
    assert!(matches!(
        terminal.effects(),
        [
            ProducerEffect::CancelBatchTimer { batch_id, .. },
            ProducerEffect::ReleaseBatch {
                batch_id: released_batch,
            },
            ProducerEffect::ReleasePayload { payload_id, .. },
            ProducerEffect::Complete {
                operation_id,
                completion: ProducerCompletion::Failed(failure),
            },
        ] if *batch_id == active_batch
            && *released_batch == active_batch
            && payload_id.get() == 2
            && *operation_id == active
            && failure.kind() == ProducerFailureKind::ExecutionUnavailable
    ));
    assert_eq!(producer.completion_slots(), 2);
    for operation_id in [completed, active] {
        producer
            .apply(ProducerInput::CompletionReclaimed { operation_id })
            .unwrap_or_else(|error| panic!("completion reclaim failed: {error}"));
    }
    assert_eq!(producer.completion_slots(), 0);
}
