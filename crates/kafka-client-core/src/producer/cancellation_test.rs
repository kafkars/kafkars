//! Open and submitted cancellation outcomes preserve lifecycle ownership.

use crate::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, BatchTimerGeneration, ByteCount, Deadline,
    DeliveryStatus, ExplicitRecord, FlushId, Moment, OperationId, PartitionIndex, PayloadId,
    ProducerBatchPolicy, ProducerCancellationOutcome, ProducerCompletion, ProducerEffect,
    ProducerFailureKind, ProducerInput, ProducerMachine, ProducerOperationState, TopicId,
};

const RETAINED: ByteCount = ByteCount::new(8);

#[test]
fn open_member_cancellation_releases_exact_resources_and_remains_terminal() {
    let mut producer = ProducerMachine::with_batch_policy(ByteCount::new(64), 2, policy(10));
    assert_eq!(
        cancel(&mut producer, OperationId::from_raw(999)).cancellation_outcome(),
        Some(ProducerCancellationOutcome::AlreadyTerminal)
    );
    let (first, batch_id) = admit(&mut producer, 1);
    let (cancelled, same_batch) = admit(&mut producer, 2);
    assert_eq!(same_batch, batch_id);

    let transition = cancel(&mut producer, cancelled);

    assert_eq!(
        transition.cancellation_outcome(),
        Some(ProducerCancellationOutcome::CancelledNotSent)
    );
    assert!(matches!(
        transition.effects(),
        [
            ProducerEffect::ArmBatchTimer { .. },
            ProducerEffect::RemoveBatchMember {
                operation_id,
                ..
            },
            ProducerEffect::ReleasePayload { .. },
            ProducerEffect::Complete {
                operation_id: completed,
                completion: ProducerCompletion::Failed(failure),
            },
        ] if *operation_id == cancelled
            && *completed == cancelled
            && failure.kind() == ProducerFailureKind::Cancelled
            && failure.delivery() == DeliveryStatus::NotSent
    ));
    assert_eq!(producer.retained_bytes(), RETAINED);
    assert!(matches!(
        producer
            .operation(first)
            .map(crate::ProducerOperation::state),
        Some(ProducerOperationState::Accumulating { .. })
    ));
    assert_eq!(
        cancel(&mut producer, cancelled).cancellation_outcome(),
        Some(ProducerCancellationOutcome::AlreadyTerminal)
    );
}

#[test]
fn cancellation_seals_a_linger_elapsed_open_survivor() {
    let mut producer = ProducerMachine::with_batch_policy(ByteCount::new(64), 2, policy(10));
    let (survivor, batch_id) = admit(&mut producer, 1);
    accumulate(&mut producer, survivor, batch_id);
    let (cancelled, _) = admit(&mut producer, 2);
    producer
        .apply(ProducerInput::BatchTimerFired {
            batch_id,
            generation: BatchTimerGeneration::from_raw(1),
            now: Moment::from_tick(20),
        })
        .unwrap_or_else(|error| panic!("linger timer failed: {error}"));

    let transition = cancel(&mut producer, cancelled);

    assert!(transition.effects().iter().any(|effect| matches!(
        effect,
        ProducerEffect::MaterializeBatch { execution, .. }
            if execution.batch_id() == batch_id
    )));
    assert!(matches!(
        producer
            .operation(survivor)
            .map(crate::ProducerOperation::state),
        Some(ProducerOperationState::Materializing { .. })
    ));
}

#[test]
fn cancellation_terminal_precedes_close_barrier_completion() {
    let mut producer =
        ProducerMachine::with_batch_policy_and_flush_capacity(ByteCount::new(64), 2, policy(10), 1);
    let (first, _) = admit(&mut producer, 1);
    let (second, _) = admit(&mut producer, 2);
    producer
        .apply(ProducerInput::CloseRequested)
        .unwrap_or_else(|error| panic!("close failed: {error}"));
    cancel(&mut producer, first);

    let terminal = cancel(&mut producer, second);

    let completion = terminal
        .effects()
        .iter()
        .position(|effect| matches!(effect, ProducerEffect::Complete { .. }))
        .unwrap_or_else(|| panic!("cancellation completion missing"));
    let flush = terminal
        .effects()
        .iter()
        .position(|effect| {
            matches!(
                effect,
                ProducerEffect::CompleteFlush { flush_id }
                    if *flush_id == FlushId::from_raw(1)
            )
        })
        .unwrap_or_else(|| panic!("close barrier completion missing"));
    assert!(completion < flush);
}

#[test]
fn submitted_cancellation_is_too_late_without_mutation() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let (operation_id, batch_id) = admit(&mut producer, 1);
    accumulate(&mut producer, operation_id, batch_id);
    let execution = BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial());
    producer
        .apply(ProducerInput::BatchMaterialized {
            execution,
            now: Moment::from_tick(2),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    producer
        .apply(ProducerInput::DriverAccepted { execution })
        .unwrap_or_else(|error| panic!("driver acceptance failed: {error}"));

    let transition = cancel(&mut producer, operation_id);

    assert_eq!(
        transition.cancellation_outcome(),
        Some(ProducerCancellationOutcome::TooLate)
    );
    assert!(transition.effects().is_empty());
    assert_eq!(producer.retained_bytes(), RETAINED);
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

fn policy(max_records: usize) -> ProducerBatchPolicy {
    ProducerBatchPolicy::try_new(max_records, ByteCount::new(1_024), 20)
        .unwrap_or_else(|error| panic!("policy invalid: {error}"))
}
