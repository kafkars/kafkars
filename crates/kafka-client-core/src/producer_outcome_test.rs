//! Stage-validation and queued-deadline race scenarios.

use crate::{
    BatchId, ByteCount, Deadline, DeliveryStatus, ExplicitRecord, Moment, OperationId,
    PartitionIndex, PayloadId, ProducerBatchPolicy, ProducerBatchSuccess, ProducerEffect,
    ProducerInput, ProducerMachine, ProducerMachineError, TopicId, TransitionError,
};

fn ready_batch() -> (ProducerMachine, OperationId, BatchId) {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let admitted = producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(10),
            record: ExplicitRecord::new(
                PayloadId::from_raw(1),
                TopicId::from_raw(1),
                PartitionIndex::from_raw(0),
                ByteCount::new(32),
            ),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    let Some(crate::ProducerEffect::AccumulateExplicit {
        operation_id,
        batch_id,
        ..
    }) = admitted.effects().first()
    else {
        panic!("missing accumulation effect")
    };
    let operation_id = *operation_id;
    let batch_id = *batch_id;
    producer
        .apply(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id,
            accumulator_bytes: ByteCount::new(32),
            now: Moment::from_tick(0),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    (producer, operation_id, batch_id)
}

#[test]
fn materialization_and_driver_rejections_are_stage_specific() {
    let (mut producer, _, batch_id) = ready_batch();
    assert_eq!(
        producer.apply(ProducerInput::DriverRejected { batch_id }),
        Err(ProducerMachineError::Transition(
            TransitionError::InvalidState
        ))
    );
    assert!(
        producer
            .apply(ProducerInput::BatchMaterializationFailed { batch_id })
            .is_ok()
    );

    let (mut producer, _, batch_id) = ready_batch();
    producer
        .apply(ProducerInput::BatchMaterialized {
            batch_id,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    assert_eq!(
        producer.apply(ProducerInput::BatchMaterializationFailed { batch_id }),
        Err(ProducerMachineError::Transition(
            TransitionError::InvalidState
        ))
    );
    assert!(
        producer
            .apply(ProducerInput::DriverRejected { batch_id })
            .is_ok()
    );
}

#[test]
fn broker_outcome_requires_driver_ownership() {
    let (mut producer, _, batch_id) = ready_batch();
    producer
        .apply(ProducerInput::BatchMaterialized {
            batch_id,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    assert_eq!(
        producer.apply(ProducerInput::BrokerFailed {
            batch_id,
            broker_code: 6,
            delivery: DeliveryStatus::PossiblySent,
        }),
        Err(ProducerMachineError::Transition(
            TransitionError::InvalidState
        ))
    );
    producer
        .apply(ProducerInput::DriverAccepted { batch_id })
        .unwrap_or_else(|error| panic!("driver acceptance failed: {error}"));
    assert!(
        producer
            .apply(ProducerInput::BrokerFailed {
                batch_id,
                broker_code: 6,
                delivery: DeliveryStatus::PossiblySent,
            })
            .is_ok()
    );
}

#[test]
fn transport_failure_is_distinct_from_broker_error() {
    let (mut producer, _, batch_id) = ready_batch();
    producer
        .apply(ProducerInput::BatchMaterialized {
            batch_id,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    producer
        .apply(ProducerInput::DriverAccepted { batch_id })
        .unwrap_or_else(|error| panic!("driver acceptance failed: {error}"));
    let terminal = producer
        .apply(ProducerInput::TransportFailed {
            batch_id,
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("transport failure failed: {error}"));
    assert!(matches!(
        terminal.effects().last(),
        Some(crate::ProducerEffect::Complete {
            completion: crate::ProducerCompletion::Failed(failure),
            ..
        }) if failure.kind() == crate::ProducerFailureKind::Transport
            && failure.broker_code().is_none()
    ));
}

#[test]
fn queued_deadline_fact_is_harmless_after_driver_or_terminal_ownership() {
    let (mut producer, operation_id, batch_id) = ready_batch();
    producer
        .apply(ProducerInput::BatchMaterialized {
            batch_id,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    producer
        .apply(ProducerInput::DriverAccepted { batch_id })
        .unwrap_or_else(|error| panic!("driver acceptance failed: {error}"));
    let queued = producer
        .apply(ProducerInput::DeadlineElapsed {
            operation_id,
            now: Moment::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("queued deadline failed: {error}"));
    assert!(queued.effects().is_empty());
    producer
        .apply(ProducerInput::BrokerSucceeded {
            batch_id,
            success: ProducerBatchSuccess::new(1, None, None),
        })
        .unwrap_or_else(|error| panic!("broker success failed: {error}"));
    assert!(
        producer
            .apply(ProducerInput::DeadlineElapsed {
                operation_id,
                now: Moment::from_tick(11),
            })
            .is_ok_and(|transition| transition.effects().is_empty())
    );
    producer
        .apply(ProducerInput::CompletionReclaimed { operation_id })
        .unwrap_or_else(|error| panic!("reclaim failed: {error}"));
    assert!(
        producer
            .apply(ProducerInput::DeadlineElapsed {
                operation_id,
                now: Moment::from_tick(12),
            })
            .is_ok_and(|transition| transition.effects().is_empty())
    );
}

#[test]
fn older_batch_failure_preserves_the_newer_open_route() {
    let policy = ProducerBatchPolicy::try_new(2, ByteCount::new(20), 100)
        .unwrap_or_else(|error| panic!("valid policy: {error}"));
    let mut producer = ProducerMachine::with_batch_policy(ByteCount::new(128), 4, policy);
    let admit = |producer: &mut ProducerMachine, payload| {
        producer
            .apply(ProducerInput::AdmitExplicit {
                now: Moment::from_tick(0),
                deadline: Deadline::from_tick(100),
                record: ExplicitRecord::new(
                    PayloadId::from_raw(payload),
                    TopicId::from_raw(1),
                    PartitionIndex::from_raw(0),
                    ByteCount::new(32),
                ),
            })
            .unwrap_or_else(|error| panic!("admission failed: {error}"))
    };
    let first = admit(&mut producer, 1);
    let Some(ProducerEffect::AccumulateExplicit {
        operation_id: first_id,
        batch_id: older_batch,
        ..
    }) = first.effects().first()
    else {
        panic!("missing first accumulation")
    };
    producer
        .apply(ProducerInput::RecordAccumulated {
            operation_id: *first_id,
            batch_id: *older_batch,
            accumulator_bytes: ByteCount::new(20),
            now: Moment::from_tick(0),
        })
        .unwrap_or_else(|error| panic!("first accumulation failed: {error}"));

    let second = admit(&mut producer, 2);
    let Some(ProducerEffect::AccumulateExplicit {
        batch_id: newer_batch,
        ..
    }) = second.effects().first()
    else {
        panic!("missing second accumulation")
    };
    assert_ne!(newer_batch, older_batch);
    producer
        .apply(ProducerInput::BatchMaterializationFailed {
            batch_id: *older_batch,
        })
        .unwrap_or_else(|error| panic!("older batch failure failed: {error}"));

    let third = admit(&mut producer, 3);
    assert!(matches!(
        third.effects().first(),
        Some(ProducerEffect::AccumulateExplicit { batch_id, .. }) if batch_id == newer_batch
    ));
}
