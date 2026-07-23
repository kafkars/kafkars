//! Scenarios for explicit-partition producer input and ordered effect transitions.

use crate::{
    AcknowledgementPolicy, AdmissionRejection, BatchId, ByteCount, CompressionPolicy, Deadline,
    DeliveryStatus, ExplicitRecord, Moment, OperationId, PartitionIndex, PayloadId,
    ProducerCompletion, ProducerEffect, ProducerInput, ProducerMachine, ProducerMachineError,
    ProducerTransition, TopicId, TransitionError,
};

fn record(payload: u64, bytes: u64) -> ExplicitRecord {
    ExplicitRecord::new(
        PayloadId::from_raw(payload),
        TopicId::from_raw(8),
        PartitionIndex::from_raw(3),
        ByteCount::new(bytes),
    )
}

fn admit(producer: &mut ProducerMachine, record: ExplicitRecord) -> OperationId {
    let transition = producer.apply(ProducerInput::AdmitExplicit {
        now: Moment::from_tick(10),
        deadline: Deadline::from_tick(100),
        record,
    });
    let Ok(transition) = transition else {
        panic!("explicit record should be admitted");
    };
    let [
        ProducerEffect::AccumulateExplicit {
            operation_id,
            deadline,
            record: admitted,
        },
    ] = transition.effects()
    else {
        panic!("admission should emit one accumulator effect");
    };
    assert_eq!(*deadline, Deadline::from_tick(100));
    assert_eq!(*admitted, record);
    *operation_id
}

#[test]
fn explicit_uncompressed_acks_all_batch_settles_in_release_order() {
    let mut producer = ProducerMachine::new(ByteCount::new(1_024), 2);
    let record = record(11, 300);
    let operation_id = admit(&mut producer, record);
    let batch_id = BatchId::from_raw(21);

    let ready = producer.apply(ProducerInput::BatchReady {
        operation_id,
        batch_id,
        now: Moment::from_tick(20),
    });
    assert_eq!(
        ready,
        Ok(ProducerTransition::One([ProducerEffect::SubmitProduce {
            operation_id,
            batch_id,
            deadline: Deadline::from_tick(100),
            topic_id: TopicId::from_raw(8),
            partition: PartitionIndex::from_raw(3),
            acknowledgements: AcknowledgementPolicy::All,
            compression: CompressionPolicy::Uncompressed,
        }]))
    );
    assert_eq!(
        producer.apply(ProducerInput::DriverAccepted {
            operation_id,
            batch_id,
        }),
        Ok(ProducerTransition::None)
    );

    let terminal = producer.apply(ProducerInput::BrokerSucceeded {
        operation_id,
        batch_id,
    });
    assert_eq!(
        terminal,
        Ok(ProducerTransition::Three([
            ProducerEffect::ReleaseBatch { batch_id },
            ProducerEffect::ReleasePayload {
                payload_id: PayloadId::from_raw(11),
                retained_bytes: ByteCount::new(300),
            },
            ProducerEffect::Complete {
                operation_id,
                completion: ProducerCompletion::Delivered,
            },
        ]))
    );
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
    assert_eq!(producer.completion_slots(), 1);
    assert_eq!(
        producer.apply(ProducerInput::CompletionReclaimed { operation_id }),
        Ok(ProducerTransition::None)
    );
    assert_eq!(producer.completion_slots(), 0);
}

#[test]
fn driver_rejection_is_not_sent_and_releases_materialized_work() {
    let mut producer = ProducerMachine::new(ByteCount::new(128), 1);
    let operation_id = admit(&mut producer, record(12, 64));
    let batch_id = BatchId::from_raw(22);
    assert!(
        producer
            .apply(ProducerInput::BatchReady {
                operation_id,
                batch_id,
                now: Moment::from_tick(20),
            })
            .is_ok()
    );

    assert_eq!(
        producer.apply(ProducerInput::DriverRejected {
            operation_id,
            batch_id,
        }),
        Ok(ProducerTransition::Three([
            ProducerEffect::ReleaseBatch { batch_id },
            ProducerEffect::ReleasePayload {
                payload_id: PayloadId::from_raw(12),
                retained_bytes: ByteCount::new(64),
            },
            ProducerEffect::Complete {
                operation_id,
                completion: ProducerCompletion::Failed(DeliveryStatus::NotSent),
            },
        ]))
    );
}

#[test]
fn pre_driver_deadline_requires_elapsed_moment_and_releases_once() {
    let mut producer = ProducerMachine::new(ByteCount::new(128), 1);
    let operation_id = admit(&mut producer, record(13, 64));

    assert_eq!(
        producer.apply(ProducerInput::DeadlineElapsed {
            operation_id,
            now: Moment::from_tick(99),
        }),
        Err(ProducerMachineError::Transition(
            TransitionError::DeadlineNotElapsed
        ))
    );
    assert_eq!(producer.retained_bytes(), ByteCount::new(64));
    assert_eq!(
        producer.apply(ProducerInput::DeadlineElapsed {
            operation_id,
            now: Moment::from_tick(100),
        }),
        Ok(ProducerTransition::Two([
            ProducerEffect::ReleasePayload {
                payload_id: PayloadId::from_raw(13),
                retained_bytes: ByteCount::new(64),
            },
            ProducerEffect::Complete {
                operation_id,
                completion: ProducerCompletion::Failed(DeliveryStatus::NotSent),
            },
        ]))
    );
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
    assert_eq!(
        producer.apply(ProducerInput::DeadlineElapsed {
            operation_id,
            now: Moment::from_tick(101),
        }),
        Err(ProducerMachineError::Transition(
            TransitionError::AlreadyCompleted
        ))
    );
}

#[test]
fn broker_failure_preserves_driver_delivery_certainty() {
    let mut producer = ProducerMachine::new(ByteCount::new(128), 1);
    let operation_id = admit(&mut producer, record(14, 64));
    let batch_id = BatchId::from_raw(24);
    assert!(
        producer
            .apply(ProducerInput::BatchReady {
                operation_id,
                batch_id,
                now: Moment::from_tick(20),
            })
            .is_ok()
    );
    assert!(
        producer
            .apply(ProducerInput::DriverAccepted {
                operation_id,
                batch_id,
            })
            .is_ok()
    );
    let terminal = producer.apply(ProducerInput::BrokerFailed {
        operation_id,
        batch_id,
        delivery: DeliveryStatus::PossiblySent,
    });
    let Ok(transition) = terminal else {
        panic!("driver failure should settle the operation");
    };
    assert_eq!(
        transition.effects().last(),
        Some(&ProducerEffect::Complete {
            operation_id,
            completion: ProducerCompletion::Failed(DeliveryStatus::PossiblySent),
        })
    );
}

#[test]
fn admission_failure_mutates_no_capacity_owner() {
    let mut producer = ProducerMachine::new(ByteCount::new(10), 1);
    let result = producer.apply(ProducerInput::AdmitExplicit {
        now: Moment::from_tick(10),
        deadline: Deadline::from_tick(100),
        record: record(15, 11),
    });

    assert_eq!(
        result,
        Err(ProducerMachineError::Admission(
            AdmissionRejection::ByteCapacity
        ))
    );
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
    assert_eq!(producer.completion_slots(), 0);
}

#[test]
fn mismatched_batch_cannot_cross_driver_ownership_boundary() {
    let mut producer = ProducerMachine::new(ByteCount::new(128), 1);
    let operation_id = admit(&mut producer, record(16, 64));
    assert!(
        producer
            .apply(ProducerInput::BatchReady {
                operation_id,
                batch_id: BatchId::from_raw(30),
                now: Moment::from_tick(20),
            })
            .is_ok()
    );
    assert_eq!(
        producer.apply(ProducerInput::DriverAccepted {
            operation_id,
            batch_id: BatchId::from_raw(31),
        }),
        Err(ProducerMachineError::Transition(
            TransitionError::BatchMismatch
        ))
    );
    assert_eq!(producer.retained_bytes(), ByteCount::new(64));
    assert_eq!(producer.completion_slots(), 1);
}

#[test]
fn batch_ready_after_deadline_never_emits_driver_submission() {
    let mut producer = ProducerMachine::new(ByteCount::new(128), 1);
    let operation_id = admit(&mut producer, record(17, 64));
    let batch_id = BatchId::from_raw(32);

    assert_eq!(
        producer.apply(ProducerInput::BatchReady {
            operation_id,
            batch_id,
            now: Moment::from_tick(100),
        }),
        Ok(ProducerTransition::Three([
            ProducerEffect::ReleaseBatch { batch_id },
            ProducerEffect::ReleasePayload {
                payload_id: PayloadId::from_raw(17),
                retained_bytes: ByteCount::new(64),
            },
            ProducerEffect::Complete {
                operation_id,
                completion: ProducerCompletion::Failed(DeliveryStatus::NotSent),
            },
        ]))
    );
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
}
