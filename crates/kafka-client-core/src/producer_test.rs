//! Tests for atomic producer admission and settlement.

use crate::{
    AdmissionRejection, BatchId, ByteCount, Deadline, DeliveryStatus, Moment, OperationId,
    ProducerMachine, ProducerMachineError, TransitionError,
};

#[test]
fn admission_reserves_bytes_and_completion_together() {
    let mut producer = ProducerMachine::new(ByteCount::new(1_024), 2);

    let admission = producer.try_admit(
        Moment::from_tick(10),
        Deadline::from_tick(100),
        ByteCount::new(300),
        "record-a",
    );

    let Ok(admitted) = admission else {
        panic!("admission should succeed");
    };
    assert_eq!(admitted.id(), OperationId::from_raw(1));
    assert_eq!(admitted.deadline(), Deadline::from_tick(100));
    assert_eq!(admitted.bytes(), ByteCount::new(300));
    assert_eq!(producer.retained_bytes(), ByteCount::new(300));
    assert_eq!(producer.completion_slots(), 1);
    assert_eq!(admitted.into_parts().1, "record-a");
}

#[test]
fn byte_rejection_returns_original_value_without_reserving_completion() {
    let mut producer = ProducerMachine::new(ByteCount::new(100), 2);

    let rejection = producer.try_admit(
        Moment::from_tick(1),
        Deadline::from_tick(10),
        ByteCount::new(101),
        String::from("owned-record"),
    );

    let Err(error) = rejection else {
        panic!("oversized record should be rejected");
    };
    let (reason, record) = error.into_parts();
    assert_eq!(reason, AdmissionRejection::ByteCapacity);
    assert_eq!(record, "owned-record");
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
    assert_eq!(producer.completion_slots(), 0);
}

#[test]
fn completion_rejection_rolls_back_the_byte_reservation() {
    let mut producer = ProducerMachine::new(ByteCount::new(100), 1);
    let first = producer.try_admit(
        Moment::from_tick(1),
        Deadline::from_tick(10),
        ByteCount::new(40),
        "first",
    );
    let Ok(first) = first else {
        panic!("first record should be admitted");
    };
    assert_eq!(
        producer.settle_failed(first.id(), DeliveryStatus::NotSent,),
        Ok(())
    );
    assert_eq!(
        producer.settle_failed(first.id(), DeliveryStatus::NotSent,),
        Err(ProducerMachineError::Transition(
            TransitionError::AlreadyCompleted
        ))
    );
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));

    let second = producer.try_admit(
        Moment::from_tick(2),
        Deadline::from_tick(20),
        ByteCount::new(60),
        "second",
    );
    let Err(second) = second else {
        panic!("retained completion should backpressure admission");
    };
    assert_eq!(second.reason(), AdmissionRejection::CompletionCapacity);
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
    assert_eq!(producer.completion_slots(), 1);

    assert_eq!(producer.reclaim_completion(first.id()), Ok(()));
    assert!(
        producer
            .try_admit(
                Moment::from_tick(3),
                Deadline::from_tick(30),
                ByteCount::new(60),
                "second",
            )
            .is_ok()
    );
}

#[test]
fn submitted_operation_cannot_be_expired_by_client_timing() {
    let mut producer = ProducerMachine::new(ByteCount::new(100), 1);
    let admission = producer.try_admit(
        Moment::from_tick(1),
        Deadline::from_tick(10),
        ByteCount::new(20),
        "record",
    );
    let Ok(admitted) = admission else {
        panic!("record should be admitted");
    };

    assert_eq!(
        producer.mark_ready(admitted.id(), BatchId::from_raw(1)),
        Ok(())
    );
    assert_eq!(
        producer.mark_submitted(admitted.id(), BatchId::from_raw(1)),
        Ok(())
    );
    assert_eq!(
        producer.expire_before_submission(admitted.id()),
        Err(ProducerMachineError::Transition(
            TransitionError::InvalidState
        ))
    );
    assert_eq!(producer.retained_bytes(), ByteCount::new(20));
    assert_eq!(producer.completion_slots(), 1);
}

#[test]
fn expired_record_never_crosses_the_admission_boundary() {
    let mut producer = ProducerMachine::new(ByteCount::new(100), 1);

    let rejection = producer.try_admit(
        Moment::from_tick(10),
        Deadline::from_tick(10),
        ByteCount::new(20),
        "expired",
    );

    let Err(error) = rejection else {
        panic!("expired record should be rejected");
    };
    assert_eq!(error.reason(), AdmissionRejection::DeadlineElapsed);
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
    assert_eq!(producer.completion_slots(), 0);
}

#[test]
fn driver_submission_does_not_invent_possibly_sent() {
    let mut producer = ProducerMachine::new(ByteCount::new(100), 1);
    let admission = producer.try_admit(
        Moment::from_tick(1),
        Deadline::from_tick(10),
        ByteCount::new(20),
        "record",
    );
    let Ok(admitted) = admission else {
        panic!("record should be admitted");
    };

    assert_eq!(
        producer.mark_ready(admitted.id(), BatchId::from_raw(2)),
        Ok(())
    );
    assert_eq!(
        producer.mark_submitted(admitted.id(), BatchId::from_raw(2)),
        Ok(())
    );
    assert_eq!(
        producer.settle_failed(admitted.id(), DeliveryStatus::NotSent,),
        Ok(())
    );
    assert_eq!(producer.reclaim_completion(admitted.id()), Ok(()));
}

#[test]
fn possibly_sent_is_rejected_before_driver_ownership() {
    let mut producer = ProducerMachine::new(ByteCount::new(100), 1);
    let admission = producer.try_admit(
        Moment::from_tick(1),
        Deadline::from_tick(10),
        ByteCount::new(20),
        "record",
    );
    let Ok(admitted) = admission else {
        panic!("record should be admitted");
    };

    assert_eq!(
        producer.settle_failed(admitted.id(), DeliveryStatus::PossiblySent,),
        Err(ProducerMachineError::Transition(
            TransitionError::InvalidState
        ))
    );
    assert_eq!(producer.retained_bytes(), ByteCount::new(20));
    assert_eq!(producer.completion_slots(), 1);
}

#[test]
fn close_rejects_new_work_but_preserves_accepted_settlement() {
    let mut producer = ProducerMachine::new(ByteCount::new(100), 2);
    let admission = producer.try_admit(
        Moment::from_tick(1),
        Deadline::from_tick(10),
        ByteCount::new(20),
        "accepted",
    );
    let Ok(admitted) = admission else {
        panic!("record should be admitted");
    };

    producer.close_admission();
    assert!(!producer.admission_is_open());
    let rejected = producer.try_admit(
        Moment::from_tick(2),
        Deadline::from_tick(20),
        ByteCount::new(20),
        "new",
    );
    let Err(rejected) = rejected else {
        panic!("closed producer should reject new work");
    };
    assert_eq!(rejected.reason(), AdmissionRejection::Closed);
    assert_eq!(producer.expire_before_submission(admitted.id()), Ok(()));
    assert_eq!(producer.reclaim_completion(admitted.id()), Ok(()));
}
