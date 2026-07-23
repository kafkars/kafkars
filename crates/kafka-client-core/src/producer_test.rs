//! Tests for atomic explicit-record admission and batch policy validation.

use crate::{
    AdmissionRejection, ByteCount, Deadline, ExplicitRecord, Moment, PartitionIndex, PayloadId,
    ProducerBatchPolicy, ProducerBatchPolicyError, ProducerInput, ProducerMachine,
    ProducerMachineError, TopicId,
};

fn record(payload: u64, bytes: u64) -> ExplicitRecord {
    ExplicitRecord::new(
        PayloadId::from_raw(payload),
        TopicId::from_raw(2),
        PartitionIndex::from_raw(0),
        ByteCount::new(bytes),
    )
}

#[test]
fn admission_reserves_bytes_and_completion_atomically() {
    let mut producer = ProducerMachine::with_batch_policy(
        ByteCount::new(64),
        1,
        ProducerBatchPolicy::try_new(2, ByteCount::new(128), 10)
            .unwrap_or_else(|error| panic!("valid policy: {error}")),
    );
    assert!(
        producer
            .apply(ProducerInput::AdmitExplicit {
                now: Moment::from_tick(0),
                deadline: Deadline::from_tick(10),
                record: record(1, 64),
            })
            .is_ok()
    );
    assert_eq!(producer.retained_bytes(), ByteCount::new(64));
    assert_eq!(producer.completion_slots(), 1);

    assert_eq!(
        producer.apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(10),
            record: record(2, 1),
        }),
        Err(ProducerMachineError::Admission(
            AdmissionRejection::ByteCapacity
        ))
    );
    assert_eq!(producer.completion_slots(), 1);
}

#[test]
fn elapsed_deadline_never_crosses_admission() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    assert_eq!(
        producer.apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(10),
            deadline: Deadline::from_tick(10),
            record: record(1, 1),
        }),
        Err(ProducerMachineError::Admission(
            AdmissionRejection::DeadlineElapsed
        ))
    );
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
    assert_eq!(producer.completion_slots(), 0);
}

#[test]
fn linger_deadline_overflow_rejects_without_reservation() {
    let policy = ProducerBatchPolicy::try_new(2, ByteCount::new(64), 2)
        .unwrap_or_else(|error| panic!("valid policy: {error}"));
    let mut producer = ProducerMachine::with_batch_policy(ByteCount::new(64), 1, policy);
    assert_eq!(
        producer.apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(u64::MAX - 1),
            deadline: Deadline::from_tick(u64::MAX),
            record: record(1, 1),
        }),
        Err(ProducerMachineError::Admission(
            AdmissionRejection::DeadlineOverflow
        ))
    );
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
}

#[test]
fn zero_batch_limits_are_rejected() {
    assert_eq!(
        ProducerBatchPolicy::try_new(0, ByteCount::new(1), 0),
        Err(ProducerBatchPolicyError::ZeroRecordLimit)
    );
    assert_eq!(
        ProducerBatchPolicy::try_new(1, ByteCount::new(0), 0),
        Err(ProducerBatchPolicyError::ZeroByteLimit)
    );
}
