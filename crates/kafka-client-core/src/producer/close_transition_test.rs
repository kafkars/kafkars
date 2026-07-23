//! Producer-machine scenarios for atomic close-and-drain transitions.

use crate::{
    AdmissionRejection, AdmissionSequence, BatchExecutionGeneration, BatchExecutionId, BatchId,
    ByteCount, Deadline, ExplicitRecord, FlushId, FlushLedgerError, Moment, OperationId,
    PartitionIndex, PayloadId, ProducerEffect, ProducerInput, ProducerMachine,
    ProducerMachineError, TopicId,
};

const BYTES: ByteCount = ByteCount::new(11);

#[test]
fn close_atomically_captures_drain_barrier_before_rejecting_new_records() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    let (operation_id, batch_id) = admit(&mut producer, 1);

    let close = producer
        .apply(ProducerInput::CloseRequested)
        .unwrap_or_else(|error| panic!("close request failed: {error}"));

    assert_eq!(
        close.effects(),
        [ProducerEffect::AcceptFlush {
            flush_id: FlushId::from_raw(1),
            barrier: AdmissionSequence::from_raw(2),
        }]
    );
    assert!(!producer.admission_is_open());
    assert_eq!(
        producer.apply(admission(2)),
        Err(ProducerMachineError::Admission(AdmissionRejection::Closed))
    );

    accumulate(&mut producer, operation_id, batch_id);
    let terminal = producer
        .apply(ProducerInput::BatchMaterializationFailed {
            execution: execution(batch_id),
        })
        .unwrap_or_else(|error| panic!("materialization failure failed: {error}"));
    assert_eq!(
        terminal.effects().last(),
        Some(&ProducerEffect::CompleteFlush {
            flush_id: FlushId::from_raw(1),
        })
    );
}

#[test]
fn empty_close_completes_its_barrier_and_repeated_close_remains_bounded() {
    let mut producer = ProducerMachine::with_batch_policy_and_flush_capacity(
        ByteCount::new(64),
        1,
        crate::ProducerBatchPolicy::single_record(),
        2,
    );

    for expected in [FlushId::from_raw(1), FlushId::from_raw(2)] {
        let close = producer
            .apply(ProducerInput::CloseRequested)
            .unwrap_or_else(|error| panic!("close request failed: {error}"));
        assert!(matches!(
            close.effects(),
            [
                ProducerEffect::AcceptFlush {
                    flush_id: accepted,
                    barrier,
                },
                ProducerEffect::CompleteFlush {
                    flush_id: completed,
                },
            ] if *accepted == expected
                && *completed == expected
                && *barrier == AdmissionSequence::from_raw(1)
        ));
        assert!(!producer.admission_is_open());
    }

    assert_eq!(
        producer.apply(ProducerInput::CloseRequested),
        Err(ProducerMachineError::Flush(FlushLedgerError::Capacity))
    );
}

#[test]
fn close_capacity_failure_leaves_record_admission_open() {
    let mut producer = ProducerMachine::with_batch_policy_and_flush_capacity(
        ByteCount::new(64),
        1,
        crate::ProducerBatchPolicy::single_record(),
        0,
    );

    assert_eq!(
        producer.apply(ProducerInput::CloseRequested),
        Err(ProducerMachineError::Flush(FlushLedgerError::Capacity))
    );
    assert!(producer.admission_is_open());
    assert_eq!(producer.flush_slots(), 0);
    assert!(producer.apply(admission(1)).is_ok());
}

#[test]
fn close_identity_failure_leaves_record_admission_open() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    producer.flushes.exhaust_identity();

    assert_eq!(
        producer.apply(ProducerInput::CloseRequested),
        Err(ProducerMachineError::Flush(
            FlushLedgerError::IdentityExhausted
        ))
    );
    assert!(producer.admission_is_open());
    assert_eq!(producer.flush_slots(), 0);
    assert!(producer.apply(admission(1)).is_ok());
}

fn admit(producer: &mut ProducerMachine, payload: u64) -> (OperationId, BatchId) {
    let transition = producer
        .apply(admission(payload))
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

const fn admission(payload: u64) -> ProducerInput {
    ProducerInput::AdmitExplicit {
        now: Moment::from_tick(0),
        deadline: Deadline::from_tick(100),
        record: ExplicitRecord::new(
            PayloadId::from_raw(payload),
            TopicId::from_raw(7),
            PartitionIndex::from_raw(0),
            BYTES,
        ),
    }
}

const fn execution(batch_id: BatchId) -> BatchExecutionId {
    BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial())
}
