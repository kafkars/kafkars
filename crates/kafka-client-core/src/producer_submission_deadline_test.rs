//! Submission deadline ownership across ties and expired open-batch members.

use crate::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, BatchTimerGeneration, ByteCount, Deadline,
    ExplicitRecord, Moment, OperationId, PartitionIndex, PayloadId, ProducerBatchPolicy,
    ProducerEffect, ProducerInput, ProducerMachine, TopicId,
};

fn execution(batch_id: BatchId) -> BatchExecutionId {
    BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial())
}

fn record(payload: u64) -> ExplicitRecord {
    ExplicitRecord::new(
        PayloadId::from_raw(payload),
        TopicId::from_raw(1),
        PartitionIndex::from_raw(0),
        ByteCount::new(16),
    )
}

fn producer(max_records: usize, linger: u64) -> ProducerMachine {
    let policy = ProducerBatchPolicy::try_new(max_records, ByteCount::new(1_024), linger)
        .unwrap_or_else(|error| panic!("valid policy: {error}"));
    let mut producer =
        ProducerMachine::with_batch_policy(ByteCount::new(1_024), max_records, policy);
    producer.install_identity_for_test();
    producer
}

fn admit(producer: &mut ProducerMachine, payload: u64, deadline: u64) -> (OperationId, BatchId) {
    let transition = producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(deadline),
            record: record(payload),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    let Some(ProducerEffect::AccumulateExplicit {
        operation_id,
        batch_id,
        ..
    }) = transition.effects().first()
    else {
        panic!("missing accumulation effect")
    };
    (*operation_id, *batch_id)
}

fn accumulate(producer: &mut ProducerMachine, operation_id: OperationId, batch_id: BatchId) {
    producer
        .apply(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id,
            accumulator_bytes: ByteCount::new(16),
            now: Moment::from_tick(0),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
}

fn submission(producer: &mut ProducerMachine, batch_id: BatchId, now: u64) -> ProducerEffect {
    let transition = producer
        .apply(ProducerInput::BatchMaterialized {
            execution: execution(batch_id),
            now: Moment::from_tick(now),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    transition
        .effects()
        .first()
        .copied()
        .unwrap_or_else(|| panic!("missing submission effect"))
}

#[test]
fn earliest_deadline_names_its_member_even_when_admitted_later() {
    let mut producer = producer(3, 1_000);
    let (first, batch_id) = admit(&mut producer, 1, 100);
    accumulate(&mut producer, first, batch_id);
    let (earliest, same_batch) = admit(&mut producer, 2, 20);
    assert_eq!(same_batch, batch_id);
    accumulate(&mut producer, earliest, batch_id);
    let (third, same_batch) = admit(&mut producer, 3, 50);
    assert_eq!(same_batch, batch_id);
    accumulate(&mut producer, third, batch_id);

    assert!(matches!(
        submission(&mut producer, batch_id, 1),
        ProducerEffect::SubmitProduce {
            deadline_operation_id,
            deadline,
            ..
        } if deadline_operation_id == earliest && deadline == Deadline::from_tick(20)
    ));
}

#[test]
fn equal_deadlines_choose_the_first_member_in_admission_order() {
    let mut producer = producer(2, 1_000);
    let (first, batch_id) = admit(&mut producer, 1, 30);
    accumulate(&mut producer, first, batch_id);
    let (second, same_batch) = admit(&mut producer, 2, 30);
    assert_eq!(same_batch, batch_id);
    accumulate(&mut producer, second, batch_id);

    assert!(matches!(
        submission(&mut producer, batch_id, 1),
        ProducerEffect::SubmitProduce {
            deadline_operation_id,
            deadline,
            ..
        } if deadline_operation_id == first && deadline == Deadline::from_tick(30)
    ));
}

#[test]
fn expiry_before_seal_transfers_deadline_ownership_to_a_live_member() {
    let mut producer = producer(3, 20);
    let (expired, batch_id) = admit(&mut producer, 1, 10);
    accumulate(&mut producer, expired, batch_id);
    let (live, same_batch) = admit(&mut producer, 2, 50);
    assert_eq!(same_batch, batch_id);
    accumulate(&mut producer, live, batch_id);

    let expiry = producer
        .apply(ProducerInput::BatchTimerFired {
            batch_id,
            generation: BatchTimerGeneration::from_raw(1),
            now: Moment::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("expiry failed: {error}"));
    assert!(expiry.effects().iter().any(|effect| matches!(
        effect,
        ProducerEffect::RemoveBatchMember {
            operation_id,
            ..
        } if *operation_id == expired
    )));

    producer
        .apply(ProducerInput::BatchTimerFired {
            batch_id,
            generation: BatchTimerGeneration::from_raw(2),
            now: Moment::from_tick(20),
        })
        .unwrap_or_else(|error| panic!("linger failed: {error}"));
    assert!(matches!(
        submission(&mut producer, batch_id, 20),
        ProducerEffect::SubmitProduce {
            deadline_operation_id,
            deadline,
            ..
        } if deadline_operation_id == live && deadline == Deadline::from_tick(50)
    ));
}
