//! Scenarios for earliest-member deadlines and stale batch timer facts.

use crate::{
    BatchId, BatchTimerGeneration, ByteCount, Deadline, ExplicitRecord, Moment, OperationId,
    PartitionIndex, PayloadId, ProducerBatchPolicy, ProducerCompletion, ProducerEffect,
    ProducerFailureKind, ProducerInput, ProducerMachine, TopicId,
};

fn record(payload: u64) -> ExplicitRecord {
    ExplicitRecord::new(
        PayloadId::from_raw(payload),
        TopicId::from_raw(1),
        PartitionIndex::from_raw(0),
        ByteCount::new(32),
    )
}

fn admit(
    producer: &mut ProducerMachine,
    payload: u64,
    deadline: u64,
) -> (OperationId, BatchId, Vec<ProducerEffect>) {
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
    (*operation_id, *batch_id, transition.effects()[1..].to_vec())
}

fn confirm(producer: &mut ProducerMachine, operation_id: OperationId, batch_id: BatchId) {
    producer
        .apply(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id,
            accumulator_bytes: ByteCount::new(20),
            now: Moment::from_tick(0),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
}

#[test]
fn earlier_deadline_rearms_timer_and_stale_facts_are_harmless() {
    let policy = ProducerBatchPolicy::try_new(10, ByteCount::new(1_024), 20)
        .unwrap_or_else(|error| panic!("valid policy: {error}"));
    let mut producer = ProducerMachine::with_batch_policy(ByteCount::new(128), 4, policy);
    let (first, batch_id, _) = admit(&mut producer, 1, 100);
    confirm(&mut producer, first, batch_id);
    let (second, same_batch, tail) = admit(&mut producer, 2, 10);
    assert_eq!(same_batch, batch_id);
    assert!(matches!(
        tail.as_slice(),
        [ProducerEffect::ArmBatchTimer {
            generation,
            deadline,
            ..
        }] if *generation == BatchTimerGeneration::from_raw(2)
            && *deadline == Deadline::from_tick(10)
    ));
    confirm(&mut producer, second, batch_id);

    let stale = producer
        .apply(ProducerInput::BatchTimerFired {
            batch_id,
            generation: BatchTimerGeneration::from_raw(1),
            now: Moment::from_tick(20),
        })
        .unwrap_or_else(|error| panic!("stale timer failed: {error}"));
    assert!(stale.effects().is_empty());

    let expired = producer
        .apply(ProducerInput::BatchTimerFired {
            batch_id,
            generation: BatchTimerGeneration::from_raw(2),
            now: Moment::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("deadline timer failed: {error}"));
    assert!(matches!(
        expired.effects().first(),
        Some(ProducerEffect::ArmBatchTimer {
            generation,
            deadline,
            ..
        }) if *generation == BatchTimerGeneration::from_raw(3)
            && *deadline == Deadline::from_tick(20)
    ));
    assert!(matches!(
        expired.effects().last(),
        Some(ProducerEffect::Complete {
            operation_id,
            completion: ProducerCompletion::Failed(failure),
        }) if *operation_id == second
            && failure.kind() == ProducerFailureKind::DeadlineElapsed
    ));
    assert!(expired.effects().iter().any(|effect| matches!(
        effect,
        ProducerEffect::RemoveBatchMember {
            batch_id: removed_batch,
            operation_id,
        } if *removed_batch == batch_id && *operation_id == second
    )));

    producer
        .apply(ProducerInput::BatchTimerFired {
            batch_id,
            generation: BatchTimerGeneration::from_raw(3),
            now: Moment::from_tick(20),
        })
        .unwrap_or_else(|error| panic!("linger timer failed: {error}"));
    producer
        .apply(ProducerInput::BatchMaterializationFailed { batch_id })
        .unwrap_or_else(|error| panic!("materialization failure failed: {error}"));
    let removed = producer
        .apply(ProducerInput::BatchTimerFired {
            batch_id,
            generation: BatchTimerGeneration::from_raw(3),
            now: Moment::from_tick(30),
        })
        .unwrap_or_else(|error| panic!("removed timer failed: {error}"));
    assert!(removed.effects().is_empty());
}

#[test]
fn linger_before_accumulation_rearms_the_operation_deadline() {
    let policy = ProducerBatchPolicy::try_new(10, ByteCount::new(1_024), 10)
        .unwrap_or_else(|error| panic!("valid policy: {error}"));
    let mut producer = ProducerMachine::with_batch_policy(ByteCount::new(64), 1, policy);
    let (operation_id, batch_id, initial) = admit(&mut producer, 1, 30);
    assert!(matches!(
        initial.as_slice(),
        [ProducerEffect::ArmBatchTimer {
            generation,
            deadline,
            ..
        }] if *generation == BatchTimerGeneration::from_raw(1)
            && *deadline == Deadline::from_tick(10)
    ));

    let linger = producer
        .apply(ProducerInput::BatchTimerFired {
            batch_id,
            generation: BatchTimerGeneration::from_raw(1),
            now: Moment::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("linger timer failed: {error}"));
    assert!(matches!(
        linger.effects(),
        [ProducerEffect::ArmBatchTimer {
            generation,
            deadline,
            ..
        }] if *generation == BatchTimerGeneration::from_raw(2)
            && *deadline == Deadline::from_tick(30)
    ));
    assert!(
        !linger
            .effects()
            .iter()
            .any(|effect| matches!(effect, ProducerEffect::MaterializeBatch { .. }))
    );

    let expired = producer
        .apply(ProducerInput::BatchTimerFired {
            batch_id,
            generation: BatchTimerGeneration::from_raw(2),
            now: Moment::from_tick(30),
        })
        .unwrap_or_else(|error| panic!("deadline timer failed: {error}"));
    assert!(matches!(
        expired.effects().last(),
        Some(ProducerEffect::Complete {
            operation_id: completed,
            completion: ProducerCompletion::Failed(failure),
        }) if *completed == operation_id
            && failure.kind() == ProducerFailureKind::DeadlineElapsed
    ));
    assert!(
        !expired
            .effects()
            .iter()
            .any(|effect| matches!(effect, ProducerEffect::MaterializeBatch { .. }))
    );
}
