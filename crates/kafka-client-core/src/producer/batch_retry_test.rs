//! Producer batch accumulation and retry-mutation ownership scenarios.

use crate::{
    BatchExecutionGeneration, BatchTimerGeneration, ByteCount, Deadline, Moment, OperationId,
    PartitionIndex, ProducerBatchPolicy, ProducerMachineError, TopicId, TransitionError,
};

use super::{BatchRoute, BatchState, ProducerBatch};

#[test]
fn retry_commit_replaces_every_attempt_identity_in_one_owner() {
    let mut batch = ProducerBatch::new(
        BatchRoute {
            topic_id: TopicId::from_raw(7),
            partition: PartitionIndex::from_raw(3),
        },
        ProducerBatchPolicy::single_record(),
        Moment::from_tick(1),
        OperationId::from_raw(11),
        Deadline::from_tick(20),
    )
    .unwrap_or_else(|| panic!("test batch deadline is representable"));
    let replacement = BatchExecutionGeneration::try_from_raw(2)
        .unwrap_or_else(|| panic!("replacement generation is nonzero"));
    let timer_generation = BatchTimerGeneration::from_raw(4);
    let retry_deadline = Deadline::from_tick(8);

    batch.commit_retry_waiting(
        replacement,
        1,
        timer_generation,
        retry_deadline,
        crate::DeliveryStatus::NotSent,
    );

    assert_eq!(batch.execution_generation, Some(replacement));
    assert_eq!(batch.retries_started, 1);
    assert_eq!(batch.timer_generation, timer_generation);
    assert_eq!(batch.timer_deadline, retry_deadline);
    assert_eq!(batch.state, BatchState::RetryWaiting);
}

#[test]
fn accumulation_count_preserves_out_of_order_facts() {
    let mut batch = batch_with_policy(3);
    batch.commit_add_member(OperationId::from_raw(12), Deadline::from_tick(20), None);
    batch.commit_add_member(OperationId::from_raw(13), Deadline::from_tick(20), None);

    for (operation_id, accumulated_members, readies_batch) in [
        (OperationId::from_raw(12), 1, false),
        (OperationId::from_raw(11), 2, false),
        (OperationId::from_raw(13), 3, true),
    ] {
        let plan = batch
            .plan_accumulation(operation_id, ByteCount::new(10))
            .unwrap_or_else(|error| panic!("accumulation should plan: {error}"));
        assert_eq!(plan.accumulated_members, accumulated_members);
        assert_eq!(plan.readies_batch, readies_batch);
        batch.commit_accumulation(plan, ByteCount::new(10));
        assert_eq!(batch.accumulated_members, accumulated_members);
    }

    assert!(batch.all_accumulated());
    assert!(batch.is_ready());
}

#[test]
fn duplicate_accumulation_does_not_advance_count_or_bytes() {
    let mut batch = batch_with_policy(2);
    let operation_id = OperationId::from_raw(11);
    let plan = batch
        .plan_accumulation(operation_id, ByteCount::new(10))
        .unwrap_or_else(|error| panic!("first accumulation should plan: {error}"));
    batch.commit_accumulation(plan, ByteCount::new(10));

    assert!(matches!(
        batch.plan_accumulation(operation_id, ByteCount::new(10)),
        Err(ProducerMachineError::Transition(
            TransitionError::AlreadyAccumulated
        ))
    ));
    assert_eq!(batch.accumulated_members, 1);
    assert_eq!(batch.accumulator_bytes, ByteCount::new(10));
    assert_eq!(batch.members[0].accumulator_bytes, Some(ByteCount::new(10)));
}

fn batch_with_policy(max_records: usize) -> ProducerBatch {
    let policy = ProducerBatchPolicy::try_new(max_records, ByteCount::new(1_024), 20)
        .unwrap_or_else(|error| panic!("test policy should be valid: {error}"));
    ProducerBatch::new(
        BatchRoute {
            topic_id: TopicId::from_raw(7),
            partition: PartitionIndex::from_raw(3),
        },
        policy,
        Moment::from_tick(1),
        OperationId::from_raw(11),
        Deadline::from_tick(20),
    )
    .unwrap_or_else(|| panic!("test batch deadline is representable"))
}
