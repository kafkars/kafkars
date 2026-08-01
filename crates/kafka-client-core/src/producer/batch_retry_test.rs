//! Retry-batch mutation ownership scenarios.

use crate::{
    BatchExecutionGeneration, BatchTimerGeneration, Deadline, Moment, OperationId, PartitionIndex,
    ProducerBatchPolicy, TopicId,
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
