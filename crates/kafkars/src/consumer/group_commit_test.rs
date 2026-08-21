//! Public accepted group commit observation shape contract.

use std::future::Future;

use super::{
    Checkpoint, CommitConsumerCheckpoint, Consumer, ConsumerCommitAdmissionError,
    ConsumerCommitError,
};

#[test]
fn accepted_commit_is_send_future_with_blocking_and_advisory_observation() {
    fn require_send<T: Send>() {}
    fn require_future<T: Future<Output = Result<(), ConsumerCommitError>>>() {}
    fn contract(operation: CommitConsumerCheckpoint) {
        let _: Option<crate::KafkaError> = operation.advisory_error();
        let _: Result<(), ConsumerCommitError> = operation.wait();
    }
    fn admission_contract(consumer: &mut Consumer, checkpoint: Checkpoint) {
        let _: Result<CommitConsumerCheckpoint, ConsumerCommitAdmissionError> =
            consumer.try_commit(checkpoint, std::time::Duration::ZERO);
    }

    require_send::<CommitConsumerCheckpoint>();
    require_future::<CommitConsumerCheckpoint>();
    let _ = contract as fn(CommitConsumerCheckpoint);
    let _ = admission_contract as fn(&mut Consumer, Checkpoint);
}
