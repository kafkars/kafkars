//! Private classic-group commit observer translation shape contract.

use std::future::Future;

use super::group_consumer_commit::{GroupConsumerCommit, GroupConsumerCommitError};

#[test]
fn bridge_commit_is_send_runtime_neutral_observation() {
    fn require_send<T: Send>() {}
    fn require_future<T: Future<Output = Result<(), GroupConsumerCommitError>>>() {}
    fn contract(operation: GroupConsumerCommit) {
        let _: Option<crate::KafkaError> = operation.advisory_error();
        let _: Result<(), GroupConsumerCommitError> = operation.wait();
    }
    fn error_contract(error: GroupConsumerCommitError) {
        let _: (
            Option<super::group_consumer_checkpoint::GroupConsumerCheckpoint>,
            crate::KafkaError,
        ) = error.into_parts();
    }

    require_send::<GroupConsumerCommit>();
    require_future::<GroupConsumerCommit>();
    let _ = contract as fn(GroupConsumerCommit);
    let _ = error_contract as fn(GroupConsumerCommitError);
}
