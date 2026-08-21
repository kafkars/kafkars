//! Exact facade classic-group commit admission shape contract.

use super::{
    group_consumer::GroupConsumerEngine, group_consumer_checkpoint::GroupConsumerCheckpoint,
    group_consumer_commit::GroupConsumerCommit,
};
use crate::KafkaError;

type CommitAdmission = fn(
    &mut GroupConsumerEngine,
    GroupConsumerCheckpoint,
    std::time::Duration,
) -> Result<GroupConsumerCommit, (GroupConsumerCheckpoint, KafkaError)>;

#[test]
fn rejection_returns_the_exact_private_checkpoint_owner() {
    let _: CommitAdmission = GroupConsumerEngine::try_commit;
}
