//! Lossless hosted group close admission shape contract.

use super::{group_consumer::GroupConsumerEngine, group_consumer_close::GroupConsumerClose};
use crate::KafkaError;

type CloseAdmission =
    fn(GroupConsumerEngine) -> Result<GroupConsumerClose, (GroupConsumerEngine, KafkaError)>;

#[test]
fn rejected_close_returns_the_exact_bridge_owner() {
    let _: CloseAdmission = GroupConsumerEngine::try_close;
}
