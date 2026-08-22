//! Lossless hosted group close admission shape contract.

use super::{group_consumer::GroupConsumerEngine, group_consumer_close::GroupConsumerClose};
use crate::{KafkaError, RetryAdvice};
use kafka_client_engine::GroupConsumerCloseAdmissionErrorKind;

type CloseAdmission =
    fn(GroupConsumerEngine) -> Result<GroupConsumerClose, (GroupConsumerEngine, KafkaError)>;

#[test]
fn rejected_close_returns_the_exact_bridge_owner() {
    let _: CloseAdmission = GroupConsumerEngine::try_close;
}

#[test]
fn contended_close_admission_is_explicitly_safe_to_retry() {
    let error = super::group_consumer_close_admission::translate_close_admission(
        GroupConsumerCloseAdmissionErrorKind::Contended,
    );
    assert_eq!(error.retry_advice(), RetryAdvice::RetrySafe);
}
