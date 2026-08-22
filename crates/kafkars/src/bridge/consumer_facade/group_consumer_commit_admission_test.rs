//! Exact facade classic-group commit admission shape contract.

use super::{
    group_consumer::GroupConsumerEngine, group_consumer_checkpoint::GroupConsumerCheckpoint,
    group_consumer_commit::GroupConsumerCommit,
};
use crate::{ErrorKind, KafkaError, RetryAdvice};
use kafka_client_engine::GroupConsumerCommitAdmissionErrorKind;

type CommitAdmission = fn(
    &mut GroupConsumerEngine,
    GroupConsumerCheckpoint,
    std::time::Duration,
) -> Result<GroupConsumerCommit, (GroupConsumerCheckpoint, KafkaError)>;

#[test]
fn rejection_returns_the_exact_private_checkpoint_owner() {
    let _: CommitAdmission = GroupConsumerEngine::try_commit;
}

#[test]
fn only_transient_pre_admission_rejections_are_safe_to_retry() {
    for (kind, public, retry) in [
        (
            GroupConsumerCommitAdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
            RetryAdvice::DoNotRetry,
        ),
        (
            GroupConsumerCommitAdmissionErrorKind::Closed,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            GroupConsumerCommitAdmissionErrorKind::Contended,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            GroupConsumerCommitAdmissionErrorKind::GroupUnavailable,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            GroupConsumerCommitAdmissionErrorKind::Backpressure,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            GroupConsumerCommitAdmissionErrorKind::StaleCheckpoint,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            GroupConsumerCommitAdmissionErrorKind::HostUnavailable,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
    ] {
        let error = super::group_consumer_commit_admission::translate_commit_admission(kind);
        assert_eq!(error.kind(), public);
        assert_eq!(error.retry_advice(), retry);
    }
}
