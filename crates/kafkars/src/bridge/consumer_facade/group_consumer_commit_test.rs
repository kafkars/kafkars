//! Private group-commit observer shape and exact retry-advice evidence.

use std::future::Future;

use kafka_client_engine::{
    GroupConsumerCommitDeliveryStatus as EngineDelivery,
    GroupConsumerCommitFailureKind as EngineFailure,
};

use super::group_consumer_commit::{
    GroupConsumerCommit, GroupConsumerCommitError, translate_commit_failure,
};
use crate::{DeliveryStatus, RetryAdvice};

#[test]
fn only_definitely_unsent_transient_commits_advise_checkpoint_retry() {
    for failure in [
        EngineFailure::DriverRejected,
        EngineFailure::Transport,
        EngineFailure::DeadlineElapsed,
        EngineFailure::ExecutionUnavailable,
        EngineFailure::Compatibility,
        EngineFailure::InvalidResponse,
        EngineFailure::ResponseTooLarge,
    ] {
        for delivery in [EngineDelivery::NotSent, EngineDelivery::PossiblySent] {
            let error = translate_commit_failure(failure, delivery);
            let expected = if delivery == EngineDelivery::NotSent
                && matches!(
                    failure,
                    EngineFailure::DriverRejected | EngineFailure::Transport
                ) {
                RetryAdvice::RetrySafe
            } else {
                RetryAdvice::DoNotRetry
            };
            assert_eq!(error.retry_advice(), expected);
            assert_eq!(
                error.delivery_status(),
                Some(match delivery {
                    EngineDelivery::NotSent => DeliveryStatus::NotSent,
                    EngineDelivery::PossiblySent => DeliveryStatus::PossiblySent,
                })
            );
        }
    }
}

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
