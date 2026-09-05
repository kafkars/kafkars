//! Runtime-neutral facade translation for accepted classic-group checkpoint commits.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    GroupConsumerCommitDeliveryStatus as EngineDeliveryStatus,
    GroupConsumerCommitFailureKind as EngineFailureKind,
    GroupConsumerCommitObserver as EngineObserver,
    GroupConsumerCommitObserverError as EngineObserverError,
    GroupConsumerCommitOutcome as EngineOutcome,
    GroupConsumerCommitPartitionResult as EnginePartitionResult,
};

use crate::{DeliveryStatus, ErrorKind, KafkaError};

use super::group_consumer_checkpoint::GroupConsumerCheckpoint;

/// Private named observer over one accepted engine commit.
pub(crate) struct GroupConsumerCommit {
    inner: EngineObserver,
    advisory_error: Option<KafkaError>,
}

impl GroupConsumerCommit {
    pub(crate) const fn new(inner: EngineObserver, advisory_error: Option<KafkaError>) -> Self {
        Self {
            inner,
            advisory_error,
        }
    }

    pub(crate) fn advisory_error(&self) -> Option<KafkaError> {
        self.advisory_error.clone()
    }

    #[expect(
        clippy::result_large_err,
        reason = "commit failure preserves the exact checkpoint when the engine returns it"
    )]
    pub(crate) fn wait(self) -> Result<(), GroupConsumerCommitError> {
        self.inner
            .wait()
            .map_err(|error| {
                GroupConsumerCommitError::without_checkpoint(translate_observer_error(error))
            })
            .and_then(translate_outcome)
    }
}

impl Future for GroupConsumerCommit {
    type Output = Result<(), GroupConsumerCommitError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context).map(|result| {
            result
                .map_err(|error| {
                    GroupConsumerCommitError::without_checkpoint(translate_observer_error(error))
                })
                .and_then(translate_outcome)
        })
    }
}

impl core::fmt::Debug for GroupConsumerCommit {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("GroupConsumerCommit")
            .field("inner", &self.inner)
            .field("advisory_error", &self.advisory_error)
            .finish()
    }
}

#[expect(
    clippy::result_large_err,
    reason = "terminal translation preserves the exact checkpoint returned by the engine"
)]
fn translate_outcome(outcome: EngineOutcome) -> Result<(), GroupConsumerCommitError> {
    match outcome {
        EngineOutcome::Committed(_batch) => Ok(()),
        EngineOutcome::BrokerRejected(batch, checkpoint) => {
            let broker_code = batch
                .outcomes()
                .iter()
                .find_map(|outcome| match outcome.result() {
                    EnginePartitionResult::Committed => None,
                    EnginePartitionResult::Rejected(error) => Some(error.code()),
                });
            Err(GroupConsumerCommitError::with_checkpoint(
                GroupConsumerCheckpoint::from_engine(checkpoint),
                KafkaError::new(ErrorKind::Broker, "Kafka rejected the group offset commit")
                    .with_broker_code(broker_code)
                    .with_delivery_status(DeliveryStatus::PossiblySent),
            ))
        }
        EngineOutcome::Failed(failure, checkpoint) => {
            Err(GroupConsumerCommitError::with_checkpoint(
                GroupConsumerCheckpoint::from_engine(checkpoint),
                translate_commit_failure(failure.kind(), failure.delivery()),
            ))
        }
    }
}

pub(super) fn translate_commit_failure(
    failure: EngineFailureKind,
    delivery: EngineDeliveryStatus,
) -> KafkaError {
    let kind = match failure {
        EngineFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        EngineFailureKind::DriverRejected => ErrorKind::Backpressure,
        EngineFailureKind::ExecutionUnavailable => ErrorKind::Internal,
        EngineFailureKind::Transport => ErrorKind::Transport,
        EngineFailureKind::Compatibility => ErrorKind::Compatibility,
        EngineFailureKind::InvalidResponse | EngineFailureKind::ResponseTooLarge => {
            ErrorKind::Internal
        }
    };
    let delivery = match delivery {
        EngineDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        EngineDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    };
    let error = KafkaError::new(kind, "group offset commit did not complete")
        .with_delivery_status(delivery);
    if delivery == DeliveryStatus::NotSent
        && matches!(
            failure,
            EngineFailureKind::DriverRejected | EngineFailureKind::Transport
        )
    {
        error.with_safe_retry()
    } else {
        error
    }
}

pub(crate) struct GroupConsumerCommitError {
    checkpoint: Option<GroupConsumerCheckpoint>,
    error: KafkaError,
}

impl GroupConsumerCommitError {
    fn with_checkpoint(checkpoint: GroupConsumerCheckpoint, error: KafkaError) -> Self {
        Self {
            checkpoint: Some(checkpoint),
            error,
        }
    }

    fn without_checkpoint(error: KafkaError) -> Self {
        Self {
            checkpoint: None,
            error,
        }
    }

    pub(crate) fn into_parts(self) -> (Option<GroupConsumerCheckpoint>, KafkaError) {
        (self.checkpoint, self.error)
    }
}

fn translate_observer_error(error: EngineObserverError) -> KafkaError {
    match error {
        EngineObserverError::AlreadyObserved | EngineObserverError::Stale => KafkaError::new(
            ErrorKind::State,
            "group offset commit observer is no longer live",
        ),
        EngineObserverError::InternalInvariant => KafkaError::new(
            ErrorKind::Internal,
            "group offset commit terminal correlation failed",
        ),
    }
}
