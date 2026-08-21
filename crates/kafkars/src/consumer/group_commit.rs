//! Named runtime-neutral observation of one accepted group checkpoint commit.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::consumer_facade::group_consumer_commit::GroupConsumerCommit};

use super::{Checkpoint, Consumer, ConsumerCommitAdmissionError, ConsumerCommitError};

impl Consumer {
    /// Attempts one assignment-fenced offset commit with an explicit timeout.
    ///
    /// Rejection precedes deterministic admission and returns the exact
    /// checkpoint. Acceptance already owns terminal-completion capacity.
    #[expect(
        clippy::result_large_err,
        reason = "pre-admission rejection returns the exact assignment-fenced checkpoint"
    )]
    pub fn try_commit(
        &mut self,
        checkpoint: Checkpoint,
        timeout: std::time::Duration,
    ) -> Result<CommitConsumerCheckpoint, ConsumerCommitAdmissionError> {
        self.engine
            .try_commit(checkpoint.into_bridge(), timeout)
            .map(CommitConsumerCheckpoint::from_bridge)
            .map_err(|(checkpoint, error)| {
                ConsumerCommitAdmissionError::new(Checkpoint::from_bridge(checkpoint), error)
            })
    }
}

/// Sole terminal observer for one accepted assignment-fenced offset commit.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling the accepted commit"]
pub struct CommitConsumerCheckpoint {
    inner: GroupConsumerCommit,
}

impl CommitConsumerCheckpoint {
    pub(crate) const fn from_bridge(inner: GroupConsumerCommit) -> Self {
        Self { inner }
    }

    /// Returns an advisory post-admission fault without replacing the terminal.
    pub fn advisory_error(&self) -> Option<KafkaError> {
        self.inner.advisory_error()
    }

    /// Blocks on the same bounded terminal cell used by [`Future::poll`].
    #[expect(
        clippy::result_large_err,
        reason = "accepted failure retains the exact checkpoint when recovery is authoritative"
    )]
    pub fn wait(self) -> Result<(), ConsumerCommitError> {
        self.inner.wait().map_err(translate_commit_error)
    }
}

impl Future for CommitConsumerCheckpoint {
    type Output = Result<(), ConsumerCommitError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map_err(translate_commit_error))
    }
}

fn translate_commit_error(
    error: crate::bridge::consumer_facade::group_consumer_commit::GroupConsumerCommitError,
) -> ConsumerCommitError {
    let (checkpoint, error) = error.into_parts();
    ConsumerCommitError::new(checkpoint.map(Checkpoint::from_bridge), error)
}
