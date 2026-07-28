//! Runtime-neutral observation of one accepted group checkpoint commit.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use kafka_client_core::GroupOffsetCommitTerminal;

use super::{GroupConsumerCommitOutcome, outcome::translate_terminal};
use crate::{
    completion::{CompletionObserver, CompletionObserverError},
    consumer::group_batch::GroupConsumerCheckpointObservation,
};

/// Sole named observer for one accepted classic-group offset commit.
#[must_use = "dropping abandons observation without cancelling the accepted commit"]
pub struct GroupConsumerCommitObserver {
    inner: CompletionObserver<GroupOffsetCommitTerminal>,
    observation: Option<GroupConsumerCheckpointObservation>,
    _lifetime: Arc<dyn Send + Sync>,
}

impl GroupConsumerCommitObserver {
    pub(in crate::consumer) fn new(
        inner: CompletionObserver<GroupOffsetCommitTerminal>,
        observation: GroupConsumerCheckpointObservation,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            inner,
            observation: Some(observation),
            _lifetime: lifetime,
        }
    }

    /// Blocks on the same bounded terminal cell used by [`Future::poll`].
    pub fn wait(mut self) -> Result<GroupConsumerCommitOutcome, GroupConsumerCommitObserverError> {
        let terminal = self.inner.wait().map_err(observer_error)?;
        let observation = self
            .observation
            .take()
            .ok_or(GroupConsumerCommitObserverError::AlreadyObserved)?;
        translate_terminal(terminal, observation)
    }
}

impl Future for GroupConsumerCommitObserver {
    type Output = Result<GroupConsumerCommitOutcome, GroupConsumerCommitObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Ready(Ok(terminal)) => {
                let Some(observation) = this.observation.take() else {
                    return Poll::Ready(Err(GroupConsumerCommitObserverError::AlreadyObserved));
                };
                Poll::Ready(translate_terminal(terminal, observation))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(observer_error(error))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl fmt::Debug for GroupConsumerCommitObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GroupConsumerCommitObserver")
            .finish_non_exhaustive()
    }
}

/// Failure to observe or translate one accepted commit terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerCommitObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The bounded completion generation is no longer live.
    Stale,
    /// A terminal violated the exact admitted checkpoint correlation.
    InternalInvariant,
}

impl fmt::Display for GroupConsumerCommitObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "group checkpoint commit was already observed",
            Self::Stale => "group checkpoint commit observer is stale",
            Self::InternalInvariant => {
                "group checkpoint commit terminal violated its admitted correlation"
            }
        })
    }
}

impl std::error::Error for GroupConsumerCommitObserverError {}

const fn observer_error(error: CompletionObserverError) -> GroupConsumerCommitObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            GroupConsumerCommitObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => GroupConsumerCommitObserverError::Stale,
    }
}
