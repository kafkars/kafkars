//! Runtime-neutral observation of one accepted transactional offset transfer.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::TransactionOffsetCommitId;

use crate::{
    completion::{CompletionObserver, CompletionObserverError},
    transaction::offset_commit::TransactionOffsetCommitResult,
};

use super::{
    TransactionOffsetsOutcome, TransactionToken, offset_outcome::translate_offset_outcome,
};

/// Sole named observer for one accepted transactional offset transfer.
#[must_use = "dropping abandons observation without cancelling the accepted transfer"]
pub struct TransactionOffsetsObserver<'send, 'owner> {
    inner: CompletionObserver<TransactionOffsetCommitResult>,
    _transaction: &'send mut TransactionToken<'owner>,
    operation_id: TransactionOffsetCommitId,
}

impl<'send, 'owner> TransactionOffsetsObserver<'send, 'owner> {
    pub(super) const fn new(
        inner: CompletionObserver<TransactionOffsetCommitResult>,
        transaction: &'send mut TransactionToken<'owner>,
        operation_id: TransactionOffsetCommitId,
    ) -> Self {
        Self {
            inner,
            _transaction: transaction,
            operation_id,
        }
    }

    /// Blocks on the same bounded terminal cell used by [`Future::poll`].
    pub fn wait(self) -> Result<TransactionOffsetsOutcome, TransactionOffsetsObserverError> {
        translate(
            self.inner.wait().map_err(observer_error)?,
            self.operation_id,
        )
    }
}

impl Future for TransactionOffsetsObserver<'_, '_> {
    type Output = Result<TransactionOffsetsOutcome, TransactionOffsetsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| translate(result.map_err(observer_error)?, this.operation_id))
    }
}

impl fmt::Debug for TransactionOffsetsObserver<'_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionOffsetsObserver")
            .finish_non_exhaustive()
    }
}

/// Failure to observe or correlate an accepted offset transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOffsetsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The bounded completion generation is no longer live.
    Stale,
    /// The terminal did not match the exact accepted operation.
    InternalInvariant,
}

impl fmt::Display for TransactionOffsetsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "transactional offsets were already observed",
            Self::Stale => "transactional offset observer is stale",
            Self::InternalInvariant => "transactional offset terminal violated its correlation",
        })
    }
}

impl std::error::Error for TransactionOffsetsObserverError {}

fn translate(
    result: TransactionOffsetCommitResult,
    operation_id: TransactionOffsetCommitId,
) -> Result<TransactionOffsetsOutcome, TransactionOffsetsObserverError> {
    if result.operation_id() != operation_id {
        return Err(TransactionOffsetsObserverError::InternalInvariant);
    }
    let outcome = result.outcome();
    drop(result.into_input());
    Ok(translate_offset_outcome(outcome))
}

const fn observer_error(error: CompletionObserverError) -> TransactionOffsetsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            TransactionOffsetsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => TransactionOffsetsObserverError::Stale,
    }
}
