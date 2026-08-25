//! Runtime-neutral observation of one accepted transactional record send.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use kafka_client_core::{TransactionEpoch, TransactionSendId};

use crate::{
    completion::{CompletionObserver, CompletionObserverError},
    transaction::send::TransactionSendTerminal,
};

use super::{TransactionSendOutcome, TransactionToken, send_outcome::translate_send_terminal};

/// Sole named observer for one accepted transactional record send.
#[must_use = "dropping abandons observation without cancelling the accepted send"]
pub struct TransactionSendObserver<'send, 'owner> {
    inner: CompletionObserver<TransactionSendTerminal>,
    _transaction: &'send mut TransactionToken<'owner>,
    epoch: TransactionEpoch,
    send_id: TransactionSendId,
    topic: Option<Arc<str>>,
    topic_uuid: Option<[u8; 16]>,
    partition: Option<i32>,
}

impl<'send, 'owner> TransactionSendObserver<'send, 'owner> {
    pub(super) const fn new(
        inner: CompletionObserver<TransactionSendTerminal>,
        transaction: &'send mut TransactionToken<'owner>,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        topic: Arc<str>,
        topic_uuid: Option<[u8; 16]>,
        partition: Option<i32>,
    ) -> Self {
        Self {
            inner,
            _transaction: transaction,
            epoch,
            send_id,
            topic: Some(topic),
            topic_uuid,
            partition,
        }
    }

    /// Blocks on the same bounded terminal cell used by [`Future::poll`].
    pub fn wait(mut self) -> Result<TransactionSendOutcome, TransactionSendObserverError> {
        let terminal = self.inner.wait().map_err(observer_error)?;
        let topic = self
            .topic
            .take()
            .ok_or(TransactionSendObserverError::AlreadyObserved)?;
        translate_send_terminal(
            terminal,
            self.epoch,
            self.send_id,
            topic,
            self.topic_uuid,
            self.partition,
        )
        .ok_or(TransactionSendObserverError::InternalInvariant)
    }
}

impl Future for TransactionSendObserver<'_, '_> {
    type Output = Result<TransactionSendOutcome, TransactionSendObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Ready(Ok(terminal)) => {
                let Some(topic) = this.topic.take() else {
                    return Poll::Ready(Err(TransactionSendObserverError::AlreadyObserved));
                };
                Poll::Ready(
                    translate_send_terminal(
                        terminal,
                        this.epoch,
                        this.send_id,
                        topic,
                        this.topic_uuid,
                        this.partition,
                    )
                    .ok_or(TransactionSendObserverError::InternalInvariant),
                )
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(observer_error(error))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl fmt::Debug for TransactionSendObserver<'_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionSendObserver")
            .finish_non_exhaustive()
    }
}

/// Failure to observe or correlate one accepted transactional send.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionSendObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The bounded completion generation is no longer live.
    Stale,
    /// The terminal did not match the exact accepted transaction and send.
    InternalInvariant,
}

impl fmt::Display for TransactionSendObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "transactional send was already observed",
            Self::Stale => "transactional send observer is stale",
            Self::InternalInvariant => {
                "transactional send terminal violated its accepted correlation"
            }
        })
    }
}

impl std::error::Error for TransactionSendObserverError {}

const fn observer_error(error: CompletionObserverError) -> TransactionSendObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => TransactionSendObserverError::AlreadyObserved,
        CompletionObserverError::Stale => TransactionSendObserverError::Stale,
    }
}
