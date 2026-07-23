//! Named runtime-neutral producer delivery observation over one completion cell.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::ProducerCompletion as CoreProducerCompletion;

use crate::completion::CompletionObserver;

use super::{
    ProducerDeliveryError, ProducerDeliveryFailure, ProducerObserverError, ProducerRecordMetadata,
};

/// Terminal result returned by asynchronous and blocking producer observation.
pub type ProducerDeliveryResult = Result<ProducerRecordMetadata, ProducerDeliveryError>;

/// Single non-cloneable observer for one accepted producer operation.
///
/// Dropping this value abandons observation only. It does not cancel engine
/// work or claim that Kafka did not receive the operation.
#[must_use = "dropping abandons delivery observation without cancelling the operation"]
pub struct ProducerDeliveryObserver {
    inner: CompletionObserver<CoreProducerCompletion>,
}

impl ProducerDeliveryObserver {
    pub(crate) const fn from_completion(inner: CompletionObserver<CoreProducerCompletion>) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> ProducerDeliveryResult {
        match self.inner.wait() {
            Ok(completion) => translate(completion),
            Err(error) => Err(observer_error(error)),
        }
    }
}

impl Future for ProducerDeliveryObserver {
    type Output = ProducerDeliveryResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(completion)) => Poll::Ready(translate(completion)),
            Poll::Ready(Err(error)) => Poll::Ready(Err(observer_error(error))),
        }
    }
}

impl fmt::Debug for ProducerDeliveryObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerDeliveryObserver")
            .finish_non_exhaustive()
    }
}

const fn translate(completion: CoreProducerCompletion) -> ProducerDeliveryResult {
    match completion {
        CoreProducerCompletion::Delivered(metadata) => {
            Ok(ProducerRecordMetadata::from_core(metadata))
        }
        CoreProducerCompletion::Failed(failure) => Err(ProducerDeliveryError::Failed(
            ProducerDeliveryFailure::from_core(failure),
        )),
    }
}

const fn observer_error(
    error: crate::completion::CompletionObserverError,
) -> ProducerDeliveryError {
    ProducerDeliveryError::Observer(ProducerObserverError::from_completion(error))
}
