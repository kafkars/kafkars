//! Runtime-neutral producer send spanning pending admission and delivery.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    ProducerDeliveryError, ProducerDeliveryObserver, ProducerObserverError, ProducerRecordMetadata,
};

use super::super::pending::{
    PendingCellError, PendingCellTransition, PendingSendCell, ProducerSendFailure,
};

/// Terminal result for a producer send that may wait for local admission.
pub type ProducerSendResult = Result<ProducerRecordMetadata, ProducerSendError>;

/// Failure from either local pending admission or accepted delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerSendError {
    /// Work crossed core admission and then delivery observation failed.
    Delivery(ProducerDeliveryError),
    /// Work settled locally and definitely did not reach transport ownership.
    Local(ProducerSendFailure),
    /// The pending observation cell was consumed or abandoned.
    Observer(ProducerObserverError),
}

impl fmt::Display for ProducerSendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Delivery(error) => error.fmt(formatter),
            Self::Local(failure) => {
                write!(
                    formatter,
                    "producer send failed locally: {:?}",
                    failure.kind()
                )
            }
            Self::Observer(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ProducerSendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Delivery(error) => Some(error),
            Self::Local(_) => None,
            Self::Observer(error) => Some(error),
        }
    }
}

/// Named, non-cloneable operation covering pending admission and delivery.
#[must_use = "dropping before promotion cancels pending ownership; after promotion it abandons observation"]
pub struct ProducerSend {
    state: ProducerSendState,
}

enum ProducerSendState {
    Pending(Arc<PendingSendCell>),
    Accepted(ProducerDeliveryObserver),
    Ready(ProducerSendFailure),
    Consumed,
}

impl ProducerSend {
    pub(crate) const fn from_pending(cell: Arc<PendingSendCell>) -> Self {
        Self {
            state: ProducerSendState::Pending(cell),
        }
    }

    pub(crate) const fn from_accepted(observer: ProducerDeliveryObserver) -> Self {
        Self {
            state: ProducerSendState::Accepted(observer),
        }
    }

    pub(crate) const fn from_local_failure(failure: ProducerSendFailure) -> Self {
        Self {
            state: ProducerSendState::Ready(failure),
        }
    }

    /// Blocks on the same pending and accepted cells used by `Future::poll`.
    pub fn wait(mut self) -> ProducerSendResult {
        loop {
            let state = std::mem::replace(&mut self.state, ProducerSendState::Consumed);
            match state {
                ProducerSendState::Pending(cell) => match cell.wait() {
                    Ok(PendingCellTransition::Accepted(observer)) => {
                        self.state = ProducerSendState::Accepted(observer);
                    }
                    Ok(PendingCellTransition::Ready(failure)) => {
                        return Err(ProducerSendError::Local(failure));
                    }
                    Err(error) => return Err(pending_error(error)),
                },
                ProducerSendState::Accepted(observer) => {
                    return observer.wait().map_err(ProducerSendError::Delivery);
                }
                ProducerSendState::Ready(failure) => {
                    return Err(ProducerSendError::Local(failure));
                }
                ProducerSendState::Consumed => {
                    return Err(ProducerSendError::Observer(
                        ProducerObserverError::AlreadyObserved,
                    ));
                }
            }
        }
    }
}

impl Future for ProducerSend {
    type Output = ProducerSendResult;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let state = std::mem::replace(&mut self.state, ProducerSendState::Consumed);
            match state {
                ProducerSendState::Pending(cell) => match cell.poll(context) {
                    Ok(Poll::Pending) => {
                        self.state = ProducerSendState::Pending(cell);
                        return Poll::Pending;
                    }
                    Ok(Poll::Ready(PendingCellTransition::Accepted(observer))) => {
                        self.state = ProducerSendState::Accepted(observer);
                    }
                    Ok(Poll::Ready(PendingCellTransition::Ready(failure))) => {
                        return Poll::Ready(Err(ProducerSendError::Local(failure)));
                    }
                    Err(error) => return Poll::Ready(Err(pending_error(error))),
                },
                ProducerSendState::Accepted(mut observer) => {
                    match Pin::new(&mut observer).poll(context) {
                        Poll::Pending => {
                            self.state = ProducerSendState::Accepted(observer);
                            return Poll::Pending;
                        }
                        Poll::Ready(result) => {
                            return Poll::Ready(result.map_err(ProducerSendError::Delivery));
                        }
                    }
                }
                ProducerSendState::Ready(failure) => {
                    return Poll::Ready(Err(ProducerSendError::Local(failure)));
                }
                ProducerSendState::Consumed => {
                    return Poll::Ready(Err(ProducerSendError::Observer(
                        ProducerObserverError::AlreadyObserved,
                    )));
                }
            }
        }
    }
}

impl Drop for ProducerSend {
    fn drop(&mut self) {
        let state = std::mem::replace(&mut self.state, ProducerSendState::Consumed);
        if let ProducerSendState::Pending(cell) = state {
            let _outcome = cell.abandon();
        }
    }
}

impl fmt::Debug for ProducerSend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerSend")
            .finish_non_exhaustive()
    }
}

const fn pending_error(error: PendingCellError) -> ProducerSendError {
    match error {
        PendingCellError::AlreadyConsumed => {
            ProducerSendError::Observer(ProducerObserverError::AlreadyObserved)
        }
        PendingCellError::Abandoned
        | PendingCellError::TransitionInProgress
        | PendingCellError::AlreadySettled => {
            ProducerSendError::Observer(ProducerObserverError::Stale)
        }
    }
}
