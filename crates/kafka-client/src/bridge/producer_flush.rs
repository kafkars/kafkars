//! Single-observer producer flush bridge over accepted or immediately-ready state.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::ProducerFlushObserver as EngineFlushObserver;

use crate::{KafkaError, bridge::producer_result::flush::translate_flush_result};

/// Private runtime-neutral flush observer shared by asynchronous and blocking APIs.
#[must_use = "dropping abandons observation without cancelling an accepted producer flush"]
pub(crate) struct ProducerFlush {
    state: ProducerFlushState,
    accepted_diagnostic: Option<KafkaError>,
}

enum ProducerFlushState {
    Accepted(EngineFlushObserver),
    Ready(Option<Result<(), KafkaError>>),
}

impl ProducerFlush {
    pub(crate) const fn accepted(
        observer: EngineFlushObserver,
        accepted_diagnostic: Option<KafkaError>,
    ) -> Self {
        Self {
            state: ProducerFlushState::Accepted(observer),
            accepted_diagnostic,
        }
    }

    pub(crate) const fn ready(result: Result<(), KafkaError>) -> Self {
        Self {
            state: ProducerFlushState::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }

    /// Blocks on the same terminal state used by `Future::poll`.
    pub(crate) fn wait(self) -> Result<(), KafkaError> {
        match self.state {
            ProducerFlushState::Accepted(observer) => translate_flush_result(observer.wait()),
            ProducerFlushState::Ready(Some(result)) => result,
            ProducerFlushState::Ready(None) => Err(already_observed()),
        }
    }
}

impl Future for ProducerFlush {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.state {
            ProducerFlushState::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_flush_result)
            }
            ProducerFlushState::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

fn already_observed() -> KafkaError {
    crate::KafkaError::new(
        crate::ErrorKind::State,
        "producer flush was already observed",
    )
}

impl fmt::Debug for ProducerFlush {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerFlush")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}
