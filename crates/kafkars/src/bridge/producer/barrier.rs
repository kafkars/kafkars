//! Shared single-observer state for producer flush and close barriers.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::ProducerFlushObserver as EngineFlushObserver;

use crate::{
    KafkaError,
    bridge::producer_result::{close::translate_close_result, flush::translate_flush_result},
};

/// Private runtime-neutral barrier shared by asynchronous and blocking APIs.
#[must_use = "dropping abandons observation without cancelling an accepted producer barrier"]
pub(crate) struct ProducerBarrier {
    state: BarrierState,
    kind: BarrierKind,
    accepted_diagnostic: Option<KafkaError>,
}

enum BarrierState {
    Accepted(EngineFlushObserver),
    Ready(Option<Result<(), KafkaError>>),
}

#[derive(Clone, Copy)]
pub(crate) enum BarrierKind {
    Flush,
    Close,
}

impl ProducerBarrier {
    pub(crate) const fn accepted(
        kind: BarrierKind,
        observer: EngineFlushObserver,
        accepted_diagnostic: Option<KafkaError>,
    ) -> Self {
        Self {
            state: BarrierState::Accepted(observer),
            kind,
            accepted_diagnostic,
        }
    }

    pub(crate) const fn ready(kind: BarrierKind, result: Result<(), KafkaError>) -> Self {
        Self {
            state: BarrierState::Ready(Some(result)),
            kind,
            accepted_diagnostic: None,
        }
    }

    /// Blocks on the same terminal state used by `Future::poll`.
    pub(crate) fn wait(self) -> Result<(), KafkaError> {
        match self.state {
            BarrierState::Accepted(observer) => translate(self.kind, observer.wait()),
            BarrierState::Ready(Some(result)) => result,
            BarrierState::Ready(None) => Err(already_observed(self.kind)),
        }
    }
}

impl Future for ProducerBarrier {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.state {
            BarrierState::Accepted(observer) => {
                let kind = this.kind;
                Pin::new(observer)
                    .poll(context)
                    .map(|result| translate(kind, result))
            }
            BarrierState::Ready(result) => Poll::Ready(
                result
                    .take()
                    .unwrap_or_else(|| Err(already_observed(this.kind))),
            ),
        }
    }
}

fn translate(
    kind: BarrierKind,
    result: kafka_client_engine::ProducerFlushResult,
) -> Result<(), KafkaError> {
    match kind {
        BarrierKind::Flush => translate_flush_result(result),
        BarrierKind::Close => translate_close_result(result),
    }
}

fn already_observed(kind: BarrierKind) -> KafkaError {
    let message = match kind {
        BarrierKind::Flush => "producer flush was already observed",
        BarrierKind::Close => "producer close was already observed",
    };
    crate::KafkaError::new(crate::ErrorKind::State, message)
}

impl fmt::Debug for ProducerBarrier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerBarrier")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}
