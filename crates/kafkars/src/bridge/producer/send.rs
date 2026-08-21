//! One ergonomic producer send across waiting admission and terminal delivery.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{CancellationOutcome, ErrorKind, KafkaError, RecordMetadata};

use super::ProducerDelivery;

enum ProducerSendState {
    Ready(Option<Result<RecordMetadata, KafkaError>>),
    Accepted(ProducerDelivery),
    Consumed,
}

/// Runtime-neutral bridge for a result that may still be waiting on capacity.
pub(crate) struct ProducerSend {
    state: ProducerSendState,
}

impl ProducerSend {
    pub(crate) const fn ready(error: KafkaError) -> Self {
        Self {
            state: ProducerSendState::Ready(Some(Err(error))),
        }
    }

    pub(crate) const fn accepted(delivery: ProducerDelivery) -> Self {
        Self {
            state: ProducerSendState::Accepted(delivery),
        }
    }

    pub(crate) fn wait(self) -> Result<RecordMetadata, KafkaError> {
        match self.state {
            ProducerSendState::Ready(Some(result)) => result,
            ProducerSendState::Accepted(delivery) => delivery.wait(),
            ProducerSendState::Ready(None) | ProducerSendState::Consumed => Err(already_observed()),
        }
    }

    pub(crate) fn cancel(&mut self) -> Result<CancellationOutcome, KafkaError> {
        match &mut self.state {
            ProducerSendState::Accepted(delivery) => delivery.cancel(),
            ProducerSendState::Ready(_) | ProducerSendState::Consumed => Err(KafkaError::new(
                ErrorKind::State,
                "producer send is already terminal",
            )),
        }
    }
}

impl Future for ProducerSend {
    type Output = Result<RecordMetadata, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.state {
            ProducerSendState::Ready(result) => {
                let result = result.take().unwrap_or_else(|| Err(already_observed()));
                this.state = ProducerSendState::Consumed;
                Poll::Ready(result)
            }
            ProducerSendState::Accepted(delivery) => match Pin::new(delivery).poll(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    this.state = ProducerSendState::Consumed;
                    Poll::Ready(result)
                }
            },
            ProducerSendState::Consumed => Poll::Ready(Err(already_observed())),
        }
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(ErrorKind::State, "producer send was already observed")
}

impl std::fmt::Debug for ProducerSend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProducerSend")
            .field(
                "state",
                &match self.state {
                    ProducerSendState::Ready(_) => "ready",
                    ProducerSendState::Accepted(_) => "accepted",
                    ProducerSendState::Consumed => "consumed",
                },
            )
            .finish()
    }
}
