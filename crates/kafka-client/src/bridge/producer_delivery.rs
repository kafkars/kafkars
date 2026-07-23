//! Single-observer delivery bridge retaining facade topic and accepted diagnostics.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::ProducerDeliveryObserver as EngineDeliveryObserver;

use crate::{
    ErrorKind, KafkaError, RecordMetadata,
    bridge::producer_result::delivery::translate_delivery_result,
};

/// Private runtime-neutral observer shared by asynchronous and blocking APIs.
#[must_use = "dropping abandons observation without cancelling accepted producer work"]
pub(crate) struct ProducerDelivery {
    topic: Option<String>,
    observer: EngineDeliveryObserver,
    /// Advisory post-ownership mechanism fault retained for diagnostics only.
    ///
    /// It never replaces, predicts, or changes the observer's one terminal
    /// delivery result.
    accepted_diagnostic: Option<KafkaError>,
}

impl ProducerDelivery {
    pub(crate) const fn new(
        topic: String,
        observer: EngineDeliveryObserver,
        accepted_diagnostic: Option<KafkaError>,
    ) -> Self {
        Self {
            topic: Some(topic),
            observer,
            accepted_diagnostic,
        }
    }

    /// Blocks on the same terminal observer used by `Future::poll`.
    pub(crate) fn wait(self) -> Result<RecordMetadata, KafkaError> {
        let Some(topic) = self.topic else {
            return Err(already_observed());
        };
        translate_delivery_result(topic, self.observer.wait())
    }
}

impl Future for ProducerDelivery {
    type Output = Result<RecordMetadata, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.observer).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => match this.topic.take() {
                Some(topic) => Poll::Ready(translate_delivery_result(topic, result)),
                None => Poll::Ready(Err(already_observed())),
            },
        }
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(ErrorKind::State, "producer delivery was already observed")
}

impl fmt::Debug for ProducerDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerDelivery")
            .field("topic", &self.topic)
            .field("observer", &self.observer)
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish()
    }
}
