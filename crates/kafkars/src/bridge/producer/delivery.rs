//! Single-observer delivery bridge retaining facade topic and accepted diagnostics.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use kafka_client_engine::ProducerDeliveryObserver as EngineDeliveryObserver;

use crate::{
    CancellationOutcome, ErrorKind, KafkaError, RecordMetadata, TopicUuid,
    bridge::producer_result::{
        cancellation::{
            translate_cancellation_error, translate_cancellation_fault,
            translate_cancellation_outcome,
        },
        delivery::translate_delivery_result,
    },
};

/// Private runtime-neutral observer shared by asynchronous and blocking APIs.
#[must_use = "dropping abandons observation without cancelling accepted producer work"]
pub(crate) struct ProducerDelivery {
    topic: Option<Arc<str>>,
    topic_uuid: Option<TopicUuid>,
    create_timestamp: i64,
    serialized_key_size: Option<usize>,
    serialized_value_size: Option<usize>,
    observer: EngineDeliveryObserver,
    /// Advisory post-ownership mechanism fault retained for diagnostics only.
    ///
    /// It never replaces, predicts, or changes the observer's one terminal
    /// delivery result.
    accepted_diagnostic: Option<KafkaError>,
    /// Advisory wake fault after an authoritative cancellation decision.
    ///
    /// A committed cancellation outcome remains successful; this retained
    /// diagnostic is visible only through `Debug`.
    cancellation_diagnostic: Option<KafkaError>,
}

impl ProducerDelivery {
    pub(crate) const fn new(
        topic: Arc<str>,
        topic_uuid: Option<TopicUuid>,
        create_timestamp: i64,
        serialized_key_size: Option<usize>,
        serialized_value_size: Option<usize>,
        observer: EngineDeliveryObserver,
        accepted_diagnostic: Option<KafkaError>,
    ) -> Self {
        Self {
            topic: Some(topic),
            topic_uuid,
            create_timestamp,
            serialized_key_size,
            serialized_value_size,
            observer,
            accepted_diagnostic,
            cancellation_diagnostic: None,
        }
    }

    pub(crate) fn cancel(&mut self) -> Result<CancellationOutcome, KafkaError> {
        let accepted = self
            .observer
            .try_cancel()
            .map_err(|error| translate_cancellation_error(&error))?;
        if self.cancellation_diagnostic.is_none() {
            self.cancellation_diagnostic = accepted.fault().map(translate_cancellation_fault);
        }
        Ok(translate_cancellation_outcome(accepted.outcome()))
    }

    /// Blocks on the same terminal observer used by `Future::poll`.
    pub(crate) fn wait(self) -> Result<RecordMetadata, KafkaError> {
        let Some(topic) = self.topic else {
            return Err(already_observed());
        };
        translate_delivery_result(
            topic,
            self.topic_uuid,
            self.create_timestamp,
            self.serialized_key_size,
            self.serialized_value_size,
            self.observer.wait(),
        )
    }
}

impl Future for ProducerDelivery {
    type Output = Result<RecordMetadata, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.observer).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => match this.topic.take() {
                Some(topic) => Poll::Ready(translate_delivery_result(
                    topic,
                    this.topic_uuid,
                    this.create_timestamp,
                    this.serialized_key_size,
                    this.serialized_value_size,
                    result,
                )),
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
            .field("topic_uuid", &self.topic_uuid)
            .field("create_timestamp", &self.create_timestamp)
            .field("serialized_key_size", &self.serialized_key_size)
            .field("serialized_value_size", &self.serialized_value_size)
            .field("observer", &self.observer)
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .field("cancellation_diagnostic", &self.cancellation_diagnostic)
            .finish()
    }
}
