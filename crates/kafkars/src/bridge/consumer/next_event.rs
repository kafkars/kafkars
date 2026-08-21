//! Private runtime-neutral translation over assigned event observation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::AssignedConsumerNextEvent as EngineNextEvent;

use crate::{KafkaError, consumer::AssignedConsumerEvent};

use super::{
    event::translate_assigned_event, next_event_result::translate_assigned_consumer_next_event,
};

/// Private named event observer retaining the engine's unique consumer borrow.
pub(crate) struct AssignedConsumerNextEvent<'consumer> {
    inner: EngineNextEvent<'consumer>,
}

impl<'consumer> AssignedConsumerNextEvent<'consumer> {
    pub(super) const fn from_engine(inner: EngineNextEvent<'consumer>) -> Self {
        Self { inner }
    }

    pub(crate) fn wait(self) -> Result<Option<AssignedConsumerEvent>, KafkaError> {
        self.inner
            .wait()
            .map(|event| event.map(translate_assigned_event))
            .map_err(translate_assigned_consumer_next_event)
    }
}

impl Future for AssignedConsumerNextEvent<'_> {
    type Output = Result<Option<AssignedConsumerEvent>, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context).map(|result| {
            result
                .map(|event| event.map(translate_assigned_event))
                .map_err(translate_assigned_consumer_next_event)
        })
    }
}

impl std::fmt::Debug for AssignedConsumerNextEvent<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerNextEvent")
            .finish_non_exhaustive()
    }
}
