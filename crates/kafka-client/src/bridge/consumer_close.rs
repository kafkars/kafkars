//! Private runtime-neutral observation of one assigned-consumer close.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    AssignedConsumerCloseObserver as EngineCloseObserver, AssignedConsumerTryCloseAccepted,
    AssignedConsumerTryCloseError,
};

use crate::KafkaError;

use super::consumer_result::{
    translate_assigned_close_admission, translate_assigned_close_fault,
    translate_assigned_close_observation,
};

/// Private single observer shared by asynchronous and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted consumer close work"]
pub(crate) struct AssignedConsumerClose {
    observer: EngineCloseObserver,
    accepted_diagnostic: Option<KafkaError>,
}

impl AssignedConsumerClose {
    pub(crate) fn from_admission(
        admission: Result<AssignedConsumerTryCloseAccepted, AssignedConsumerTryCloseError>,
    ) -> Result<Self, KafkaError> {
        match admission {
            Ok(accepted) => Ok(Self {
                accepted_diagnostic: accepted.fault().map(translate_assigned_close_fault),
                observer: accepted.into_observer(),
            }),
            Err(error) => Err(translate_assigned_close_admission(error)),
        }
    }

    pub(crate) fn wait(self) -> Result<(), KafkaError> {
        self.observer
            .wait()
            .map_err(translate_assigned_close_observation)
    }
}

impl Future for AssignedConsumerClose {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.observer)
            .poll(context)
            .map(|result| result.map_err(translate_assigned_close_observation))
    }
}

impl fmt::Debug for AssignedConsumerClose {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AssignedConsumerClose")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}
