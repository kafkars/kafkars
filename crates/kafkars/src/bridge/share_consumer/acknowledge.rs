//! Runtime-neutral admission and observation of one share acknowledgement.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use kafka_client_engine::share::{
    ShareAcknowledgeOutcome as EngineOutcome,
    ShareAcknowledgementAdmissionErrorKind as EngineAdmissionErrorKind,
    ShareAcknowledgementObserver as EngineObserver,
};

use crate::{DeliveryStatus, ErrorKind, KafkaError};

use super::{
    ShareAcknowledgementError, ShareAcknowledgementResponse,
    acknowledge_result::{translate_failure, translate_observer_error},
    acknowledgement::ShareAcknowledgement as BridgeAcknowledgement,
    registration::ShareConsumerEngine,
};

/// Private sole terminal observer for one accepted acknowledgement.
#[must_use = "dropping observation does not cancel an accepted acknowledgement"]
pub(crate) struct ShareConsumerAcknowledge {
    inner: EngineObserver,
    advisory_error: Option<KafkaError>,
}

impl ShareConsumerEngine {
    pub(crate) fn try_acknowledge(
        &mut self,
        acknowledgement: BridgeAcknowledgement,
        timeout: Duration,
    ) -> Result<ShareConsumerAcknowledge, (BridgeAcknowledgement, KafkaError)> {
        match self
            .handle
            .try_acknowledge(acknowledgement.into_engine(), timeout)
        {
            Ok(accepted) => Ok(ShareConsumerAcknowledge {
                advisory_error: accepted.wake_failed().then(|| {
                    KafkaError::new(
                        ErrorKind::Internal,
                        "share acknowledgement was accepted but host wakeup failed",
                    )
                }),
                inner: accepted.into_observer(),
            }),
            Err(error) => {
                let semantic = translate_admission_kind(error.kind());
                Err((
                    BridgeAcknowledgement::from_engine(error.into_acknowledgement()),
                    semantic,
                ))
            }
        }
    }
}

impl ShareConsumerAcknowledge {
    pub(crate) fn advisory_error(&self) -> Option<KafkaError> {
        self.advisory_error.clone()
    }

    pub(crate) fn wait(self) -> Result<ShareAcknowledgementResponse, ShareAcknowledgementError> {
        translate_observation(self.inner.wait())
    }
}

impl Future for ShareConsumerAcknowledge {
    type Output = Result<ShareAcknowledgementResponse, ShareAcknowledgementError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(translate_observation)
    }
}

impl std::fmt::Debug for ShareConsumerAcknowledge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareConsumerAcknowledge")
            .field("advisory_error", &self.advisory_error)
            .finish_non_exhaustive()
    }
}

fn translate_observation(
    result: Result<EngineOutcome, kafka_client_engine::share::ShareAcknowledgementObserverError>,
) -> Result<ShareAcknowledgementResponse, ShareAcknowledgementError> {
    match result {
        Ok(EngineOutcome::Responded(response)) => {
            Ok(ShareAcknowledgementResponse::from_engine(response))
        }
        Ok(EngineOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

pub(super) fn translate_admission_kind(kind: EngineAdmissionErrorKind) -> KafkaError {
    let public = match kind {
        EngineAdmissionErrorKind::InvalidDeadline | EngineAdmissionErrorKind::InvalidRequest => {
            ErrorKind::Configuration
        }
        EngineAdmissionErrorKind::ForeignRegistry | EngineAdmissionErrorKind::Internal => {
            ErrorKind::Internal
        }
        EngineAdmissionErrorKind::Closed
        | EngineAdmissionErrorKind::Unavailable
        | EngineAdmissionErrorKind::StaleAcknowledgement => ErrorKind::State,
        EngineAdmissionErrorKind::Contended | EngineAdmissionErrorKind::Backpressure => {
            ErrorKind::Backpressure
        }
        EngineAdmissionErrorKind::DeadlineElapsed => ErrorKind::Timeout,
    };
    let error = KafkaError::new(public, format!("share acknowledgement rejected: {kind:?}"))
        .with_delivery_status(DeliveryStatus::NotSent);
    match kind {
        EngineAdmissionErrorKind::Contended | EngineAdmissionErrorKind::Backpressure => {
            error.with_safe_retry()
        }
        EngineAdmissionErrorKind::InvalidDeadline
        | EngineAdmissionErrorKind::ForeignRegistry
        | EngineAdmissionErrorKind::Closed
        | EngineAdmissionErrorKind::Unavailable
        | EngineAdmissionErrorKind::DeadlineElapsed
        | EngineAdmissionErrorKind::StaleAcknowledgement
        | EngineAdmissionErrorKind::InvalidRequest
        | EngineAdmissionErrorKind::Internal => error,
    }
}
