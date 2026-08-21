//! Concrete observation of accepted or locally rejected initialization.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    TransactionInitializationAccepted, TransactionInitializationAdmissionError,
    TransactionInitializationObserver as EngineObserver,
};

use crate::KafkaError;

use super::{
    TransactionalProducerEngine,
    result::{translate_accepted_fault, translate_admission_error, translate_observation},
};

pub(crate) type TransactionInitializationResult = Result<TransactionalProducerEngine, KafkaError>;

enum TransactionInitializationInner {
    Accepted(EngineObserver),
    Ready(Option<TransactionInitializationResult>),
}

/// Private single observer shared by asynchronous and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted initialization"]
pub(crate) struct TransactionInitialization {
    inner: TransactionInitializationInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl TransactionInitialization {
    pub(crate) fn from_admission(
        admission: Result<
            TransactionInitializationAccepted,
            TransactionInitializationAdmissionError,
        >,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: TransactionInitializationInner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(&error))),
        }
    }

    pub(crate) fn wait(self) -> TransactionInitializationResult {
        match self.inner {
            TransactionInitializationInner::Accepted(observer) => {
                translate_observation(observer.wait())
            }
            TransactionInitializationInner::Ready(Some(result)) => result,
            TransactionInitializationInner::Ready(None) => Err(super::result::already_observed()),
        }
    }

    pub(crate) fn ready(result: TransactionInitializationResult) -> Self {
        Self {
            inner: TransactionInitializationInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }
}

impl Future for TransactionInitialization {
    type Output = TransactionInitializationResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            TransactionInitializationInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            TransactionInitializationInner::Ready(result) => Poll::Ready(
                result
                    .take()
                    .unwrap_or_else(|| Err(super::result::already_observed())),
            ),
        }
    }
}

impl std::fmt::Debug for TransactionInitialization {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransactionInitialization")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}
