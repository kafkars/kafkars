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
use crate::bridge::admin::AdminEngine;

use super::{
    TransactionalProducerEngine,
    result::{translate_accepted_fault, translate_admission_error, translate_observation},
};

pub(crate) type TransactionInitializationResult = Result<TransactionalProducerEngine, KafkaError>;

#[expect(
    clippy::large_enum_variant,
    reason = "the single-shot operation retains its exact observer or terminal owner inline without another allocation"
)]
enum TransactionInitializationInner {
    Accepted(EngineObserver),
    Ready(Option<TransactionInitializationResult>),
}

/// Private single observer shared by asynchronous and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted initialization"]
pub(crate) struct TransactionInitialization {
    inner: TransactionInitializationInner,
    accepted_diagnostic: Option<KafkaError>,
    admin: Option<AdminEngine>,
}

impl TransactionInitialization {
    pub(crate) fn from_admission(
        admission: Result<
            TransactionInitializationAccepted,
            TransactionInitializationAdmissionError,
        >,
        admin: AdminEngine,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: TransactionInitializationInner::Accepted(accepted.into_observer()),
                admin: Some(admin),
            },
            Err(error) => Self::ready(Err(translate_admission_error(&error))),
        }
    }

    pub(crate) fn wait(mut self) -> TransactionInitializationResult {
        match self.inner {
            TransactionInitializationInner::Accepted(observer) => translate_observation(
                observer.wait(),
                self.admin
                    .take()
                    .unwrap_or_else(|| unreachable!("accepted initialization retains admin")),
            ),
            TransactionInitializationInner::Ready(Some(result)) => result,
            TransactionInitializationInner::Ready(None) => Err(super::result::already_observed()),
        }
    }

    pub(crate) fn ready(result: TransactionInitializationResult) -> Self {
        Self {
            inner: TransactionInitializationInner::Ready(Some(result)),
            accepted_diagnostic: None,
            admin: None,
        }
    }
}

impl Future for TransactionInitialization {
    type Output = TransactionInitializationResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            TransactionInitializationInner::Accepted(observer) => {
                let Poll::Ready(result) = Pin::new(observer).poll(context) else {
                    return Poll::Pending;
                };
                Poll::Ready(translate_observation(
                    result,
                    this.admin
                        .take()
                        .unwrap_or_else(|| unreachable!("accepted initialization retains admin")),
                ))
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
