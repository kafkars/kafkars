//! Concrete observation of accepted or rejected `DeleteTopics` work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    DeleteTopicsAccepted, DeleteTopicsAdmissionError,
    DeleteTopicsObserver as EngineDeleteTopicsObserver,
};

use crate::{ErrorKind, KafkaError, admin::BatchResult};

use super::admin_delete_result::{
    translate_accepted_fault, translate_admission_error, translate_observation,
};

pub(crate) type AdminDeleteTopicsResult = Result<BatchResult<String, ()>, KafkaError>;

enum AdminDeleteTopicsInner {
    Accepted(EngineDeleteTopicsObserver),
    Ready(Option<AdminDeleteTopicsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminDeleteTopics {
    inner: AdminDeleteTopicsInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminDeleteTopics {
    pub(crate) fn from_admission(
        admission: Result<DeleteTopicsAccepted, DeleteTopicsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => {
                let accepted_diagnostic = accepted.fault().map(translate_accepted_fault);
                Self {
                    inner: AdminDeleteTopicsInner::Accepted(accepted.into_observer()),
                    accepted_diagnostic,
                }
            }
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminDeleteTopicsResult {
        match self.inner {
            AdminDeleteTopicsInner::Accepted(observer) => translate_observation(observer.wait()),
            AdminDeleteTopicsInner::Ready(Some(result)) => result,
            AdminDeleteTopicsInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminDeleteTopicsResult) -> Self {
        Self {
            inner: AdminDeleteTopicsInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }

    #[cfg(test)]
    pub(super) fn ready_for_test(result: AdminDeleteTopicsResult) -> Self {
        Self::ready(result)
    }
}

impl Future for AdminDeleteTopics {
    type Output = AdminDeleteTopicsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminDeleteTopicsInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            AdminDeleteTopicsInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminDeleteTopics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDeleteTopics")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(ErrorKind::State, "DeleteTopics was already observed")
}
