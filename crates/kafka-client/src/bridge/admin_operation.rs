//! Concrete runtime-neutral observation of accepted or immediately rejected admin work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    CreateTopicsAccepted, CreateTopicsAdmissionError,
    CreateTopicsObserver as EngineCreateTopicsObserver,
};

use crate::{ErrorKind, KafkaError, admin::BatchResult};

use super::admin_result::{
    translate_accepted_fault, translate_admission_error, translate_observation,
};

pub(crate) type AdminCreateTopicsResult = Result<BatchResult<String, ()>, KafkaError>;

enum AdminCreateTopicsInner {
    Accepted(EngineCreateTopicsObserver),
    Ready(Option<AdminCreateTopicsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminCreateTopics {
    inner: AdminCreateTopicsInner,
    /// Advisory post-admission mechanism fault retained for `Debug` only.
    ///
    /// It never replaces, predicts, or changes the observer's terminal result.
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminCreateTopics {
    pub(crate) fn from_admission(
        admission: Result<CreateTopicsAccepted, CreateTopicsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => {
                let accepted_diagnostic = accepted.fault().map(translate_accepted_fault);
                Self {
                    inner: AdminCreateTopicsInner::Accepted(accepted.into_observer()),
                    accepted_diagnostic,
                }
            }
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminCreateTopicsResult {
        match self.inner {
            AdminCreateTopicsInner::Accepted(observer) => translate_observation(observer.wait()),
            AdminCreateTopicsInner::Ready(Some(result)) => result,
            AdminCreateTopicsInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminCreateTopicsResult) -> Self {
        Self {
            inner: AdminCreateTopicsInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }

    #[cfg(test)]
    pub(super) fn ready_for_test(result: AdminCreateTopicsResult) -> Self {
        Self::ready(result)
    }
}

impl Future for AdminCreateTopics {
    type Output = AdminCreateTopicsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminCreateTopicsInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            AdminCreateTopicsInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminCreateTopics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminCreateTopics")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(ErrorKind::State, "CreateTopics was already observed")
}
