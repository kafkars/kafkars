//! Observation of generic destructive legacy configuration replacement.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{ErrorKind, KafkaError, admin::LegacyReplaceConfigResourcesResult};

use super::{
    engine::{Accepted, AdmissionError, Observer as EngineObserver},
    result::{translate_accepted_fault, translate_admission_error, translate_resource_observation},
};

pub(crate) type AdminLegacyReplaceConfigResourcesResult =
    Result<LegacyReplaceConfigResourcesResult, KafkaError>;

enum Inner {
    Accepted(EngineObserver),
    Ready(Option<AdminLegacyReplaceConfigResourcesResult>),
}

/// Private named observer shared by generic async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminLegacyReplaceConfigResources {
    inner: Inner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminLegacyReplaceConfigResources {
    pub(crate) fn from_admission(admission: Result<Accepted, AdmissionError>) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: Inner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminLegacyReplaceConfigResourcesResult {
        match self.inner {
            Inner::Accepted(observer) => translate_resource_observation(observer.wait()),
            Inner::Ready(Some(result)) => result,
            Inner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminLegacyReplaceConfigResourcesResult) -> Self {
        Self {
            inner: Inner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }
}

impl Future for AdminLegacyReplaceConfigResources {
    type Output = AdminLegacyReplaceConfigResourcesResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            Inner::Accepted(observer) => Pin::new(observer)
                .poll(context)
                .map(translate_resource_observation),
            Inner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminLegacyReplaceConfigResources {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminLegacyReplaceConfigResources")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(
        ErrorKind::State,
        "LegacyReplaceConfigResources was already observed",
    )
}
