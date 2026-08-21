//! Observation of accepted or rejected generic `IncrementalAlterConfigs` work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    IncrementalAlterConfigsAccepted, IncrementalAlterConfigsAdmissionError,
    IncrementalAlterConfigsObserver as EngineObserver,
};

use crate::{ErrorKind, KafkaError, admin::IncrementalAlterConfigResourcesResult};

use super::admin_alter_configs_result::{
    translate_accepted_fault, translate_admission_error, translate_resource_observation,
};

pub(crate) type AdminIncrementalAlterConfigResourcesResult =
    Result<IncrementalAlterConfigResourcesResult, KafkaError>;

enum AdminIncrementalAlterConfigResourcesInner {
    Accepted(EngineObserver),
    Ready(Option<AdminIncrementalAlterConfigResourcesResult>),
}

/// Private named observer shared by generic async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminIncrementalAlterConfigResources {
    inner: AdminIncrementalAlterConfigResourcesInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminIncrementalAlterConfigResources {
    pub(crate) fn from_admission(
        admission: Result<IncrementalAlterConfigsAccepted, IncrementalAlterConfigsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: AdminIncrementalAlterConfigResourcesInner::Accepted(
                    accepted.into_observer(),
                ),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminIncrementalAlterConfigResourcesResult {
        match self.inner {
            AdminIncrementalAlterConfigResourcesInner::Accepted(observer) => {
                translate_resource_observation(observer.wait())
            }
            AdminIncrementalAlterConfigResourcesInner::Ready(Some(result)) => result,
            AdminIncrementalAlterConfigResourcesInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminIncrementalAlterConfigResourcesResult) -> Self {
        Self {
            inner: AdminIncrementalAlterConfigResourcesInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }
}

impl Future for AdminIncrementalAlterConfigResources {
    type Output = AdminIncrementalAlterConfigResourcesResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminIncrementalAlterConfigResourcesInner::Accepted(observer) => Pin::new(observer)
                .poll(context)
                .map(translate_resource_observation),
            AdminIncrementalAlterConfigResourcesInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminIncrementalAlterConfigResources {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminIncrementalAlterConfigResources")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(
        ErrorKind::State,
        "IncrementalAlterConfigs was already observed",
    )
}
