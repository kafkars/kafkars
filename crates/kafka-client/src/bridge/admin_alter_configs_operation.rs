//! Concrete observation of accepted or rejected `IncrementalAlterConfigs` work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    IncrementalAlterConfigsAccepted, IncrementalAlterConfigsAdmissionError,
    IncrementalAlterConfigsObserver as EngineIncrementalAlterConfigsObserver,
};

use crate::{ErrorKind, KafkaError, admin::IncrementalAlterConfigsResult};

use super::admin_alter_configs_result::{
    translate_accepted_fault, translate_admission_error, translate_observation,
};

pub(crate) type AdminIncrementalAlterConfigsResult =
    Result<IncrementalAlterConfigsResult, KafkaError>;

enum AdminIncrementalAlterConfigsInner {
    Accepted(EngineIncrementalAlterConfigsObserver),
    Ready(Option<AdminIncrementalAlterConfigsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminIncrementalAlterConfigs {
    inner: AdminIncrementalAlterConfigsInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminIncrementalAlterConfigs {
    pub(crate) fn from_admission(
        admission: Result<IncrementalAlterConfigsAccepted, IncrementalAlterConfigsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: AdminIncrementalAlterConfigsInner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminIncrementalAlterConfigsResult {
        match self.inner {
            AdminIncrementalAlterConfigsInner::Accepted(observer) => {
                translate_observation(observer.wait())
            }
            AdminIncrementalAlterConfigsInner::Ready(Some(result)) => result,
            AdminIncrementalAlterConfigsInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminIncrementalAlterConfigsResult) -> Self {
        Self {
            inner: AdminIncrementalAlterConfigsInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }

    #[cfg(test)]
    pub(super) fn ready_for_test(result: AdminIncrementalAlterConfigsResult) -> Self {
        Self::ready(result)
    }
}

impl Future for AdminIncrementalAlterConfigs {
    type Output = AdminIncrementalAlterConfigsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminIncrementalAlterConfigsInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            AdminIncrementalAlterConfigsInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminIncrementalAlterConfigs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminIncrementalAlterConfigs")
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
