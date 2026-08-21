//! Concrete observation of accepted or rejected generic `DescribeConfigs` work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    DescribeConfigsAccepted, DescribeConfigsAdmissionError,
    DescribeConfigsObserver as EngineDescribeConfigsObserver,
};

use crate::{ErrorKind, KafkaError, admin::DescribeConfigResourcesResult};

use super::admin_configs_result::{
    translate_accepted_fault, translate_admission_error, translate_resource_observation,
};

pub(crate) type AdminDescribeConfigResourcesResult =
    Result<DescribeConfigResourcesResult, KafkaError>;

enum AdminDescribeConfigResourcesInner {
    Accepted(EngineDescribeConfigsObserver),
    Ready(Option<AdminDescribeConfigResourcesResult>),
}

/// Private named observation shared by async and blocking generic facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminDescribeConfigResources {
    inner: AdminDescribeConfigResourcesInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminDescribeConfigResources {
    pub(crate) fn from_admission(
        admission: Result<DescribeConfigsAccepted, DescribeConfigsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: AdminDescribeConfigResourcesInner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminDescribeConfigResourcesResult {
        match self.inner {
            AdminDescribeConfigResourcesInner::Accepted(observer) => {
                translate_resource_observation(observer.wait())
            }
            AdminDescribeConfigResourcesInner::Ready(Some(result)) => result,
            AdminDescribeConfigResourcesInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminDescribeConfigResourcesResult) -> Self {
        Self {
            inner: AdminDescribeConfigResourcesInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }
}

impl Future for AdminDescribeConfigResources {
    type Output = AdminDescribeConfigResourcesResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminDescribeConfigResourcesInner::Accepted(observer) => Pin::new(observer)
                .poll(context)
                .map(translate_resource_observation),
            AdminDescribeConfigResourcesInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminDescribeConfigResources {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDescribeConfigResources")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(ErrorKind::State, "DescribeConfigs was already observed")
}
