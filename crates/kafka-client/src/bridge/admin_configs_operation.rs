//! Concrete observation of accepted or rejected `DescribeConfigs` work.

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

use crate::{ErrorKind, KafkaError, admin::DescribeConfigsResult};

use super::admin_configs_result::{
    translate_accepted_fault, translate_admission_error, translate_observation,
};

pub(crate) type AdminDescribeConfigsResult = Result<DescribeConfigsResult, KafkaError>;

enum AdminDescribeConfigsInner {
    Accepted(EngineDescribeConfigsObserver),
    Ready(Option<AdminDescribeConfigsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminDescribeConfigs {
    inner: AdminDescribeConfigsInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminDescribeConfigs {
    pub(crate) fn from_admission(
        admission: Result<DescribeConfigsAccepted, DescribeConfigsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: AdminDescribeConfigsInner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminDescribeConfigsResult {
        match self.inner {
            AdminDescribeConfigsInner::Accepted(observer) => translate_observation(observer.wait()),
            AdminDescribeConfigsInner::Ready(Some(result)) => result,
            AdminDescribeConfigsInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminDescribeConfigsResult) -> Self {
        Self {
            inner: AdminDescribeConfigsInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }

    #[cfg(test)]
    pub(super) fn ready_for_test(result: AdminDescribeConfigsResult) -> Self {
        Self::ready(result)
    }
}

impl Future for AdminDescribeConfigs {
    type Output = AdminDescribeConfigsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminDescribeConfigsInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            AdminDescribeConfigsInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminDescribeConfigs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDescribeConfigs")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(ErrorKind::State, "DescribeConfigs was already observed")
}
