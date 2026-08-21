//! Concrete observation of accepted or rejected Admin `DeleteConsumerGroups` work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    DeleteConsumerGroupsAccepted, DeleteConsumerGroupsAdmissionError,
    DeleteConsumerGroupsObserver as EngineObserver,
};

use crate::{ErrorKind, KafkaError, admin::DeleteConsumerGroupsResult};

use super::result::{translate_accepted_fault, translate_admission_error, translate_observation};

pub(crate) type AdminDeleteConsumerGroupsResult = Result<DeleteConsumerGroupsResult, KafkaError>;

enum AdminDeleteConsumerGroupsInner {
    Accepted(EngineObserver),
    Ready(Option<AdminDeleteConsumerGroupsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminDeleteConsumerGroups {
    inner: AdminDeleteConsumerGroupsInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminDeleteConsumerGroups {
    pub(crate) fn from_admission(
        admission: Result<DeleteConsumerGroupsAccepted, DeleteConsumerGroupsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: AdminDeleteConsumerGroupsInner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminDeleteConsumerGroupsResult {
        match self.inner {
            AdminDeleteConsumerGroupsInner::Accepted(observer) => {
                translate_observation(observer.wait())
            }
            AdminDeleteConsumerGroupsInner::Ready(Some(result)) => result,
            AdminDeleteConsumerGroupsInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminDeleteConsumerGroupsResult) -> Self {
        Self {
            inner: AdminDeleteConsumerGroupsInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }
}

impl Future for AdminDeleteConsumerGroups {
    type Output = AdminDeleteConsumerGroupsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminDeleteConsumerGroupsInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            AdminDeleteConsumerGroupsInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminDeleteConsumerGroups {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDeleteConsumerGroups")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(
        ErrorKind::State,
        "DeleteConsumerGroups was already observed",
    )
}
