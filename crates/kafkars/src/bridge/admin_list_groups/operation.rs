//! Concrete observation of accepted or rejected all-group listing work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    ListConsumerGroupsAccepted, ListConsumerGroupsAdmissionError,
    ListConsumerGroupsObserver as EngineObserver,
};

use crate::{ErrorKind, KafkaError, admin::ListGroupsResult};

use super::result::{translate_accepted_fault, translate_admission_error, translate_observation};

pub(crate) type AdminListGroupsResult = Result<ListGroupsResult, KafkaError>;

enum Inner {
    Accepted(EngineObserver),
    Ready(Option<AdminListGroupsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminListGroups {
    inner: Inner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminListGroups {
    pub(crate) fn from_admission(
        admission: Result<ListConsumerGroupsAccepted, ListConsumerGroupsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: Inner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminListGroupsResult {
        match self.inner {
            Inner::Accepted(observer) => translate_observation(observer.wait()),
            Inner::Ready(Some(result)) => result,
            Inner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminListGroupsResult) -> Self {
        Self {
            inner: Inner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }
}

impl Future for AdminListGroups {
    type Output = AdminListGroupsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            Inner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            Inner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminListGroups {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminListGroups")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(ErrorKind::State, "ListGroups was already observed")
}
