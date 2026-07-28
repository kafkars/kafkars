//! Concrete observation of accepted or rejected group-description work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    DescribeConsumerGroupsAccepted, DescribeConsumerGroupsAdmissionError,
    DescribeConsumerGroupsObserver as EngineObserver,
};

use crate::{ErrorKind, KafkaError, admin::DescribeConsumerGroupsResult};

use super::result::{translate_accepted_fault, translate_admission_error, translate_observation};

pub(crate) type AdminDescribeConsumerGroupsResult =
    Result<DescribeConsumerGroupsResult, KafkaError>;

enum Inner {
    Accepted(EngineObserver),
    Ready(Option<AdminDescribeConsumerGroupsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminDescribeConsumerGroups {
    inner: Inner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminDescribeConsumerGroups {
    pub(crate) fn from_admission(
        admission: Result<DescribeConsumerGroupsAccepted, DescribeConsumerGroupsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: Inner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminDescribeConsumerGroupsResult {
        match self.inner {
            Inner::Accepted(observer) => translate_observation(observer.wait()),
            Inner::Ready(Some(result)) => result,
            Inner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminDescribeConsumerGroupsResult) -> Self {
        Self {
            inner: Inner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }
}

impl Future for AdminDescribeConsumerGroups {
    type Output = AdminDescribeConsumerGroupsResult;

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

impl fmt::Debug for AdminDescribeConsumerGroups {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDescribeConsumerGroups")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(
        ErrorKind::State,
        "DescribeConsumerGroups was already observed",
    )
}
