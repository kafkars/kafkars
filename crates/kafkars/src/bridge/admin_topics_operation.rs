//! Concrete observation of accepted or rejected `DescribeTopics` work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    DescribeTopicsAccepted, DescribeTopicsAdmissionError,
    DescribeTopicsObserver as EngineDescribeTopicsObserver,
};

use crate::{
    ErrorKind, KafkaError,
    admin::{BatchResult, TopicDescription},
};

use super::admin_topics_result::{
    translate_accepted_fault, translate_admission_error, translate_observation,
};

pub(crate) type AdminDescribeTopicsResult =
    Result<BatchResult<String, TopicDescription>, KafkaError>;

enum AdminDescribeTopicsInner {
    Accepted(EngineDescribeTopicsObserver),
    Ready(Option<AdminDescribeTopicsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminDescribeTopics {
    inner: AdminDescribeTopicsInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminDescribeTopics {
    pub(crate) fn from_admission(
        admission: Result<DescribeTopicsAccepted, DescribeTopicsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => {
                let accepted_diagnostic = accepted.fault().map(translate_accepted_fault);
                Self {
                    inner: AdminDescribeTopicsInner::Accepted(accepted.into_observer()),
                    accepted_diagnostic,
                }
            }
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminDescribeTopicsResult {
        match self.inner {
            AdminDescribeTopicsInner::Accepted(observer) => translate_observation(observer.wait()),
            AdminDescribeTopicsInner::Ready(Some(result)) => result,
            AdminDescribeTopicsInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminDescribeTopicsResult) -> Self {
        Self {
            inner: AdminDescribeTopicsInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }

    #[cfg(test)]
    pub(super) fn ready_for_test(result: AdminDescribeTopicsResult) -> Self {
        Self::ready(result)
    }
}

impl Future for AdminDescribeTopics {
    type Output = AdminDescribeTopicsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminDescribeTopicsInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            AdminDescribeTopicsInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminDescribeTopics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDescribeTopics")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(ErrorKind::State, "DescribeTopics was already observed")
}
