//! Concrete observation of accepted or rejected topic-ID `DescribeTopics` work.

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

use super::{
    admin_topics_by_id_result::translate_observation,
    admin_topics_result::{translate_accepted_fault, translate_admission_error},
};

pub(crate) type AdminDescribeTopicsByIdResult =
    Result<BatchResult<[u8; 16], TopicDescription>, KafkaError>;

enum AdminDescribeTopicsByIdInner {
    Accepted(EngineDescribeTopicsObserver),
    Ready(Option<AdminDescribeTopicsByIdResult>),
}

/// Private named observation shared by async and blocking topic-ID facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminDescribeTopicsById {
    inner: AdminDescribeTopicsByIdInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminDescribeTopicsById {
    pub(crate) fn from_admission(
        admission: Result<DescribeTopicsAccepted, DescribeTopicsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => {
                let accepted_diagnostic = accepted.fault().map(translate_accepted_fault);
                Self {
                    inner: AdminDescribeTopicsByIdInner::Accepted(accepted.into_observer()),
                    accepted_diagnostic,
                }
            }
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminDescribeTopicsByIdResult {
        match self.inner {
            AdminDescribeTopicsByIdInner::Accepted(observer) => {
                translate_observation(observer.wait())
            }
            AdminDescribeTopicsByIdInner::Ready(Some(result)) => result,
            AdminDescribeTopicsByIdInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminDescribeTopicsByIdResult) -> Self {
        Self {
            inner: AdminDescribeTopicsByIdInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }
}

impl Future for AdminDescribeTopicsById {
    type Output = AdminDescribeTopicsByIdResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminDescribeTopicsByIdInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            AdminDescribeTopicsByIdInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminDescribeTopicsById {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDescribeTopicsById")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(
        ErrorKind::State,
        "topic-ID DescribeTopics was already observed",
    )
}
