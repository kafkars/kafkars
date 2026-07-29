//! Concrete observation of accepted multi-consumer-group offset work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    ListConsumerGroupOffsetsAccepted, ListConsumerGroupOffsetsAdmissionError,
    ListConsumerGroupOffsetsObserver as EngineObserver,
};

use crate::{ErrorKind, KafkaError, admin::ListConsumerGroupsOffsetsResult};

use super::{
    list_groups_result::translate_observation,
    list_result::{translate_accepted_fault, translate_admission_error},
};

pub(crate) type AdminListConsumerGroupsOffsetsResult =
    Result<ListConsumerGroupsOffsetsResult, KafkaError>;

enum AdminListConsumerGroupsOffsetsInner {
    Accepted(EngineObserver),
    Ready(Option<AdminListConsumerGroupsOffsetsResult>),
}

/// Private observer shared by async and blocking plural facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminListConsumerGroupsOffsets {
    inner: AdminListConsumerGroupsOffsetsInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminListConsumerGroupsOffsets {
    pub(crate) fn from_admission(
        admission: Result<ListConsumerGroupOffsetsAccepted, ListConsumerGroupOffsetsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: AdminListConsumerGroupsOffsetsInner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminListConsumerGroupsOffsetsResult {
        match self.inner {
            AdminListConsumerGroupsOffsetsInner::Accepted(observer) => {
                translate_observation(observer.wait())
            }
            AdminListConsumerGroupsOffsetsInner::Ready(Some(result)) => result,
            AdminListConsumerGroupsOffsetsInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminListConsumerGroupsOffsetsResult) -> Self {
        Self {
            inner: AdminListConsumerGroupsOffsetsInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }
}

impl Future for AdminListConsumerGroupsOffsets {
    type Output = AdminListConsumerGroupsOffsetsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminListConsumerGroupsOffsetsInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            AdminListConsumerGroupsOffsetsInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminListConsumerGroupsOffsets {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminListConsumerGroupsOffsets")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(
        ErrorKind::State,
        "ListConsumerGroupsOffsets was already observed",
    )
}
