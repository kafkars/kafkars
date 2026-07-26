//! Concrete observation of accepted or rejected group-offset listing work.

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

use crate::{ErrorKind, KafkaError, admin::ListConsumerGroupOffsetsResult};

use super::admin_group_offsets_result::{
    translate_accepted_fault, translate_admission_error, translate_observation,
};

pub(crate) type AdminListConsumerGroupOffsetsResult =
    Result<ListConsumerGroupOffsetsResult, KafkaError>;

enum AdminListConsumerGroupOffsetsInner {
    Accepted(EngineObserver),
    Ready(Option<AdminListConsumerGroupOffsetsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminListConsumerGroupOffsets {
    inner: AdminListConsumerGroupOffsetsInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminListConsumerGroupOffsets {
    pub(crate) fn from_admission(
        admission: Result<ListConsumerGroupOffsetsAccepted, ListConsumerGroupOffsetsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: AdminListConsumerGroupOffsetsInner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminListConsumerGroupOffsetsResult {
        match self.inner {
            AdminListConsumerGroupOffsetsInner::Accepted(observer) => {
                translate_observation(observer.wait())
            }
            AdminListConsumerGroupOffsetsInner::Ready(Some(result)) => result,
            AdminListConsumerGroupOffsetsInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminListConsumerGroupOffsetsResult) -> Self {
        Self {
            inner: AdminListConsumerGroupOffsetsInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }

    #[cfg(test)]
    pub(super) fn ready_for_test(result: AdminListConsumerGroupOffsetsResult) -> Self {
        Self::ready(result)
    }
}

impl Future for AdminListConsumerGroupOffsets {
    type Output = AdminListConsumerGroupOffsetsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminListConsumerGroupOffsetsInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            AdminListConsumerGroupOffsetsInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminListConsumerGroupOffsets {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminListConsumerGroupOffsets")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(
        ErrorKind::State,
        "ListConsumerGroupOffsets was already observed",
    )
}
