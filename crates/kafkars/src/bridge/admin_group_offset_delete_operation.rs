//! Concrete observation of accepted or rejected group-offset deletion work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    DeleteConsumerGroupOffsetsAccepted, DeleteConsumerGroupOffsetsAdmissionError,
    DeleteConsumerGroupOffsetsObserver as EngineObserver,
};

use crate::{ErrorKind, KafkaError, admin::DeleteConsumerGroupOffsetsResult};

use super::admin_group_offset_delete_result::{
    translate_accepted_fault, translate_admission_error, translate_observation,
};

pub(crate) type AdminDeleteConsumerGroupOffsetsResult =
    Result<DeleteConsumerGroupOffsetsResult, KafkaError>;

enum AdminDeleteConsumerGroupOffsetsInner {
    Accepted(EngineObserver),
    Ready(Option<AdminDeleteConsumerGroupOffsetsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminDeleteConsumerGroupOffsets {
    inner: AdminDeleteConsumerGroupOffsetsInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminDeleteConsumerGroupOffsets {
    pub(crate) fn from_admission(
        admission: Result<
            DeleteConsumerGroupOffsetsAccepted,
            DeleteConsumerGroupOffsetsAdmissionError,
        >,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: AdminDeleteConsumerGroupOffsetsInner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminDeleteConsumerGroupOffsetsResult {
        match self.inner {
            AdminDeleteConsumerGroupOffsetsInner::Accepted(observer) => {
                translate_observation(observer.wait())
            }
            AdminDeleteConsumerGroupOffsetsInner::Ready(Some(result)) => result,
            AdminDeleteConsumerGroupOffsetsInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminDeleteConsumerGroupOffsetsResult) -> Self {
        Self {
            inner: AdminDeleteConsumerGroupOffsetsInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }

    #[cfg(test)]
    pub(super) fn ready_for_test(result: AdminDeleteConsumerGroupOffsetsResult) -> Self {
        Self::ready(result)
    }
}

impl Future for AdminDeleteConsumerGroupOffsets {
    type Output = AdminDeleteConsumerGroupOffsetsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminDeleteConsumerGroupOffsetsInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            AdminDeleteConsumerGroupOffsetsInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminDeleteConsumerGroupOffsets {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDeleteConsumerGroupOffsets")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(
        ErrorKind::State,
        "DeleteConsumerGroupOffsets was already observed",
    )
}
