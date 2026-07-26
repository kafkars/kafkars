//! Concrete observation of accepted or rejected group-offset alteration work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    AlterConsumerGroupOffsetsAccepted, AlterConsumerGroupOffsetsAdmissionError,
    AlterConsumerGroupOffsetsObserver as EngineObserver,
};

use crate::{ErrorKind, KafkaError, admin::AlterConsumerGroupOffsetsResult};

use super::alter_result::{
    translate_accepted_fault, translate_admission_error, translate_observation,
};

pub(crate) type AdminAlterConsumerGroupOffsetsResult =
    Result<AlterConsumerGroupOffsetsResult, KafkaError>;

enum AdminAlterConsumerGroupOffsetsInner {
    Accepted(EngineObserver),
    Ready(Option<AdminAlterConsumerGroupOffsetsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminAlterConsumerGroupOffsets {
    inner: AdminAlterConsumerGroupOffsetsInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminAlterConsumerGroupOffsets {
    pub(crate) fn from_admission(
        admission: Result<
            AlterConsumerGroupOffsetsAccepted,
            AlterConsumerGroupOffsetsAdmissionError,
        >,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: AdminAlterConsumerGroupOffsetsInner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(&error))),
        }
    }

    pub(crate) fn wait(self) -> AdminAlterConsumerGroupOffsetsResult {
        match self.inner {
            AdminAlterConsumerGroupOffsetsInner::Accepted(observer) => {
                translate_observation(observer.wait())
            }
            AdminAlterConsumerGroupOffsetsInner::Ready(Some(result)) => result,
            AdminAlterConsumerGroupOffsetsInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminAlterConsumerGroupOffsetsResult) -> Self {
        Self {
            inner: AdminAlterConsumerGroupOffsetsInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }

    #[cfg(test)]
    pub(super) fn ready_for_test(result: AdminAlterConsumerGroupOffsetsResult) -> Self {
        Self::ready(result)
    }
}

impl Future for AdminAlterConsumerGroupOffsets {
    type Output = AdminAlterConsumerGroupOffsetsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminAlterConsumerGroupOffsetsInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            AdminAlterConsumerGroupOffsetsInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminAlterConsumerGroupOffsets {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminAlterConsumerGroupOffsets")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(
        ErrorKind::State,
        "AlterConsumerGroupOffsets was already observed",
    )
}
