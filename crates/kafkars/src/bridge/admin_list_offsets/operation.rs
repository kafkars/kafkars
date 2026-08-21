//! Concrete observation of accepted or rejected Admin `ListOffsets` work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    AdminListOffsetsAccepted, AdminListOffsetsAdmissionError,
    AdminListOffsetsObserver as EngineObserver,
};

use crate::{ErrorKind, KafkaError, admin::ListOffsetsResult};

use super::result::{translate_accepted_fault, translate_admission_error, translate_observation};

pub(crate) type AdminListOffsetsResult = Result<ListOffsetsResult, KafkaError>;

enum AdminListOffsetsInner {
    Accepted(EngineObserver),
    Ready(Option<AdminListOffsetsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminListOffsets {
    inner: AdminListOffsetsInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminListOffsets {
    pub(crate) fn from_admission(
        admission: Result<AdminListOffsetsAccepted, AdminListOffsetsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: AdminListOffsetsInner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminListOffsetsResult {
        match self.inner {
            AdminListOffsetsInner::Accepted(observer) => translate_observation(observer.wait()),
            AdminListOffsetsInner::Ready(Some(result)) => result,
            AdminListOffsetsInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminListOffsetsResult) -> Self {
        Self {
            inner: AdminListOffsetsInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }
}

impl Future for AdminListOffsets {
    type Output = AdminListOffsetsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminListOffsetsInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            AdminListOffsetsInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminListOffsets {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminListOffsets")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(ErrorKind::State, "ListOffsets was already observed")
}
