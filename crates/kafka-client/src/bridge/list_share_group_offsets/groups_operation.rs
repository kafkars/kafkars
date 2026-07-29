//! Concrete observation of accepted multi-ShareGroup offset work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{ErrorKind, KafkaError, admin::ListShareGroupsOffsetsResult};

use super::{
    engine::{Accepted, AdmissionError, Observer as EngineObserver},
    groups_result::translate_observation,
    result::{translate_accepted_fault, translate_admission_error},
};

pub(crate) type AdminListShareGroupsOffsetsResult =
    Result<ListShareGroupsOffsetsResult, KafkaError>;

enum Inner {
    Accepted(EngineObserver),
    Ready(Option<AdminListShareGroupsOffsetsResult>),
}

/// Private observer shared by async and blocking plural facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminListShareGroupsOffsets {
    inner: Inner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminListShareGroupsOffsets {
    pub(crate) fn from_admission(admission: Result<Accepted, AdmissionError>) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: Inner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminListShareGroupsOffsetsResult {
        match self.inner {
            Inner::Accepted(observer) => translate_observation(observer.wait()),
            Inner::Ready(Some(result)) => result,
            Inner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminListShareGroupsOffsetsResult) -> Self {
        Self {
            inner: Inner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }
}

impl Future for AdminListShareGroupsOffsets {
    type Output = AdminListShareGroupsOffsetsResult;

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

impl fmt::Debug for AdminListShareGroupsOffsets {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminListShareGroupsOffsets")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(
        ErrorKind::State,
        "ListShareGroupsOffsets was already observed",
    )
}
