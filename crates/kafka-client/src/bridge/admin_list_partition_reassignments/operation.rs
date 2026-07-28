//! Concrete observation of accepted or rejected reassignment-listing work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    ListPartitionReassignmentsAccepted, ListPartitionReassignmentsAdmissionError,
    ListPartitionReassignmentsObserver as EngineObserver,
};

use crate::{ErrorKind, KafkaError, admin::ListPartitionReassignmentsResult};

use super::result::{translate_accepted_fault, translate_admission_error, translate_observation};

pub(crate) type AdminListPartitionReassignmentsResult =
    Result<ListPartitionReassignmentsResult, KafkaError>;

enum AdminListPartitionReassignmentsInner {
    Accepted(EngineObserver),
    Ready(Option<AdminListPartitionReassignmentsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminListPartitionReassignments {
    inner: AdminListPartitionReassignmentsInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminListPartitionReassignments {
    pub(crate) fn from_admission(
        admission: Result<
            ListPartitionReassignmentsAccepted,
            ListPartitionReassignmentsAdmissionError,
        >,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: AdminListPartitionReassignmentsInner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminListPartitionReassignmentsResult {
        match self.inner {
            AdminListPartitionReassignmentsInner::Accepted(observer) => {
                translate_observation(observer.wait())
            }
            AdminListPartitionReassignmentsInner::Ready(Some(result)) => result,
            AdminListPartitionReassignmentsInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminListPartitionReassignmentsResult) -> Self {
        Self {
            inner: AdminListPartitionReassignmentsInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }
}

impl Future for AdminListPartitionReassignments {
    type Output = AdminListPartitionReassignmentsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminListPartitionReassignmentsInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            AdminListPartitionReassignmentsInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminListPartitionReassignments {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminListPartitionReassignments")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(
        ErrorKind::State,
        "ListPartitionReassignments was already observed",
    )
}
