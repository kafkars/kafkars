//! Concrete observation of accepted or rejected Admin `DeleteRecords` work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    DeleteRecordsAccepted, DeleteRecordsAdmissionError, DeleteRecordsObserver as EngineObserver,
};

use crate::{ErrorKind, KafkaError, admin::DeleteRecordsResult};

use super::result::{translate_accepted_fault, translate_admission_error, translate_observation};

pub(crate) type AdminDeleteRecordsResult = Result<DeleteRecordsResult, KafkaError>;

enum AdminDeleteRecordsInner {
    Accepted(EngineObserver),
    Ready(Option<AdminDeleteRecordsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminDeleteRecords {
    inner: AdminDeleteRecordsInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminDeleteRecords {
    pub(crate) fn from_admission(
        admission: Result<DeleteRecordsAccepted, DeleteRecordsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: AdminDeleteRecordsInner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminDeleteRecordsResult {
        match self.inner {
            AdminDeleteRecordsInner::Accepted(observer) => translate_observation(observer.wait()),
            AdminDeleteRecordsInner::Ready(Some(result)) => result,
            AdminDeleteRecordsInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminDeleteRecordsResult) -> Self {
        Self {
            inner: AdminDeleteRecordsInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }
}

impl Future for AdminDeleteRecords {
    type Output = AdminDeleteRecordsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminDeleteRecordsInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            AdminDeleteRecordsInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminDeleteRecords {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDeleteRecords")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(ErrorKind::State, "DeleteRecords was already observed")
}
