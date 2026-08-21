//! Concrete observation of accepted topic-ID `DeleteTopics` work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    DeleteTopicsAccepted, DeleteTopicsAdmissionError,
    DeleteTopicsObserver as EngineDeleteTopicsObserver,
};

use crate::{ErrorKind, KafkaError, admin::BatchResult};

use super::{
    admin_delete_by_id_result::translate_observation,
    admin_delete_result::{translate_accepted_fault, translate_admission_error},
};

pub(crate) type AdminDeleteTopicsByIdResult = Result<BatchResult<[u8; 16], ()>, KafkaError>;

enum Inner {
    Accepted(EngineDeleteTopicsObserver),
    Ready(Option<AdminDeleteTopicsByIdResult>),
}

/// Private observer shared by async and blocking topic-ID facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminDeleteTopicsById {
    inner: Inner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminDeleteTopicsById {
    pub(crate) fn from_admission(
        admission: Result<DeleteTopicsAccepted, DeleteTopicsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: Inner::Accepted(accepted.into_observer()),
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminDeleteTopicsByIdResult {
        match self.inner {
            Inner::Accepted(observer) => translate_observation(observer.wait()),
            Inner::Ready(Some(result)) => result,
            Inner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminDeleteTopicsByIdResult) -> Self {
        Self {
            inner: Inner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }
}

impl Future for AdminDeleteTopicsById {
    type Output = AdminDeleteTopicsByIdResult;

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

impl fmt::Debug for AdminDeleteTopicsById {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDeleteTopicsById")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(ErrorKind::State, "DeleteTopicsById was already observed")
}
