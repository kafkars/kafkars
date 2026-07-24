//! Concrete observation of accepted or rejected `CreatePartitions` work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    CreatePartitionsAccepted, CreatePartitionsAdmissionError,
    CreatePartitionsObserver as EngineCreatePartitionsObserver,
};

use crate::{ErrorKind, KafkaError, admin::BatchResult};

use super::admin_partitions_result::{
    translate_accepted_fault, translate_admission_error, translate_observation,
};

pub(crate) type AdminCreatePartitionsResult = Result<BatchResult<String, ()>, KafkaError>;

enum AdminCreatePartitionsInner {
    Accepted(EngineCreatePartitionsObserver),
    Ready(Option<AdminCreatePartitionsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminCreatePartitions {
    inner: AdminCreatePartitionsInner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminCreatePartitions {
    pub(crate) fn from_admission(
        admission: Result<CreatePartitionsAccepted, CreatePartitionsAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => {
                let accepted_diagnostic = accepted.fault().map(translate_accepted_fault);
                Self {
                    inner: AdminCreatePartitionsInner::Accepted(accepted.into_observer()),
                    accepted_diagnostic,
                }
            }
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminCreatePartitionsResult {
        match self.inner {
            AdminCreatePartitionsInner::Accepted(observer) => {
                translate_observation(observer.wait())
            }
            AdminCreatePartitionsInner::Ready(Some(result)) => result,
            AdminCreatePartitionsInner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminCreatePartitionsResult) -> Self {
        Self {
            inner: AdminCreatePartitionsInner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }

    #[cfg(test)]
    pub(super) fn ready_for_test(result: AdminCreatePartitionsResult) -> Self {
        Self::ready(result)
    }
}

impl Future for AdminCreatePartitions {
    type Output = AdminCreatePartitionsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminCreatePartitionsInner::Accepted(observer) => {
                Pin::new(observer).poll(context).map(translate_observation)
            }
            AdminCreatePartitionsInner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminCreatePartitions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminCreatePartitions")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(ErrorKind::State, "CreatePartitions was already observed")
}
