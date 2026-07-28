//! Concrete observation of accepted, rejected, or locally expired CreateAcls work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use crate::{DeliveryStatus, ErrorKind, KafkaError, admin::CreateAclsResult};

use super::{
    CreateAclsAdminRequest,
    engine::{Accepted, AdmissionError, Observer as EngineObserver, Request as EngineRequest},
    result::{
        PreparedCreateAclsOutcomes, translate_accepted_fault, translate_admission_error,
        translate_observation,
    },
};

pub(crate) type AdminCreateAclsResult = Result<CreateAclsResult, KafkaError>;

enum Inner {
    Accepted {
        observer: EngineObserver,
        prepared: Option<PreparedCreateAclsOutcomes>,
    },
    Ready(Option<AdminCreateAclsResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminCreateAcls {
    inner: Inner,
    accepted_diagnostic: Option<KafkaError>,
}

impl AdminCreateAcls {
    /// Reserves the public result vector before invoking engine admission.
    pub(crate) fn submit_with(
        request: CreateAclsAdminRequest,
        deadline: Instant,
        submit: impl FnOnce(EngineRequest, Duration) -> Result<Accepted, AdmissionError>,
    ) -> Self {
        let prepared = match PreparedCreateAclsOutcomes::try_new(request.binding_count()) {
            Ok(prepared) => prepared,
            Err(()) => return Self::result_capacity_rejected(),
        };
        let request = request.into_engine();
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Self::deadline_elapsed();
        }
        Self::from_admission(submit(request, remaining), prepared)
    }

    fn from_admission(
        admission: Result<Accepted, AdmissionError>,
        prepared: PreparedCreateAclsOutcomes,
    ) -> Self {
        match admission {
            Ok(accepted) => Self {
                accepted_diagnostic: accepted.fault().map(translate_accepted_fault),
                inner: Inner::Accepted {
                    observer: accepted.into_observer(),
                    prepared: Some(prepared),
                },
            },
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn deadline_elapsed() -> Self {
        Self::ready(Err(KafkaError::new(
            ErrorKind::Timeout,
            "CreateAcls deadline elapsed before submission",
        )
        .with_delivery_status(DeliveryStatus::NotSent)))
    }

    pub(crate) fn invalid_deadline() -> Self {
        Self::ready(Err(KafkaError::new(
            ErrorKind::Configuration,
            "CreateAcls deadline cannot be represented",
        )
        .with_delivery_status(DeliveryStatus::NotSent)))
    }

    fn result_capacity_rejected() -> Self {
        Self::ready(Err(KafkaError::new(
            ErrorKind::Backpressure,
            "CreateAcls public result capacity is unavailable",
        )
        .with_delivery_status(DeliveryStatus::NotSent)))
    }

    pub(crate) fn wait(self) -> AdminCreateAclsResult {
        match self.inner {
            Inner::Accepted { observer, prepared } => {
                translate_observation(observer.wait(), prepared)
            }
            Inner::Ready(Some(result)) => result,
            Inner::Ready(None) => Err(already_observed()),
        }
    }

    fn ready(result: AdminCreateAclsResult) -> Self {
        Self {
            inner: Inner::Ready(Some(result)),
            accepted_diagnostic: None,
        }
    }
}

impl Future for AdminCreateAcls {
    type Output = AdminCreateAclsResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            Inner::Accepted { observer, prepared } => match Pin::new(observer).poll(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    let translated = translate_observation(result, prepared.take());
                    this.inner = Inner::Ready(None);
                    Poll::Ready(translated)
                }
            },
            Inner::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
        }
    }
}

impl fmt::Debug for AdminCreateAcls {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminCreateAcls")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(ErrorKind::State, "CreateAcls was already observed")
}
