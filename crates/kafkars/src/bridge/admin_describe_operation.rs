//! Concrete observation of accepted or rejected `DescribeCluster` work.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use kafka_client_engine::{
    DescribeClusterAccepted, DescribeClusterAdmissionError,
    DescribeClusterObserver as EngineDescribeClusterObserver,
};

use crate::{ErrorKind, KafkaError, admin::ClusterDescription};

use super::admin_describe_result::{
    translate_accepted_fault, translate_admission_error, translate_observation,
};

pub(crate) type AdminDescribeClusterResult = Result<ClusterDescription, KafkaError>;

enum AdminDescribeClusterInner {
    Accepted(EngineDescribeClusterObserver),
    Ready(Option<AdminDescribeClusterResult>),
}

/// Private named observation shared by async and blocking facade paths.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub(crate) struct AdminDescribeCluster {
    inner: AdminDescribeClusterInner,
    accepted_diagnostic: Option<KafkaError>,
    expected_cluster_id: Option<Arc<str>>,
}

impl AdminDescribeCluster {
    pub(crate) fn from_admission(
        admission: Result<DescribeClusterAccepted, DescribeClusterAdmissionError>,
    ) -> Self {
        match admission {
            Ok(accepted) => {
                let accepted_diagnostic = accepted.fault().map(translate_accepted_fault);
                Self {
                    inner: AdminDescribeClusterInner::Accepted(accepted.into_observer()),
                    accepted_diagnostic,
                    expected_cluster_id: None,
                }
            }
            Err(error) => Self::ready(Err(translate_admission_error(error))),
        }
    }

    pub(crate) fn wait(self) -> AdminDescribeClusterResult {
        let result = match self.inner {
            AdminDescribeClusterInner::Accepted(observer) => translate_observation(observer.wait()),
            AdminDescribeClusterInner::Ready(Some(result)) => result,
            AdminDescribeClusterInner::Ready(None) => Err(already_observed()),
        };
        validate_cluster_id(result, self.expected_cluster_id.as_deref())
    }

    pub(crate) fn with_expected_cluster_id(mut self, expected: Option<Arc<str>>) -> Self {
        self.expected_cluster_id = expected;
        self
    }

    fn ready(result: AdminDescribeClusterResult) -> Self {
        Self {
            inner: AdminDescribeClusterInner::Ready(Some(result)),
            accepted_diagnostic: None,
            expected_cluster_id: None,
        }
    }

    #[cfg(test)]
    pub(super) fn ready_for_test(result: AdminDescribeClusterResult) -> Self {
        Self::ready(result)
    }
}

impl Future for AdminDescribeCluster {
    type Output = AdminDescribeClusterResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            AdminDescribeClusterInner::Accepted(observer) => {
                let expected = this.expected_cluster_id.as_deref();
                Pin::new(observer)
                    .poll(context)
                    .map(translate_observation)
                    .map(|result| validate_cluster_id(result, expected))
            }
            AdminDescribeClusterInner::Ready(result) => Poll::Ready(validate_cluster_id(
                result.take().unwrap_or_else(|| Err(already_observed())),
                this.expected_cluster_id.as_deref(),
            )),
        }
    }
}

fn validate_cluster_id(
    result: AdminDescribeClusterResult,
    expected: Option<&str>,
) -> AdminDescribeClusterResult {
    let description = result?;
    if expected.is_some_and(|expected| description.cluster_id() != expected) {
        return Err(KafkaError::new(
            ErrorKind::Identity,
            "broker cluster ID does not match the configured expectation",
        )
        .with_fatal_disposition());
    }
    Ok(description)
}

impl fmt::Debug for AdminDescribeCluster {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDescribeCluster")
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish_non_exhaustive()
    }
}

fn already_observed() -> KafkaError {
    KafkaError::new(ErrorKind::State, "DescribeCluster was already observed")
}
