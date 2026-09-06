//! Ownership-preserving Fetch route admission after one broker failure.
#![allow(
    clippy::result_large_err,
    reason = "causal admission returns the exact Fetch request and failure token"
)]

use kafka_client_core::FetchFailure;
use kafka_driver::{RouteFailureToken, RouteKind, SubmitError, TopicName};

use super::{
    super::super::DriverOwner, admission::PartitionFetchRequest, route::BrokerFetchRouteCall,
};

/// Exact broker failure authority retained until causal metadata admission.
#[must_use = "a broker failure token must be admitted or deliberately discarded"]
#[derive(Debug)]
pub(crate) struct BrokerRouteFailureToken(RouteFailureToken);

impl BrokerRouteFailureToken {
    pub(crate) fn from_driver(token: Option<RouteFailureToken>) -> Option<Self> {
        token
            .filter(|token| token.kind() == RouteKind::Broker)
            .map(Self)
    }
}

/// Causal route admission failure retaining every retryable owner.
#[must_use = "causal route admission failure ownership must be settled or recovered"]
pub(crate) struct BrokerFetchCausalRouteFailure {
    request: PartitionFetchRequest,
    kind: BrokerFetchCausalRouteFailureKind,
}

impl BrokerFetchCausalRouteFailure {
    pub(crate) fn into_parts(self) -> (PartitionFetchRequest, BrokerFetchCausalRouteFailureKind) {
        (self.request, self.kind)
    }
}

/// Exact causal route admission outcome before driver ownership.
#[derive(Debug)]
pub(crate) enum BrokerFetchCausalRouteFailureKind {
    Backpressured(BrokerRouteFailureToken),
    Terminal(FetchFailure),
}

impl BrokerFetchRouteCall {
    pub(crate) fn submit_after_failure(
        driver: &DriverOwner,
        request: PartitionFetchRequest,
        failure: BrokerRouteFailureToken,
    ) -> Result<Self, BrokerFetchCausalRouteFailure> {
        let topic = match TopicName::new(request.topic().to_owned()) {
            Ok(topic) => topic,
            Err(_error) => {
                return Err(BrokerFetchCausalRouteFailure {
                    request,
                    kind: BrokerFetchCausalRouteFailureKind::Terminal(FetchFailure::DriverRejected),
                });
            }
        };
        let deadline = request.operation_deadline().transport();
        match driver
            .driver
            .topic_view_after_failure(topic.clone(), failure.0, deadline)
        {
            Ok(call) => Ok(Self::from_admitted(request, topic, call)),
            Err(rejection) if matches!(rejection.reason(), SubmitError::Full) => {
                let (_reason, token) = rejection.into_parts();
                Err(BrokerFetchCausalRouteFailure {
                    request,
                    kind: BrokerFetchCausalRouteFailureKind::Backpressured(
                        BrokerRouteFailureToken(token),
                    ),
                })
            }
            Err(rejection) => {
                let (_reason, _token) = rejection.into_parts();
                Err(BrokerFetchCausalRouteFailure {
                    request,
                    kind: BrokerFetchCausalRouteFailureKind::Terminal(FetchFailure::DriverRejected),
                })
            }
        }
    }
}
