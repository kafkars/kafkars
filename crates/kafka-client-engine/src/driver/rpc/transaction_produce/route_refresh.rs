//! Core-authorized partition-route invalidation for transactional Produce.

use std::mem;

use kafka_client_core::ProducerBrokerFailureKind;
use kafka_driver::{Call, InvalidationDisposition, RouteFailureToken, RouteKind, SubmitError};

use crate::driver::DriverOwner;

use super::model::{
    RouteEvidence, TransactionProduceFailureKind, TransactionProduceTerminal,
    TransactionProduceTerminalFact,
};

pub(super) enum ProduceRouteRefresh {
    Unavailable,
    Queued(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
    Ready,
}

/// One explicit route-invalidation turn after core authorizes replacement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionProduceRouteRefreshPoll {
    Ready,
    Failed,
    Submitted,
    Pending,
}

impl ProduceRouteRefresh {
    pub(super) fn for_terminal(
        fact: TransactionProduceTerminalFact,
        evidence: &mut RouteEvidence,
    ) -> Self {
        if !is_routing_failure(fact) {
            return Self::Unavailable;
        }
        match evidence {
            RouteEvidence::Driver(token)
                if token.as_ref().map(RouteFailureToken::kind)
                    == Some(RouteKind::PartitionLeader) =>
            {
                token.take().map_or(Self::Unavailable, Self::Queued)
            }
            RouteEvidence::Driver(_) => Self::Unavailable,
            #[cfg(test)]
            RouteEvidence::Test(_) => Self::Unavailable,
        }
    }
}

impl TransactionProduceTerminal {
    /// Consumes the exact failed partition route only after core authorization.
    pub(crate) fn poll_route_refresh(
        &mut self,
        driver: &DriverOwner,
    ) -> TransactionProduceRouteRefreshPoll {
        match mem::replace(&mut self.route_refresh, ProduceRouteRefresh::Unavailable) {
            ProduceRouteRefresh::Unavailable => TransactionProduceRouteRefreshPoll::Failed,
            ProduceRouteRefresh::Ready => {
                self.route_refresh = ProduceRouteRefresh::Ready;
                TransactionProduceRouteRefreshPoll::Ready
            }
            ProduceRouteRefresh::Queued(route_token) => {
                match driver.driver.invalidate(route_token) {
                    Ok(call) => {
                        self.route_refresh = ProduceRouteRefresh::Active(call);
                        TransactionProduceRouteRefreshPoll::Submitted
                    }
                    Err(rejection) => {
                        let retryable = invalidation_rejection_is_retryable(rejection.reason());
                        let (_source, route_token) = rejection.into_parts();
                        if retryable {
                            self.route_refresh = ProduceRouteRefresh::Queued(route_token);
                            TransactionProduceRouteRefreshPoll::Pending
                        } else {
                            drop(route_token);
                            TransactionProduceRouteRefreshPoll::Failed
                        }
                    }
                }
            }
            ProduceRouteRefresh::Active(call) => match call.try_result() {
                Some(Ok(
                    InvalidationDisposition::Applied | InvalidationDisposition::IgnoredStale,
                )) => {
                    self.route_refresh = ProduceRouteRefresh::Ready;
                    TransactionProduceRouteRefreshPoll::Ready
                }
                Some(Ok(_) | Err(_)) => TransactionProduceRouteRefreshPoll::Failed,
                None => {
                    self.route_refresh = ProduceRouteRefresh::Active(call);
                    TransactionProduceRouteRefreshPoll::Pending
                }
            },
        }
    }
}

pub(super) const fn invalidation_rejection_is_retryable(reason: &SubmitError) -> bool {
    matches!(reason, SubmitError::Full)
}

const fn is_routing_failure(fact: TransactionProduceTerminalFact) -> bool {
    matches!(
        fact,
        TransactionProduceTerminalFact::AbortRequired { failure, .. }
            if matches!(
                failure.kind(),
                TransactionProduceFailureKind::Broker(failure)
                    if matches!(failure.kind(), ProducerBrokerFailureKind::Routing)
            )
    )
}
