//! Causal metadata-refresh ownership for a settled Produce route failure.

use std::mem;

use kafka_client_core::{
    BatchExecutionId, Deadline, DeliveryStatus, Moment, ProducerBrokerFailureKind, ProducerInput,
};
use kafka_driver::{RouteFailureToken, RouteKind};

use crate::{
    clock::OperationDeadline,
    driver::{DriverOwner, TopicRouteViewCall},
};

pub(super) enum ProduceRouteRefresh {
    None,
    Refreshed,
    DeadlineElapsed,
    Unavailable,
    Queued(RouteFailureToken),
    Rejected(RouteFailureToken),
    Active(TopicRouteViewCall),
    #[cfg(test)]
    SubmitForTest,
    #[cfg(test)]
    PendingForTest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProduceRouteRefreshPoll {
    Ready,
    Failed,
    Submitted,
    Pending,
}

impl ProduceRouteRefresh {
    #[cfg(test)]
    pub(super) fn from_input(
        input: ProducerInput,
        route_token: &mut Option<RouteFailureToken>,
    ) -> Self {
        Self::from_required(needs_route_refresh(input), route_token)
    }

    pub(super) fn from_required(
        required: bool,
        route_token: &mut Option<RouteFailureToken>,
    ) -> Self {
        if !required {
            return Self::None;
        }
        if route_token.as_ref().map(RouteFailureToken::kind) == Some(required_route_kind()) {
            route_token.take().map_or(Self::Unavailable, Self::Queued)
        } else {
            Self::Unavailable
        }
    }

    #[cfg(test)]
    pub(super) const fn submit_for_test() -> Self {
        Self::SubmitForTest
    }

    pub(super) const fn deadline(&self, deadline: Deadline) -> Option<Deadline> {
        match self {
            Self::Queued(_) | Self::Active(_) => Some(deadline),
            #[cfg(test)]
            Self::SubmitForTest | Self::PendingForTest => Some(deadline),
            Self::None
            | Self::Refreshed
            | Self::DeadlineElapsed
            | Self::Unavailable
            | Self::Rejected(_) => None,
        }
    }

    pub(super) fn poll(
        &mut self,
        driver: &DriverOwner,
        topic: &str,
        deadline: OperationDeadline,
        execution: BatchExecutionId,
        input: &mut ProducerInput,
        now: Moment,
    ) -> ProduceRouteRefreshPoll {
        if self
            .deadline(deadline.core())
            .is_some_and(|deadline| deadline.is_elapsed_at(now))
        {
            *self = Self::DeadlineElapsed;
            mark_route_refresh_deadline_elapsed(input, execution, now);
            return ProduceRouteRefreshPoll::Ready;
        }
        match mem::replace(self, Self::None) {
            Self::None => ProduceRouteRefreshPoll::Ready,
            Self::Refreshed => {
                *self = Self::Refreshed;
                mark_route_refreshed(input);
                ProduceRouteRefreshPoll::Ready
            }
            Self::DeadlineElapsed => {
                *self = Self::DeadlineElapsed;
                mark_route_refresh_deadline_elapsed(input, execution, now);
                ProduceRouteRefreshPoll::Ready
            }
            Self::Unavailable => ProduceRouteRefreshPoll::Failed,
            Self::Rejected(route_token) => {
                *self = Self::Rejected(route_token);
                ProduceRouteRefreshPoll::Failed
            }
            Self::Queued(route_token) => {
                match TopicRouteViewCall::submit_after_failure(
                    driver,
                    topic,
                    route_token,
                    deadline.transport(),
                ) {
                    Ok(call) => {
                        *self = Self::Active(call);
                        return ProduceRouteRefreshPoll::Submitted;
                    }
                    Err(rejection) => {
                        let retryable = rejection.is_retryable();
                        let route_token = rejection.into_token();
                        if retryable {
                            *self = Self::Queued(route_token);
                        } else {
                            *self = Self::Rejected(route_token);
                            return ProduceRouteRefreshPoll::Failed;
                        }
                    }
                }
                ProduceRouteRefreshPoll::Pending
            }
            Self::Active(mut call) => match call.try_terminal() {
                Some(Ok(_view)) => {
                    *self = Self::Refreshed;
                    mark_route_refreshed(input);
                    ProduceRouteRefreshPoll::Ready
                }
                Some(Err(_failure)) => ProduceRouteRefreshPoll::Failed,
                None => {
                    *self = Self::Active(call);
                    ProduceRouteRefreshPoll::Pending
                }
            },
            #[cfg(test)]
            Self::SubmitForTest => {
                *self = Self::PendingForTest;
                ProduceRouteRefreshPoll::Submitted
            }
            #[cfg(test)]
            Self::PendingForTest => {
                *self = Self::PendingForTest;
                ProduceRouteRefreshPoll::Pending
            }
        }
    }
}

pub(super) const fn required_route_kind() -> RouteKind {
    RouteKind::Broker
}

pub(super) fn needs_route_refresh(input: ProducerInput) -> bool {
    matches!(
        input,
        ProducerInput::BrokerFailed { failure, .. }
            if failure.kind() == ProducerBrokerFailureKind::Routing
    ) || matches!(
        input,
        ProducerInput::TransportFailed {
            failure,
            route_refreshed: false,
            ..
        } if failure.is_structurally_transient()
    )
}

fn mark_route_refresh_deadline_elapsed(
    input: &mut ProducerInput,
    execution: BatchExecutionId,
    now: Moment,
) {
    if !needs_route_refresh(*input) {
        return;
    }
    let delivery = match *input {
        ProducerInput::BrokerFailed { delivery, .. }
        | ProducerInput::TransportFailed { delivery, .. } => delivery,
        _ => DeliveryStatus::PossiblySent,
    };
    *input = ProducerInput::RouteRefreshDeadlineElapsed {
        execution,
        now,
        delivery,
    };
}

pub(super) fn mark_route_refreshed(input: &mut ProducerInput) {
    match input {
        ProducerInput::BrokerFailed {
            route_refreshed, ..
        }
        | ProducerInput::TransportFailed {
            route_refreshed, ..
        } => *route_refreshed = true,
        _ => {}
    }
}
