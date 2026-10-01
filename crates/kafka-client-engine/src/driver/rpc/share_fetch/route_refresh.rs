//! Deadline-bounded causal metadata recovery of one failed broker-local share route.

use std::{mem, time::Instant};

use kafka_client_core::{Deadline, Moment, partitioning::TopicMetadataGeneration};
use kafka_driver::{RouteFailureToken, TopicName};

use crate::driver::{
    DriverOwner, TopicPartitionCountAdmissionFailureKind, TopicPartitionCountCall,
};

use super::route::ShareFetchRoute;

/// Exact failed route retained until post-outcome metadata permits session replacement.
#[must_use = "a failed ShareFetch route must settle causal recovery or be accepted"]
pub(crate) struct ShareFetchRouteRefresh {
    deadline: Deadline,
    transport_deadline: Instant,
    topic: TopicName,
    observed: Option<TopicMetadataGeneration>,
    state: ShareFetchRouteRefreshState,
}

enum ShareFetchRouteRefreshState {
    Queued(RouteFailureToken),
    Active(TopicPartitionCountCall),
    Ready,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareFetchRouteRefreshPoll {
    Progress,
    Pending,
    Ready,
    Failed,
}

impl ShareFetchRouteRefresh {
    pub(crate) fn try_new(
        route: ShareFetchRoute,
        deadline: Deadline,
        transport_deadline: Instant,
        topic: &str,
    ) -> Result<Self, ShareFetchRoute> {
        Self::try_new_inner(route, deadline, transport_deadline, topic, None)
    }

    pub(crate) fn try_new_with_metadata(
        route: ShareFetchRoute,
        deadline: Deadline,
        transport_deadline: Instant,
        topic: &str,
        observed: TopicMetadataGeneration,
    ) -> Result<Self, ShareFetchRoute> {
        Self::try_new_inner(route, deadline, transport_deadline, topic, Some(observed))
    }

    fn try_new_inner(
        route: ShareFetchRoute,
        deadline: Deadline,
        transport_deadline: Instant,
        topic: &str,
        observed: Option<TopicMetadataGeneration>,
    ) -> Result<Self, ShareFetchRoute> {
        let Ok(topic) = TopicName::new(topic.to_string()) else {
            return Err(route);
        };
        route.into_broker_token().map(|token| Self {
            deadline,
            transport_deadline,
            topic,
            observed,
            state: ShareFetchRouteRefreshState::Queued(token),
        })
    }

    pub(crate) fn poll(&mut self, driver: &DriverOwner, now: Moment) -> ShareFetchRouteRefreshPoll {
        if self.deadline.is_elapsed_at(now)
            && matches!(self.state, ShareFetchRouteRefreshState::Queued(_))
        {
            self.state = ShareFetchRouteRefreshState::Failed;
            return ShareFetchRouteRefreshPoll::Failed;
        }
        match mem::replace(&mut self.state, ShareFetchRouteRefreshState::Failed) {
            ShareFetchRouteRefreshState::Queued(token) => {
                match TopicPartitionCountCall::submit_after_outcome_retaining(
                    driver,
                    self.topic.clone(),
                    token,
                    self.transport_deadline,
                ) {
                    Ok(call) => {
                        self.state = ShareFetchRouteRefreshState::Active(call);
                        ShareFetchRouteRefreshPoll::Progress
                    }
                    Err((error, token))
                        if error.kind() == TopicPartitionCountAdmissionFailureKind::Full =>
                    {
                        self.state = ShareFetchRouteRefreshState::Queued(token);
                        ShareFetchRouteRefreshPoll::Pending
                    }
                    Err((_error, token)) => {
                        drop(token);
                        ShareFetchRouteRefreshPoll::Failed
                    }
                }
            }
            ShareFetchRouteRefreshState::Active(mut call) => match call.try_terminal() {
                None => {
                    self.state = ShareFetchRouteRefreshState::Active(call);
                    ShareFetchRouteRefreshPoll::Pending
                }
                Some(Ok(view))
                    if view.logical_partition_count > 0
                        && self
                            .observed
                            .is_none_or(|floor| view.metadata_generation > floor.get()) =>
                {
                    self.state = ShareFetchRouteRefreshState::Ready;
                    ShareFetchRouteRefreshPoll::Ready
                }
                Some(Ok(_) | Err(_)) => ShareFetchRouteRefreshPoll::Failed,
            },
            ShareFetchRouteRefreshState::Ready => {
                self.state = ShareFetchRouteRefreshState::Ready;
                ShareFetchRouteRefreshPoll::Ready
            }
            ShareFetchRouteRefreshState::Failed => ShareFetchRouteRefreshPoll::Failed,
        }
    }

    pub(crate) fn discard_after_driver_shutdown(&mut self) {
        if let ShareFetchRouteRefreshState::Active(call) =
            mem::replace(&mut self.state, ShareFetchRouteRefreshState::Ready)
        {
            call.discard_after_driver_shutdown();
        }
        self.state = ShareFetchRouteRefreshState::Ready;
    }
}
