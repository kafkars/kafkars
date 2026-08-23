//! Exact membership-partition ownership during driver-authoritative leader routing.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, GroupAssignmentPartition, LiveGroupAssignment, Moment, ShareFetchBrokerId,
};

use crate::{
    clock::DeadlineCapture,
    driver::{
        DriverOwner, TopicPartitionCountAdmissionFailureKind, TopicPartitionCountFailure,
        TopicRouteViewCall,
    },
};

use super::catalog::ShareMembershipCatalog;

/// Exact assigned partition retained until metadata routing settles.
#[must_use = "a share partition route request must be submitted or released"]
pub(super) struct ShareFetchPartitionRouteRequest {
    assignment_generation: AssignmentGeneration,
    partition: GroupAssignmentPartition,
    topic: Arc<str>,
    kafka_topic_id: [u8; 16],
    capture: DeadlineCapture,
}

impl ShareFetchPartitionRouteRequest {
    pub(super) fn try_at(
        catalog: &ShareMembershipCatalog,
        assignment: &LiveGroupAssignment,
        index: usize,
        capture: DeadlineCapture,
    ) -> Result<Self, ShareFetchPartitionRouteFailureKind> {
        let partition = assignment
            .partitions()
            .get(index)
            .copied()
            .ok_or(ShareFetchPartitionRouteFailureKind::Unassigned)?;
        let identity = catalog
            .topic_identity(partition.topic_id())
            .ok_or(ShareFetchPartitionRouteFailureKind::UnknownTopic)?;
        Ok(Self {
            assignment_generation: assignment.assignment_generation(),
            partition,
            topic: Arc::clone(identity.name()),
            kafka_topic_id: identity.kafka_topic_id(),
            capture,
        })
    }

    pub(super) const fn assignment_generation(&self) -> AssignmentGeneration {
        self.assignment_generation
    }

    pub(super) const fn partition(&self) -> GroupAssignmentPartition {
        self.partition
    }

    pub(super) const fn capture(&self) -> DeadlineCapture {
        self.capture
    }
}

/// One accepted topic-view call retaining the exact assigned partition.
#[must_use = "an accepted share partition route must settle or recover"]
pub(super) struct ShareFetchPartitionRouteCall {
    request: Option<ShareFetchPartitionRouteRequest>,
    call: TopicRouteViewCall,
}

impl ShareFetchPartitionRouteCall {
    pub(super) fn submit(
        driver: &DriverOwner,
        request: ShareFetchPartitionRouteRequest,
        now: Moment,
    ) -> Result<Self, ShareFetchPartitionRouteFailure> {
        if request.capture.deadline().is_elapsed_at(now) {
            return Err(ShareFetchPartitionRouteFailure::new(
                request,
                ShareFetchPartitionRouteFailureKind::Deadline,
            ));
        }
        match TopicRouteViewCall::submit(
            driver,
            &request.topic,
            request.capture.operation_deadline().transport(),
        ) {
            Ok(call) => Ok(Self {
                request: Some(request),
                call,
            }),
            Err(error) => {
                let kind = if error.kind() == TopicPartitionCountAdmissionFailureKind::Full {
                    ShareFetchPartitionRouteFailureKind::Backpressured
                } else {
                    ShareFetchPartitionRouteFailureKind::DriverRejected
                };
                Err(ShareFetchPartitionRouteFailure::new(request, kind))
            }
        }
    }

    pub(super) fn try_terminal(
        &mut self,
    ) -> Option<Result<RoutedShareFetchPartition, ShareFetchPartitionRouteFailure>> {
        let terminal = self.call.try_terminal()?;
        let request = self
            .request
            .take()
            .unwrap_or_else(|| unreachable!("live route call retains its request"));
        Some(match terminal {
            Ok(view) if view.kafka_topic_id() != Some(request.kafka_topic_id) => {
                Err(ShareFetchPartitionRouteFailure::new(
                    request,
                    ShareFetchPartitionRouteFailureKind::TopicIdentityChanged,
                ))
            }
            Ok(view) => match view.leader_broker_id(request.partition.partition()) {
                Some(raw) => match ShareFetchBrokerId::try_from_raw(raw) {
                    Some(broker_id) => Ok(RoutedShareFetchPartition { request, broker_id }),
                    None => Err(ShareFetchPartitionRouteFailure::new(
                        request,
                        ShareFetchPartitionRouteFailureKind::InvalidBroker,
                    )),
                },
                None => Err(ShareFetchPartitionRouteFailure::new(
                    request,
                    ShareFetchPartitionRouteFailureKind::LeaderUnavailable,
                )),
            },
            Err(error) => Err(ShareFetchPartitionRouteFailure::new(
                request,
                ShareFetchPartitionRouteFailureKind::TopicView(error),
            )),
        })
    }

    pub(super) fn recover_after_driver_shutdown(mut self) -> ShareFetchPartitionRouteRequest {
        self.call.discard_after_driver_shutdown();
        self.request
            .take()
            .unwrap_or_else(|| unreachable!("unsettled route call retains its request"))
    }
}

/// Exact membership partition paired with the driver-observed leader.
#[must_use = "a routed share partition must enter a broker session or be released"]
pub(super) struct RoutedShareFetchPartition {
    request: ShareFetchPartitionRouteRequest,
    broker_id: ShareFetchBrokerId,
}

impl RoutedShareFetchPartition {
    pub(super) const fn broker_id(&self) -> ShareFetchBrokerId {
        self.broker_id
    }

    pub(super) const fn partition(&self) -> GroupAssignmentPartition {
        self.request.partition
    }

    pub(super) fn into_request(self) -> ShareFetchPartitionRouteRequest {
        self.request
    }
}

/// Route failure retaining exact assignment ownership.
#[must_use = "a failed share partition route must be retried or released"]
pub(super) struct ShareFetchPartitionRouteFailure {
    request: ShareFetchPartitionRouteRequest,
    kind: ShareFetchPartitionRouteFailureKind,
}

impl ShareFetchPartitionRouteFailure {
    const fn new(
        request: ShareFetchPartitionRouteRequest,
        kind: ShareFetchPartitionRouteFailureKind,
    ) -> Self {
        Self { request, kind }
    }

    pub(super) const fn kind(&self) -> ShareFetchPartitionRouteFailureKind {
        self.kind
    }

    pub(super) fn into_request(self) -> ShareFetchPartitionRouteRequest {
        self.request
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchPartitionRouteFailureKind {
    Unassigned,
    UnknownTopic,
    Deadline,
    Backpressured,
    DriverRejected,
    TopicIdentityChanged,
    LeaderUnavailable,
    InvalidBroker,
    TopicView(TopicPartitionCountFailure),
}
