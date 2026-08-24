//! Bounded assignment-wide routing into broker-local `ShareFetch` session plans.

use kafka_client_core::{
    AssignmentGeneration, GroupAssignmentPartition, LiveGroupAssignment, Moment, ShareFetchBrokerId,
};

use crate::{clock::DeadlineCapture, driver::DriverOwner};

use super::{
    catalog::ShareMembershipCatalog,
    fetch_plan::{ShareBrokerSessionPlan, ShareBrokerSessionPlanError},
    fetch_route::{
        RoutedShareFetchPartition, ShareFetchPartitionRouteCall, ShareFetchPartitionRouteFailure,
        ShareFetchPartitionRouteFailureKind, ShareFetchPartitionRouteRequest,
    },
};

mod outcome;
pub(super) use outcome::ShareFetchRoutedAssignment;

const SHARE_FETCH_ROUTE_RETRY_TICKS: u64 = 100_000_000;

/// Exact assignment-wide metadata-routing ownership.
#[must_use = "share fetch routing must settle, recover, or be released"]
pub(super) struct ShareFetchRoutingOwner {
    generation: AssignmentGeneration,
    capture: DeadlineCapture,
    pending: Vec<ShareFetchPartitionRouteRequest>,
    active: Option<ShareFetchPartitionRouteCall>,
    retry_not_before: Option<kafka_client_core::Deadline>,
    routed: Vec<RoutedShareFetchPartition>,
    fault: Option<ShareFetchPartitionRouteFailure>,
}

impl ShareFetchRoutingOwner {
    pub(super) fn try_begin(
        catalog: &ShareMembershipCatalog,
        assignment: &LiveGroupAssignment,
        capture: DeadlineCapture,
    ) -> Result<Self, ShareFetchRoutingStartError> {
        let mut pending = Vec::new();
        pending
            .try_reserve_exact(assignment.partitions().len())
            .map_err(|_error| ShareFetchRoutingStartError::Allocation)?;
        for index in (0..assignment.partitions().len()).rev() {
            pending.push(
                ShareFetchPartitionRouteRequest::try_at(catalog, assignment, index, capture)
                    .map_err(ShareFetchRoutingStartError::Route)?,
            );
        }
        let mut routed = Vec::new();
        routed
            .try_reserve_exact(assignment.partitions().len())
            .map_err(|_error| ShareFetchRoutingStartError::Allocation)?;
        Ok(Self {
            generation: assignment.assignment_generation(),
            capture,
            pending,
            active: None,
            retry_not_before: None,
            routed,
            fault: None,
        })
    }

    pub(super) fn turn(&mut self, driver: &DriverOwner, now: Moment) -> ShareFetchRoutingTurn {
        if let Some(fault) = self.fault.as_ref() {
            return ShareFetchRoutingTurn::Faulted(fault.kind());
        }
        if let Some(not_before) = self.retry_not_before {
            if !not_before.is_elapsed_at(now) {
                return ShareFetchRoutingTurn::Blocked;
            }
            self.retry_not_before = None;
        }
        if let Some(active) = self.active.as_mut() {
            let Some(terminal) = active.try_terminal() else {
                return ShareFetchRoutingTurn::Blocked;
            };
            self.active = None;
            return self.settle_terminal(terminal, now);
        }
        let Some(request) = self.pending.pop() else {
            return ShareFetchRoutingTurn::Complete;
        };
        match ShareFetchPartitionRouteCall::submit(driver, request, now) {
            Ok(call) => self.active = Some(call),
            Err(failure)
                if failure.kind() == ShareFetchPartitionRouteFailureKind::Backpressured =>
            {
                self.pending.push(failure.into_request());
                return ShareFetchRoutingTurn::Blocked;
            }
            Err(failure) => self.fault = Some(failure),
        }
        ShareFetchRoutingTurn::Progress
    }

    pub(super) fn recover_after_driver_shutdown(&mut self) -> bool {
        let Some(active) = self.active.take() else {
            return false;
        };
        self.pending.push(active.recover_after_driver_shutdown());
        true
    }

    pub(super) const fn generation(&self) -> AssignmentGeneration {
        self.generation
    }

    pub(super) const fn deadline(&self) -> kafka_client_core::Deadline {
        self.capture.deadline()
    }

    pub(super) fn next_deadline(&self) -> kafka_client_core::Deadline {
        self.retry_not_before
            .unwrap_or_else(|| self.capture.deadline())
            .min(self.capture.deadline())
    }

    pub(super) const fn has_active_call(&self) -> bool {
        self.active.is_some()
    }

    pub(super) fn try_take_routed_assignment(
        &mut self,
        catalog: &ShareMembershipCatalog,
    ) -> Result<ShareFetchRoutedAssignment, ShareFetchRoutingPlanError> {
        if self.active.is_some()
            || self.retry_not_before.is_some()
            || !self.pending.is_empty()
            || self.fault.is_some()
        {
            return Err(ShareFetchRoutingPlanError::Incomplete);
        }
        let mut grouped: Vec<(
            ShareFetchBrokerId,
            Vec<(
                GroupAssignmentPartition,
                kafka_client_core::partitioning::TopicMetadataGeneration,
            )>,
        )> = Vec::new();
        if grouped.try_reserve_exact(self.routed.len()).is_err() {
            return Err(ShareFetchRoutingPlanError::Allocation);
        }
        for routed in &self.routed {
            let index = if let Some(index) = grouped
                .iter()
                .position(|(broker_id, _)| *broker_id == routed.broker_id())
            {
                index
            } else {
                grouped.push((routed.broker_id(), Vec::new()));
                grouped.len() - 1
            };
            if grouped[index].1.try_reserve(1).is_err() {
                return Err(ShareFetchRoutingPlanError::Allocation);
            }
            grouped[index]
                .1
                .push((routed.partition(), routed.metadata_generation()));
        }
        let mut plans = Vec::new();
        if plans.try_reserve_exact(grouped.len()).is_err() {
            return Err(ShareFetchRoutingPlanError::Allocation);
        }
        for (broker_id, partitions) in grouped {
            let plan = ShareBrokerSessionPlan::try_routed(catalog, broker_id, &partitions)
                .map_err(ShareFetchRoutingPlanError::Plan)?;
            plans.push(plan);
        }
        self.routed.clear();
        Ok(ShareFetchRoutedAssignment::new(
            self.generation,
            self.capture,
            plans,
        ))
    }

    pub(super) fn settle_terminal(
        &mut self,
        terminal: Result<RoutedShareFetchPartition, ShareFetchPartitionRouteFailure>,
        now: Moment,
    ) -> ShareFetchRoutingTurn {
        match terminal {
            Ok(routed) => self.routed.push(routed),
            Err(failure) if failure.kind().is_transient_metadata() => {
                if self.capture.deadline().is_elapsed_at(now) {
                    self.fault =
                        Some(failure.with_kind(ShareFetchPartitionRouteFailureKind::Deadline));
                } else {
                    self.pending.push(failure.into_request());
                    self.retry_not_before = Some(
                        now.checked_deadline_after(SHARE_FETCH_ROUTE_RETRY_TICKS)
                            .unwrap_or_else(|| self.capture.deadline())
                            .min(self.capture.deadline()),
                    );
                }
            }
            Err(failure) => self.fault = Some(failure),
        }
        ShareFetchRoutingTurn::Progress
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchRoutingTurn {
    Progress,
    Blocked,
    Complete,
    Faulted(ShareFetchPartitionRouteFailureKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchRoutingStartError {
    Allocation,
    Route(ShareFetchPartitionRouteFailureKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchRoutingPlanError {
    Incomplete,
    Allocation,
    Plan(ShareBrokerSessionPlanError),
}
