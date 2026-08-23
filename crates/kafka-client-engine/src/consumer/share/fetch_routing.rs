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

/// Exact assignment-wide metadata-routing ownership.
#[must_use = "share fetch routing must settle, recover, or be released"]
pub(super) struct ShareFetchRoutingOwner {
    generation: AssignmentGeneration,
    capture: DeadlineCapture,
    pending: Vec<ShareFetchPartitionRouteRequest>,
    active: Option<ShareFetchPartitionRouteCall>,
    routed: Vec<RoutedShareFetchPartition>,
    fault: Option<ShareFetchPartitionRouteFailure>,
}

/// Completed broker-local plans retaining the original assignment boundary.
#[must_use = "routed share assignment must open its broker sessions or be released"]
pub(super) struct ShareFetchRoutedAssignment {
    generation: AssignmentGeneration,
    capture: DeadlineCapture,
    plans: Vec<ShareBrokerSessionPlan>,
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
            routed,
            fault: None,
        })
    }

    pub(super) fn turn(&mut self, driver: &DriverOwner, now: Moment) -> ShareFetchRoutingTurn {
        if let Some(fault) = self.fault.as_ref() {
            return ShareFetchRoutingTurn::Faulted(fault.kind());
        }
        if let Some(active) = self.active.as_mut() {
            let Some(terminal) = active.try_terminal() else {
                return ShareFetchRoutingTurn::Blocked;
            };
            self.active = None;
            match terminal {
                Ok(routed) => self.routed.push(routed),
                Err(failure) => self.fault = Some(failure),
            }
            return ShareFetchRoutingTurn::Progress;
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

    pub(super) const fn has_active_call(&self) -> bool {
        self.active.is_some()
    }

    pub(super) fn try_take_routed_assignment(
        &mut self,
        catalog: &ShareMembershipCatalog,
    ) -> Result<ShareFetchRoutedAssignment, ShareFetchRoutingPlanError> {
        if self.active.is_some() || !self.pending.is_empty() || self.fault.is_some() {
            return Err(ShareFetchRoutingPlanError::Incomplete);
        }
        let mut grouped: Vec<(ShareFetchBrokerId, Vec<GroupAssignmentPartition>)> = Vec::new();
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
            grouped[index].1.push(routed.partition());
        }
        let mut plans = Vec::new();
        if plans.try_reserve_exact(grouped.len()).is_err() {
            return Err(ShareFetchRoutingPlanError::Allocation);
        }
        for (broker_id, partitions) in grouped {
            let plan = ShareBrokerSessionPlan::try_initial(catalog, broker_id, &partitions)
                .map_err(ShareFetchRoutingPlanError::Plan)?;
            plans.push(plan);
        }
        self.routed.clear();
        Ok(ShareFetchRoutedAssignment {
            generation: self.generation,
            capture: self.capture,
            plans,
        })
    }
}

impl ShareFetchRoutedAssignment {
    pub(super) const fn generation(&self) -> AssignmentGeneration {
        self.generation
    }

    pub(super) const fn capture(&self) -> DeadlineCapture {
        self.capture
    }

    pub(super) fn into_plans(self) -> Vec<ShareBrokerSessionPlan> {
        self.plans
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
