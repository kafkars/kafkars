//! Post-driver-shutdown release and exact retained-owner accounting for group Fetch.

use kafka_client_core::{AssignmentEpoch, GroupPositionFence};

use crate::{
    consumer::assigned_event::AssignedConsumerEventRecovery, driver::FetchCompletionObservation,
};

use super::{
    model::{ClassicGroupFetchOwnerFault, ClassicGroupFetchOwnerFaultKind},
    owner::ClassicGroupFetchOwner,
};

/// Scalar snapshot of every group Fetch mechanism consumed after driver teardown.
#[must_use = "classic-group Fetch shutdown recovery must be inspected"]
#[derive(Debug)]
pub(in crate::consumer::group) struct ClassicGroupFetchShutdownRecovery {
    activation: Option<(GroupPositionFence, AssignmentEpoch)>,
    machine_assignment: Option<AssignmentEpoch>,
    effects: usize,
    raw_positions: usize,
    prepared_positions: usize,
    prepared: usize,
    timers: usize,
    position_calls: usize,
    fetch_calls: usize,
    fetch_deliveries: usize,
    fetch_bytes: usize,
    recovered_fetch_requests: usize,
    fetch_completion: Option<FetchCompletionObservation>,
    fetch_executor_faulted: bool,
    events: AssignedConsumerEventRecovery,
    owner_fault: Option<ClassicGroupFetchOwnerFaultKind>,
    reclaim_fault: Option<crate::consumer::fetch_execution::FetchExecutionError>,
    reclaim_faults: usize,
    reclaim_overflow: bool,
}

impl ClassicGroupFetchOwner {
    /// Reports exact pristine ownership before this Fetch owner has activated.
    pub(in crate::consumer::group) fn is_idle(&self) -> bool {
        self.machine.assignment_epoch().is_none() && self.unsettled() == 0
    }

    /// Counts every retained mechanism that must settle or be recovered.
    pub(in crate::consumer::group) fn unsettled(&self) -> usize {
        let (fetch_calls, fetch_deliveries, _fetch_bytes) = self.fetches.retained();
        let (event_claims, event_ready) = self.events.retained();
        usize::from(self.activation.is_some())
            .saturating_add(usize::from(self.seek.is_some()))
            .saturating_add(self.effects.len())
            .saturating_add(self.raw_position_deadlines.len())
            .saturating_add(self.pending_positions.len())
            .saturating_add(self.pending_fetches.len())
            .saturating_add(self.timers.timer_count())
            .saturating_add(fetch_calls)
            .saturating_add(self.positions.retained_positions())
            .saturating_add(fetch_deliveries)
            .saturating_add(event_claims)
            .saturating_add(event_ready)
            .saturating_add(usize::from(self.fault.is_some()))
            .saturating_add(self.reclaim_faults.len())
            .saturating_add(usize::from(self.reclaim_overflow.is_some()))
    }

    /// Consumes calls, store bytes, deliveries, events, and faults only after
    /// the unique embedded `DriverOwner` has been destroyed.
    pub(in crate::consumer::group) fn release_after_driver_shutdown(
        mut self,
    ) -> ClassicGroupFetchShutdownRecovery {
        self.settle_seek_driver_shutdown();
        let activation = self.activation.as_ref().map(|activation| {
            let binding = activation.binding();
            (binding.position_fence(), binding.assignment_epoch())
        });
        let machine_assignment = self.machine.assignment_epoch();
        let effects = self.effects.len();
        let raw_positions = self.raw_position_deadlines.len();
        let prepared_positions = self.pending_positions.len();
        let prepared = self.pending_fetches.len();
        let timers = self.timers.timer_count();
        let position_calls = self.positions.retained_positions();
        let (fetch_calls, fetch_deliveries, fetch_bytes) = self.fetches.retained();
        let owner_fault = self.fault.as_ref().map(ClassicGroupFetchOwnerFault::kind);
        let reclaim_fault = self
            .reclaim_faults
            .first()
            .or(self.reclaim_overflow.as_ref())
            .map(super::delivery::ClassicGroupFetchReclaimFault::error);
        let reclaim_faults = self.reclaim_faults.len();
        let reclaim_overflow = self.reclaim_overflow.is_some();
        let events = self.events.recover_after_driver_shutdown();
        let _position_completion = self
            .positions
            .release_position_calls_after_driver_shutdown();
        let fetch = self.fetches.release_fetch_executor_after_driver_shutdown();
        let fetch_executor_faulted = fetch.had_fault();
        let (requests, fetch_completion) = fetch.into_driver_recovery().into_parts();
        let recovered_fetch_requests = requests.len();
        drop(requests);
        ClassicGroupFetchShutdownRecovery {
            activation,
            machine_assignment,
            effects,
            raw_positions,
            prepared_positions,
            prepared,
            timers,
            position_calls,
            fetch_calls,
            fetch_deliveries,
            fetch_bytes,
            recovered_fetch_requests,
            fetch_completion,
            fetch_executor_faulted,
            events,
            owner_fault,
            reclaim_fault,
            reclaim_faults,
            reclaim_overflow,
        }
    }
}

impl ClassicGroupFetchShutdownRecovery {
    pub(in crate::consumer::group) const fn activation(
        &self,
    ) -> Option<(GroupPositionFence, AssignmentEpoch)> {
        self.activation
    }

    pub(in crate::consumer::group) const fn machine_assignment(&self) -> Option<AssignmentEpoch> {
        self.machine_assignment
    }

    pub(in crate::consumer::group) const fn effects(&self) -> usize {
        self.effects
    }

    pub(in crate::consumer::group) const fn prepared(&self) -> usize {
        self.prepared
    }

    pub(in crate::consumer::group) const fn prepared_positions(&self) -> usize {
        self.prepared_positions
    }

    pub(in crate::consumer::group) const fn raw_positions(&self) -> usize {
        self.raw_positions
    }

    pub(in crate::consumer::group) const fn position_calls(&self) -> usize {
        self.position_calls
    }

    pub(in crate::consumer::group) const fn timers(&self) -> usize {
        self.timers
    }

    pub(in crate::consumer::group) const fn fetch_retained(&self) -> (usize, usize, usize) {
        (self.fetch_calls, self.fetch_deliveries, self.fetch_bytes)
    }

    pub(in crate::consumer::group) const fn recovered_fetch_requests(&self) -> usize {
        self.recovered_fetch_requests
    }

    pub(in crate::consumer::group) const fn fetch_completion(
        &self,
    ) -> Option<FetchCompletionObservation> {
        self.fetch_completion
    }

    pub(in crate::consumer::group) const fn fetch_executor_faulted(&self) -> bool {
        self.fetch_executor_faulted
    }

    pub(in crate::consumer::group) const fn recovered_events(
        &self,
    ) -> AssignedConsumerEventRecovery {
        self.events
    }

    pub(in crate::consumer::group) const fn owner_fault(
        &self,
    ) -> Option<ClassicGroupFetchOwnerFaultKind> {
        self.owner_fault
    }

    pub(in crate::consumer::group) const fn reclaim_fault(
        &self,
    ) -> Option<crate::consumer::fetch_execution::FetchExecutionError> {
        self.reclaim_fault
    }

    pub(in crate::consumer::group) const fn reclaim_faults(&self) -> usize {
        self.reclaim_faults
    }

    pub(in crate::consumer::group) const fn reclaim_overflow(&self) -> bool {
        self.reclaim_overflow
    }
}
