//! Post-driver-shutdown release of one failed assigned-consumer owner.

use super::{
    assigned_event::AssignedConsumerEventRecovery, assigned_owner::AssignedConsumerOwner,
    assigned_owner_close::AssignedConsumerCloseSettlement,
    assigned_owner_fault::AssignedConsumerFaultKind,
    assigned_owner_status::AssignedConsumerRecoveryAudit, fetch_execution::FetchExecutionError,
};

/// Scalar audit of linear owners released after the driver was destroyed.
#[must_use = "assigned-consumer shutdown recovery observations must be inspected"]
#[derive(Debug)]
pub(crate) struct AssignedConsumerShutdownRecovery {
    owner_fault: Option<AssignedConsumerFaultKind>,
    recovered_position_calls: usize,
    position_completion: Option<crate::driver::PositionCompletionFailure>,
    fetch_completion: Option<crate::driver::FetchCompletionObservation>,
    fetch_executor_faulted: bool,
    recovered_fetch_requests: usize,
    reclaim_failures: usize,
    first_reclaim_failure: Option<FetchExecutionError>,
    events: AssignedConsumerEventRecovery,
    close_completion: Option<crate::completion::CompletionRegistryError>,
}

/// Before-and-after audit retained in abnormal engine-host diagnostics.
#[derive(Debug)]
pub(crate) struct AssignedConsumerRecoveryReport {
    audit: AssignedConsumerRecoveryAudit,
    release: AssignedConsumerShutdownRecovery,
    lock_was_poisoned: bool,
}

impl AssignedConsumerOwner {
    /// Consumes retained mechanism ownership only after unique driver teardown.
    pub(crate) fn release_assigned_after_driver_shutdown(
        mut self,
    ) -> AssignedConsumerShutdownRecovery {
        let close_completion = loop {
            match self.settle_close_after_driver_shutdown() {
                // The closed consumer notifier has capacity one and this owner
                // can issue only its sole close ticket. Real saturation is
                // therefore unreachable here; Retry exists for injected
                // evidence and future reviewed ticket-set expansion.
                Ok(AssignedConsumerCloseSettlement::Retry) => {}
                Ok(
                    AssignedConsumerCloseSettlement::Idle
                    | AssignedConsumerCloseSettlement::Published,
                ) => {
                    break None;
                }
                Err(error) => break Some(error),
            }
        };
        let owner_fault = self
            .fault
            .as_ref()
            .map(super::assigned_owner_fault::AssignedConsumerOwnerFault::kind);
        let recovered_position_calls = self.positions.retained_positions();
        let position_completion = self
            .positions
            .release_position_calls_after_driver_shutdown();
        let fetch = self.fetches.release_fetch_executor_after_driver_shutdown();
        let fetch_executor_faulted = fetch.had_fault();
        let (requests, fetch_completion) = fetch.into_driver_recovery().into_parts();
        let recovered_fetch_requests = requests.len();
        drop(requests);
        let reclaim_failures = self
            .reclaim_faults
            .len()
            .saturating_add(usize::from(self.reclaim_overflow.is_some()));
        let mut first_reclaim_failure = None;
        for failure in self.reclaim_faults.drain(..) {
            let (error, delivery) = failure.into_parts();
            first_reclaim_failure.get_or_insert(error);
            drop(delivery);
        }
        if let Some(failure) = self.reclaim_overflow.take() {
            let (error, delivery) = failure.into_parts();
            first_reclaim_failure.get_or_insert(error);
            drop(delivery);
        }
        let events = self.events.recover_after_driver_shutdown();
        AssignedConsumerShutdownRecovery {
            owner_fault,
            recovered_position_calls,
            position_completion,
            fetch_completion,
            fetch_executor_faulted,
            recovered_fetch_requests,
            reclaim_failures,
            first_reclaim_failure,
            events,
            close_completion,
        }
    }
}

impl AssignedConsumerShutdownRecovery {
    /// Reports whether shutdown retained evidence worth preserving.
    pub(crate) const fn requires_report(&self) -> bool {
        self.owner_fault.is_some()
            || self.position_completion.is_some()
            || self.fetch_completion.is_some()
            || self.fetch_executor_faulted
            || self.reclaim_failures != 0
            || self.events.claimed() != 0
            || self.events.ready() != 0
            || self.close_completion.is_some()
    }

    pub(crate) const fn owner_fault(&self) -> Option<AssignedConsumerFaultKind> {
        self.owner_fault
    }

    pub(crate) const fn recovered_position_calls(&self) -> usize {
        self.recovered_position_calls
    }

    pub(crate) const fn recovered_fetch_requests(&self) -> usize {
        self.recovered_fetch_requests
    }

    pub(crate) const fn reclaim_failures(&self) -> usize {
        self.reclaim_failures
    }

    pub(crate) const fn first_reclaim_failure(&self) -> Option<FetchExecutionError> {
        self.first_reclaim_failure
    }

    pub(crate) const fn recovered_event_claims(&self) -> usize {
        self.events.claimed()
    }

    pub(crate) const fn recovered_ready_events(&self) -> usize {
        self.events.ready()
    }

    pub(crate) const fn close_completion_error(
        &self,
    ) -> Option<crate::completion::CompletionRegistryError> {
        self.close_completion
    }
}

impl AssignedConsumerRecoveryReport {
    pub(crate) const fn new(
        audit: AssignedConsumerRecoveryAudit,
        release: AssignedConsumerShutdownRecovery,
        lock_was_poisoned: bool,
    ) -> Self {
        Self {
            audit,
            release,
            lock_was_poisoned,
        }
    }

    pub(crate) fn requires_cleanup_report(&self) -> bool {
        self.release.requires_report() || !self.audit.was_cleanly_closed() || self.lock_was_poisoned
    }
}
