//! Read-only Fetch owner observations and test-only prepared-owner access.

use super::{
    activation::ClassicGroupFetchActivation, model::ClassicGroupFetchOwnerFault,
    owner::ClassicGroupFetchOwner,
};

impl ClassicGroupFetchOwner {
    pub(in crate::consumer::group) const fn activation(
        &self,
    ) -> Option<&ClassicGroupFetchActivation> {
        self.activation.as_ref()
    }

    pub(in crate::consumer::group) const fn fault(&self) -> Option<&ClassicGroupFetchOwnerFault> {
        self.fault.as_ref()
    }

    pub(super) const fn is_faulted(&self) -> bool {
        self.fault.is_some() || !self.reclaim_faults.is_empty() || self.reclaim_overflow.is_some()
    }

    pub(in crate::consumer::group) const fn machine_assignment_epoch(
        &self,
    ) -> Option<kafka_client_core::AssignmentEpoch> {
        self.machine.assignment_epoch()
    }

    #[cfg(test)]
    pub(in crate::consumer::group) const fn broker_session_close_requested_for_test(&self) -> bool {
        self.fetches.broker_session_close_requested()
    }

    #[cfg(test)]
    pub(super) fn pop_prepared_for_test(
        &mut self,
    ) -> Option<crate::consumer::fetch_execution::PreparedFetchExecution> {
        self.pending_fetches.pop_front()
    }
}
