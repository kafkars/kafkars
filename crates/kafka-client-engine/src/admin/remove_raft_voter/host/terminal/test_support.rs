//! Test-only controller-refresh and recovered voter-correlation observations.

use kafka_client_core::RemoveRaftVoterPlan;

use super::super::{RemoveRaftVoterHost, RemoveRaftVoterHostError};

impl RemoveRaftVoterHost {
    pub(in crate::admin::remove_raft_voter) fn retain_controller_refresh_for_test(
        &mut self,
        plan: RemoveRaftVoterPlan,
    ) {
        drop(self.operations[0].call.take());
        self.operations[0].raw_terminal =
            Some(crate::driver::RemoveRaftVoterRawTerminal::not_controller_for_test(plan));
    }

    pub(in crate::admin::remove_raft_voter) fn raw_terminal_is_retained_for_test(&self) -> bool {
        self.operations[0].raw_terminal.is_some()
    }

    pub(in crate::admin::remove_raft_voter) fn retain_recovered_call_for_test(
        &mut self,
        plan: RemoveRaftVoterPlan,
    ) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredRemoveRaftVoterCall::for_test(plan));
    }

    pub(in crate::admin::remove_raft_voter) fn recovered_plan_matches_for_test(
        &self,
        expected: &RemoveRaftVoterPlan,
    ) -> bool {
        self.operations[0]
            .recovered_call
            .as_ref()
            .is_some_and(|recovered| recovered.matches_plan_for_test(expected))
    }

    pub(in crate::admin::remove_raft_voter) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), RemoveRaftVoterHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::remove_raft_voter) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), RemoveRaftVoterHostError> {
        self.publish_terminal(0)
    }
}
