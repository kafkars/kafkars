//! Test-only observations of recovered call and exact voter-plan ownership.

use kafka_client_core::AddRaftVoterPlan;

use super::super::{AddRaftVoterHost, AddRaftVoterHostError};

impl AddRaftVoterHost {
    pub(in crate::admin::add_raft_voter) fn retain_controller_refresh_for_test(
        &mut self,
        plan: AddRaftVoterPlan,
    ) {
        drop(self.operations[0].call.take());
        self.operations[0].raw_terminal =
            Some(crate::driver::AddRaftVoterRawTerminal::not_controller_for_test(plan));
    }

    pub(in crate::admin::add_raft_voter) fn raw_terminal_is_retained_for_test(&self) -> bool {
        self.operations[0].raw_terminal.is_some()
    }

    pub(in crate::admin::add_raft_voter) fn retain_recovered_call_for_test(
        &mut self,
        plan: AddRaftVoterPlan,
    ) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredAddRaftVoterCall::for_test(plan));
    }

    pub(in crate::admin::add_raft_voter) fn recovered_ownership_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0]
            .recovered_call
            .as_ref()
            .is_some_and(|recovered| {
                recovered.plan().voter_id() == 7
                    && recovered.plan().voter_directory_id() == [7; 16]
                    && recovered.plan().listeners()[0].host() == "controller-a"
            })
    }

    pub(in crate::admin::add_raft_voter) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), AddRaftVoterHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::add_raft_voter) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), AddRaftVoterHostError> {
        self.publish_terminal(0)
    }
}
