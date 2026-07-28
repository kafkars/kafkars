//! Test-only observations of retained ACL-deletion recovery ownership.

use super::super::{DeleteAclsHost, DeleteAclsHostError};

impl DeleteAclsHost {
    pub(in crate::admin::delete_acls) fn retain_mismatched_recovered_call_for_test(&mut self) {
        let plan = self.operations[0]
            .machine
            .plan()
            .unwrap_or_else(|| panic!("active test operation retains its plan"))
            .clone();
        let bounds = self.operations[0].bounds;
        self.operations[0].recovered_call = Some(crate::driver::RecoveredDeleteAclsCall::for_test(
            plan,
            bounds.request_limit.saturating_add(1),
            bounds.nested_count_capacity,
            bounds.result_capacity,
            bounds.outcome_capacity,
        ));
    }

    pub(in crate::admin::delete_acls) fn retain_recovered_call_for_test(&mut self) {
        let plan = self.operations[0]
            .machine
            .plan()
            .unwrap_or_else(|| panic!("active test operation retains its plan"))
            .clone();
        let bounds = self.operations[0].bounds;
        self.operations[0].recovered_call = Some(crate::driver::RecoveredDeleteAclsCall::for_test(
            plan,
            bounds.request_limit,
            bounds.nested_count_capacity,
            bounds.result_capacity,
            bounds.outcome_capacity,
        ));
    }

    pub(in crate::admin::delete_acls) fn recovered_ownership_is_retained_for_test(&self) -> bool {
        self.operations[0]
            .recovered_call
            .as_ref()
            .is_some_and(|recovered| self.operations[0].matches_recovered(recovered))
    }

    pub(in crate::admin::delete_acls) fn has_recovered_ownership_for_test(&self) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::delete_acls) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), DeleteAclsHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::delete_acls) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), DeleteAclsHostError> {
        self.publish_terminal(0)
    }
}
