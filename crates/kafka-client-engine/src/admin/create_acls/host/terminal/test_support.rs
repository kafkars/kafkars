//! Test-only observations of retained Admin `CreateAcls` recovery ownership.

use super::super::{CreateAclsHost, CreateAclsHostError};

impl CreateAclsHost {
    pub(in crate::admin::create_acls) fn retain_recovered_call_for_test(&mut self) {
        let (plan, request_limit, result_limit) = {
            let operation = &self.operations[0];
            (
                operation
                    .machine
                    .plan()
                    .unwrap_or_else(|| panic!("test operation plan"))
                    .clone(),
                operation.request_limit,
                operation.result_limit,
            )
        };
        self.operations[0].recovered_call = Some(crate::driver::RecoveredCreateAclsCall::for_test(
            plan,
            request_limit,
            result_limit,
        ));
    }

    pub(in crate::admin::create_acls) fn recovered_ownership_is_retained_for_test(&self) -> bool {
        self.operations[0].recovered_call.is_some()
            && self.operations[0]
                .machine
                .plan()
                .is_some_and(|plan| plan.bindings()[0].resource_name() == "orders")
            && self.operations[0].prepared_results.is_some()
            && self.operations[0].prepared_outcomes.is_some()
    }

    pub(in crate::admin::create_acls) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), CreateAclsHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::create_acls) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), CreateAclsHostError> {
        self.publish_terminal(0)
    }
}
