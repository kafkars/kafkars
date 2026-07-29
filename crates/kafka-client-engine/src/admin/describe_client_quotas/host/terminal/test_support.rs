//! Test-only observations of retained Admin `DescribeClientQuotas` recovery ownership.

use super::super::{DescribeClientQuotasHost, DescribeClientQuotasHostError};

impl DescribeClientQuotasHost {
    pub(in crate::admin::describe_client_quotas) fn retain_recovered_call_for_test(&mut self) {
        let (plan, request_scratch_limit, result_limit) = {
            let operation = &self.operations[0];
            (
                operation.plan.clone(),
                operation.request_scratch_limit,
                operation.result_limit,
            )
        };
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredDescribeClientQuotasCall::for_test(
                plan,
                request_scratch_limit,
                result_limit,
            ));
    }

    pub(in crate::admin::describe_client_quotas) fn recovered_call_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::describe_client_quotas) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), DescribeClientQuotasHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::describe_client_quotas) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), DescribeClientQuotasHostError> {
        self.publish_terminal(0)
    }
}
