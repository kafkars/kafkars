//! Test-only observations of recovered `AlterClientQuotas` call-and-plan ownership.

use kafka_client_core::{AlterClientQuotasInput, AlterClientQuotasPlan, OperationId};

use super::super::{AlterClientQuotasHost, AlterClientQuotasHostError};

impl AlterClientQuotasHost {
    pub(in crate::admin::alter_client_quotas) fn retain_recovered_call_for_test(
        &mut self,
        plan: AlterClientQuotasPlan,
        retained_limit: usize,
    ) {
        self.operations[0].recovered_call = Some(
            crate::driver::RecoveredAlterClientQuotasCall::for_test(plan, retained_limit),
        );
    }

    pub(in crate::admin::alter_client_quotas) fn recovered_ownership_matches_for_test(
        &self,
        plan: &AlterClientQuotasPlan,
        retained_limit: usize,
    ) -> bool {
        self.operations[0]
            .recovered_call
            .as_ref()
            .is_some_and(|recovered| recovered.matches(plan, retained_limit))
    }

    pub(in crate::admin::alter_client_quotas) fn call_matches_for_test(
        &self,
        plan: &AlterClientQuotasPlan,
        retained_limit: usize,
    ) -> bool {
        self.operations[0]
            .call
            .as_ref()
            .is_some_and(|call| call.matches(plan, retained_limit))
    }

    pub(in crate::admin::alter_client_quotas) fn rejected_submission_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].rejected_submission.is_some()
    }

    pub(in crate::admin::alter_client_quotas) fn retain_raw_terminal_for_test(
        &mut self,
        plan: AlterClientQuotasPlan,
        retained_limit: usize,
    ) {
        self.operations[0].raw_terminal = Some(
            crate::driver::AlterClientQuotasRawTerminal::for_test(plan, retained_limit),
        );
    }

    pub(in crate::admin::alter_client_quotas) fn raw_terminal_is_retained_for_test(&self) -> bool {
        self.operations[0].raw_terminal.is_some()
    }

    pub(in crate::admin::alter_client_quotas) fn settle_raw_for_test(
        &mut self,
    ) -> Result<(), AlterClientQuotasHostError> {
        self.settle_raw(0)
    }

    pub(in crate::admin::alter_client_quotas) fn apply_input_for_test(
        &mut self,
        operation_id: OperationId,
        input: AlterClientQuotasInput,
    ) -> Result<(), AlterClientQuotasHostError> {
        self.apply(operation_id, input)
    }

    pub(in crate::admin::alter_client_quotas) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), AlterClientQuotasHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::alter_client_quotas) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), AlterClientQuotasHostError> {
        self.publish_terminal(0)
    }
}
