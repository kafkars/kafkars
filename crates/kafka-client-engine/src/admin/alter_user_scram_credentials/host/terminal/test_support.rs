//! Test-only observations of recovered SCRAM call-and-correlation ownership.

use kafka_client_core::{
    AlterUserScramCredentialsInput, AlterUserScramCredentialsPlan, OperationId,
};

use super::super::{AlterUserScramCredentialsHost, AlterUserScramCredentialsHostError};

impl AlterUserScramCredentialsHost {
    pub(in crate::admin::alter_user_scram_credentials) const fn retained_bytes_for_test(
        &self,
    ) -> usize {
        self.retained_bytes
    }

    pub(in crate::admin::alter_user_scram_credentials) fn retain_recovered_call_for_test(
        &mut self,
        plan: AlterUserScramCredentialsPlan,
        prepared_request_bytes: usize,
        result_limit: usize,
    ) {
        self.operations[0].recovered_call = Some(
            crate::driver::RecoveredAlterUserScramCredentialsCall::for_test(
                plan,
                prepared_request_bytes,
                result_limit,
            ),
        );
    }

    pub(in crate::admin::alter_user_scram_credentials) fn recovered_ownership_matches_for_test(
        &self,
        plan: &AlterUserScramCredentialsPlan,
        prepared_request_bytes: usize,
        result_limit: usize,
    ) -> bool {
        self.operations[0]
            .recovered_call
            .as_ref()
            .is_some_and(|recovered| {
                recovered.matches_evidence(plan, prepared_request_bytes, result_limit)
            })
    }

    pub(in crate::admin::alter_user_scram_credentials) fn call_matches_for_test(
        &self,
        plan: &AlterUserScramCredentialsPlan,
        prepared_request_bytes: usize,
        result_limit: usize,
    ) -> bool {
        self.operations[0]
            .call
            .as_ref()
            .is_some_and(|call| call.matches_evidence(plan, prepared_request_bytes, result_limit))
    }

    pub(in crate::admin::alter_user_scram_credentials) fn rejected_submission_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].rejected_submission.is_some()
    }

    pub(in crate::admin::alter_user_scram_credentials) fn retain_raw_terminal_for_test(
        &mut self,
        plan: AlterUserScramCredentialsPlan,
        prepared_request_bytes: usize,
        result_limit: usize,
    ) {
        self.operations[0].raw_terminal = Some(
            crate::driver::AlterUserScramCredentialsRawTerminal::for_test(
                plan,
                prepared_request_bytes,
                result_limit,
            ),
        );
    }

    pub(in crate::admin::alter_user_scram_credentials) fn raw_terminal_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].raw_terminal.is_some()
    }

    pub(in crate::admin::alter_user_scram_credentials) fn settle_raw_for_test(
        &mut self,
    ) -> Result<(), AlterUserScramCredentialsHostError> {
        self.settle_raw(0)
    }

    pub(in crate::admin::alter_user_scram_credentials) fn apply_input_for_test(
        &mut self,
        operation_id: OperationId,
        input: AlterUserScramCredentialsInput,
    ) -> Result<(), AlterUserScramCredentialsHostError> {
        self.apply(operation_id, input)
    }

    pub(in crate::admin::alter_user_scram_credentials) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), AlterUserScramCredentialsHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::alter_user_scram_credentials) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), AlterUserScramCredentialsHostError> {
        self.publish_terminal(0)
    }
}
