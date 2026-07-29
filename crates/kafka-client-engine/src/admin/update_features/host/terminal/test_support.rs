//! Test-only observations of retained Admin `UpdateFeatures` recovery ownership.

use super::super::{UpdateFeaturesHost, UpdateFeaturesHostError};

impl UpdateFeaturesHost {
    pub(in crate::admin::update_features) fn retain_recovered_call_for_test(&mut self) {
        let plan = self.operations[0].response_plan.clone();
        let result_limit = self.operations[0].remaining_result_bytes;
        self.operations[0].recovered_call = Some(
            crate::driver::RecoveredUpdateFeaturesCall::for_test(plan, result_limit),
        );
    }

    pub(in crate::admin::update_features) fn retain_mismatched_recovered_call_for_test(&mut self) {
        let plan = self.operations[0].response_plan.clone();
        let result_limit = self.operations[0].remaining_result_bytes.saturating_sub(1);
        self.operations[0].recovered_call = Some(
            crate::driver::RecoveredUpdateFeaturesCall::for_test(plan, result_limit),
        );
    }

    pub(in crate::admin::update_features) fn recovered_ownership_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0]
            .recovered_call
            .as_ref()
            .is_some_and(|recovered| {
                recovered.matches_evidence(
                    &self.operations[0].response_plan,
                    self.operations[0].remaining_result_bytes,
                )
            })
            && self.operations[0].response_plan.updates()[0].feature() == "metadata.version"
    }

    pub(in crate::admin::update_features) fn call_ownership_is_retained_for_test(&self) -> bool {
        self.operations[0].call.is_some()
    }

    pub(in crate::admin::update_features) fn recovered_call_is_retained_for_test(&self) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::update_features) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), UpdateFeaturesHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::update_features) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), UpdateFeaturesHostError> {
        self.publish_terminal(0)
    }
}
