//! Test-only observations of recovered route-local configuration ownership.

use kafka_client_core::{LegacyAlterConfigsPlan, LegacyAlterConfigsRoute};

use super::super::{LegacyAlterConfigsHost, LegacyAlterConfigsHostError};

impl LegacyAlterConfigsHost {
    pub(in crate::admin::legacy_alter_configs) fn retain_recovered_call_for_test(
        &mut self,
        route: LegacyAlterConfigsRoute,
        plan: LegacyAlterConfigsPlan,
    ) {
        self.operations[0].recovered_call = Some(
            crate::driver::RecoveredLegacyAlterConfigsCall::for_test(route, plan),
        );
    }

    pub(in crate::admin::legacy_alter_configs) fn recovered_correlation_matches_for_test(
        &self,
        expected_route: LegacyAlterConfigsRoute,
        expected_plan: &LegacyAlterConfigsPlan,
    ) -> bool {
        self.operations[0]
            .recovered_call
            .as_ref()
            .is_some_and(|recovered| recovered.matches_correlation(expected_route, expected_plan))
    }

    pub(in crate::admin::legacy_alter_configs) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), LegacyAlterConfigsHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::legacy_alter_configs) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), LegacyAlterConfigsHostError> {
        self.publish_terminal(0)
    }
}
