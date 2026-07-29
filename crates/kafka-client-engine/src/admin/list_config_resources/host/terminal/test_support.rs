//! Test-only observations of retained Admin `ListConfigResources` correlation ownership.

use super::super::{
    LIST_CONFIG_RESOURCES_RESULT_BYTES, ListConfigResourcesHost, ListConfigResourcesHostError,
};

impl ListConfigResourcesHost {
    pub(in crate::admin::list_config_resources) fn retain_recovered_call_for_test(&mut self) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredListConfigResourcesCall::for_test());
    }

    pub(in crate::admin::list_config_resources) fn recovered_ownership_and_correlation_are_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].recovered_call.is_some()
            && self.request_correlation_is_retained_for_test()
    }

    pub(in crate::admin::list_config_resources) fn request_correlation_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].remaining_result_bytes == LIST_CONFIG_RESOURCES_RESULT_BYTES
            && self.operations[0]
                .machine
                .plan()
                .resource_types()
                .iter()
                .map(|resource_type| resource_type.code())
                .eq([2_i8, 64_i8])
    }

    pub(in crate::admin::list_config_resources) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), ListConfigResourcesHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::list_config_resources) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), ListConfigResourcesHostError> {
        self.publish_terminal(0)
    }
}
