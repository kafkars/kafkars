//! Test-only observations of retained Admin `DescribeFeatures` recovery ownership.

use super::super::{DescribeFeaturesHost, DescribeFeaturesHostError};

impl DescribeFeaturesHost {
    pub(in crate::admin::describe_features) fn retain_recovered_call_for_test(&mut self) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredDescribeFeaturesCall::for_test());
    }

    pub(in crate::admin::describe_features) fn recovered_call_is_retained_for_test(&self) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::describe_features) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), DescribeFeaturesHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::describe_features) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), DescribeFeaturesHostError> {
        self.publish_terminal(0)
    }
}
