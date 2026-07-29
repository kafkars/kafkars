//! Test-only observations of recovered API-91 call ownership.

use super::super::{AlterShareGroupOffsetsHost, AlterShareGroupOffsetsHostError};

impl AlterShareGroupOffsetsHost {
    pub(in crate::admin::alter_share_group_offsets) fn retain_recovered_call_for_test(&mut self) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredAlterShareGroupOffsetsCall::for_test());
    }

    pub(in crate::admin::alter_share_group_offsets) fn recovered_call_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::alter_share_group_offsets) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), AlterShareGroupOffsetsHostError> {
        self.settle_recovered_transport(0)
    }
}
