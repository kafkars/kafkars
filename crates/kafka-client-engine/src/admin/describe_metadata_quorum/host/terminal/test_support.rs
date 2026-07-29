//! Test-only observations of retained metadata-quorum call ownership.

use super::super::{DescribeMetadataQuorumHost, DescribeMetadataQuorumHostError};

impl DescribeMetadataQuorumHost {
    pub(in crate::admin::describe_metadata_quorum) fn retain_recovered_call_for_test(&mut self) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredDescribeMetadataQuorumCall::for_test());
    }

    pub(in crate::admin::describe_metadata_quorum) fn recovered_call_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::describe_metadata_quorum) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), DescribeMetadataQuorumHostError> {
        self.settle_recovered_transport(0)
    }
}
