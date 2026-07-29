//! Metadata-quorum operation entry point on the shared public admin handle.

use super::Admin;
use crate::admin::DescribeMetadataQuorumBuilder;

impl Admin {
    /// Builds an inert query for Kafka's fixed metadata quorum.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DescribeMetadataQuorumBuilder::submit`] is called.
    pub fn describe_metadata_quorum(&self) -> DescribeMetadataQuorumBuilder {
        DescribeMetadataQuorumBuilder::new(self.engine.clone(), self.engine.default_timeout())
    }
}
