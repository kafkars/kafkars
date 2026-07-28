//! Engine-owned scalar intent for one Admin `DescribeLogDirs` query.

use kafka_client_core::{AdminDescribeLogDirsPlan, AdminDescribeLogDirsPlanError};

/// One caller-ordered selected-broker request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeLogDirsRequest {
    broker_ids: Vec<i32>,
}

impl DescribeLogDirsRequest {
    /// Creates inert request intent for validation at the operation boundary.
    pub const fn new(broker_ids: Vec<i32>) -> Self {
        Self { broker_ids }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.broker_ids.shrink_to_fit();
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<AdminDescribeLogDirsPlan, AdminDescribeLogDirsPlanError> {
        AdminDescribeLogDirsPlan::new(self.broker_ids)
    }
}
