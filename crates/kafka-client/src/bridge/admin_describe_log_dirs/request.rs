//! Inert broker selection translated only at the engine boundary.

use super::engine::Request as EngineRequest;

/// Linear caller-ordered broker selection retained by the public builder.
pub(crate) struct DescribeLogDirsAdminRequest {
    broker_ids: Vec<i32>,
}

impl DescribeLogDirsAdminRequest {
    pub(crate) const fn new(broker_ids: Vec<i32>) -> Self {
        Self { broker_ids }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(self.broker_ids)
    }
}

impl std::fmt::Debug for DescribeLogDirsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeLogDirsAdminRequest")
            .field("broker_ids", &self.broker_ids)
            .finish()
    }
}
