//! Linear public `ShareGroup` offset-deletion intent translated at submission.

use super::engine::Request as EngineRequest;

/// Request retained by the inert public builder before submission.
pub(crate) struct DeleteShareGroupOffsetsAdminRequest {
    group_id: String,
    topics: Vec<String>,
}

impl DeleteShareGroupOffsetsAdminRequest {
    pub(crate) const fn new(group_id: String, topics: Vec<String>) -> Self {
        Self { group_id, topics }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(self.group_id, self.topics)
    }
}

impl std::fmt::Debug for DeleteShareGroupOffsetsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteShareGroupOffsetsAdminRequest")
            .field("group_id", &self.group_id)
            .field("topics", &self.topics)
            .finish()
    }
}
