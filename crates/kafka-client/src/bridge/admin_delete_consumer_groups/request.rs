//! Inert public Admin `DeleteConsumerGroups` intent translated at submission.

use kafka_client_engine::DeleteConsumerGroupsRequest as EngineRequest;

/// Linear request retained by the public builder before submission.
pub(crate) struct DeleteConsumerGroupsAdminRequest {
    group_ids: Vec<String>,
}

impl DeleteConsumerGroupsAdminRequest {
    pub(crate) const fn new(group_ids: Vec<String>) -> Self {
        Self { group_ids }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(self.group_ids)
    }
}

impl std::fmt::Debug for DeleteConsumerGroupsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteConsumerGroupsAdminRequest")
            .field("group_ids", &self.group_ids)
            .finish()
    }
}
