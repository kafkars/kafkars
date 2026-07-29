//! Inert consumer-group offset intent translated only at the engine boundary.

use kafka_client_engine::{
    ListConsumerGroupOffsetsRequest as EngineRequest,
    ListConsumerGroupsOffsetsRequest as EngineBatchRequest,
};

/// Linear request retained by the public builder before submission.
pub(crate) struct ListConsumerGroupOffsetsAdminRequest {
    group_id: String,
    require_stable: bool,
}

impl ListConsumerGroupOffsetsAdminRequest {
    pub(crate) const fn new(group_id: String) -> Self {
        Self {
            group_id,
            require_stable: false,
        }
    }

    pub(crate) const fn with_require_stable(mut self, require_stable: bool) -> Self {
        self.require_stable = require_stable;
        self
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(self.group_id, self.require_stable)
    }
}

impl std::fmt::Debug for ListConsumerGroupOffsetsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListConsumerGroupOffsetsAdminRequest")
            .field("group_id", &self.group_id)
            .field("require_stable", &self.require_stable)
            .finish()
    }
}

/// Linear plural request retained by the public builder before submission.
pub(crate) struct ListConsumerGroupsOffsetsAdminRequest {
    group_ids: Vec<String>,
    require_stable: bool,
}

impl ListConsumerGroupsOffsetsAdminRequest {
    pub(crate) const fn new(group_ids: Vec<String>) -> Self {
        Self {
            group_ids,
            require_stable: false,
        }
    }

    pub(crate) const fn with_require_stable(mut self, require_stable: bool) -> Self {
        self.require_stable = require_stable;
        self
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineBatchRequest {
        EngineBatchRequest::new(self.group_ids, self.require_stable)
    }
}

impl std::fmt::Debug for ListConsumerGroupsOffsetsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListConsumerGroupsOffsetsAdminRequest")
            .field("group_ids", &self.group_ids)
            .field("require_stable", &self.require_stable)
            .finish()
    }
}
