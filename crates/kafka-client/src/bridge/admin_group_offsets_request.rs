//! Inert consumer-group offset intent translated only at the engine boundary.

use kafka_client_engine::ListConsumerGroupOffsetsRequest as EngineRequest;

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

    pub(super) fn into_engine(self) -> EngineRequest {
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
