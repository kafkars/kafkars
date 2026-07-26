//! Engine-owned scalar intent for one consumer-group offset query.

use kafka_client_core::{ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsPlanError};

/// One all-partition query for an explicit consumer group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupOffsetsRequest {
    group_id: String,
    require_stable: bool,
}

impl ListConsumerGroupOffsetsRequest {
    /// Creates one inert request for validation at the admission boundary.
    pub const fn new(group_id: String, require_stable: bool) -> Self {
        Self {
            group_id,
            require_stable,
        }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.group_id = self.group_id.into_boxed_str().into_string();
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsPlanError> {
        ListConsumerGroupOffsetsPlan::new(self.group_id, self.require_stable)
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.group_id.capacity() == self.group_id.len()
    }
}
