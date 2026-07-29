//! Engine-owned scalar intent for singular and batched consumer-group queries.

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

/// Caller-ordered all-partition queries for multiple consumer groups.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupsOffsetsRequest {
    group_ids: Vec<String>,
    require_stable: bool,
}

impl ListConsumerGroupsOffsetsRequest {
    /// Creates one inert batch request for validation at admission.
    pub const fn new(group_ids: Vec<String>, require_stable: bool) -> Self {
        Self {
            group_ids,
            require_stable,
        }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        for group_id in &mut self.group_ids {
            *group_id = core::mem::take(group_id).into_boxed_str().into_string();
        }
        self.group_ids = self.group_ids.into_boxed_slice().into_vec();
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsPlanError> {
        ListConsumerGroupOffsetsPlan::new_batch(self.group_ids, self.require_stable)
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.group_ids.capacity() == self.group_ids.len()
            && self
                .group_ids
                .iter()
                .all(|group_id| group_id.capacity() == group_id.len())
    }
}
