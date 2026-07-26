//! Engine-owned canonical request intent for consumer-group offset deletion.

use kafka_client_core::{
    DeleteConsumerGroupOffsetTarget as CoreTarget, DeleteConsumerGroupOffsetsPlan,
    DeleteConsumerGroupOffsetsPlanError,
};

/// One caller-ordered topic-partition whose committed offset must be deleted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteConsumerGroupOffsetTarget {
    topic: String,
    partition: i32,
}

impl DeleteConsumerGroupOffsetTarget {
    /// Creates one raw target for validation at admission.
    pub const fn new(topic: String, partition: i32) -> Self {
        Self { topic, partition }
    }

    fn canonicalize(mut self) -> Self {
        self.topic = canonical_string(self.topic);
        self
    }

    fn into_core(self) -> CoreTarget {
        CoreTarget::new(self.topic, self.partition)
    }
}

/// One explicit group and nonempty caller-ordered deletion batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteConsumerGroupOffsetsRequest {
    group_id: String,
    targets: Vec<DeleteConsumerGroupOffsetTarget>,
}

impl DeleteConsumerGroupOffsetsRequest {
    /// Creates one inert request for validation at the public call boundary.
    pub const fn new(group_id: String, targets: Vec<DeleteConsumerGroupOffsetTarget>) -> Self {
        Self { group_id, targets }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.group_id = canonical_string(self.group_id);
        self.targets = canonical_vec(
            self.targets
                .into_iter()
                .map(DeleteConsumerGroupOffsetTarget::canonicalize)
                .collect(),
        );
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<DeleteConsumerGroupOffsetsPlan, DeleteConsumerGroupOffsetsPlanError> {
        DeleteConsumerGroupOffsetsPlan::new(
            self.group_id,
            self.targets
                .into_iter()
                .map(DeleteConsumerGroupOffsetTarget::into_core)
                .collect(),
        )
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.group_id.capacity() == self.group_id.len()
            && self.targets.capacity() == self.targets.len()
            && self
                .targets
                .iter()
                .all(|target| target.topic.capacity() == target.topic.len())
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}

fn canonical_vec<T>(value: Vec<T>) -> Vec<T> {
    value.into_boxed_slice().into_vec()
}
