//! Engine-owned canonical request intent for API-91 offset alteration.

use kafka_client_core::AlterShareGroupOffset as CoreAlteration;

/// One caller-ordered topic-partition starting-offset alteration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffset {
    topic: String,
    partition: i32,
    start_offset: i64,
}

impl AlterShareGroupOffset {
    /// Creates one inert alteration for validation at admission.
    pub const fn new(topic: String, partition: i32, start_offset: i64) -> Self {
        Self {
            topic,
            partition,
            start_offset,
        }
    }

    fn canonicalize(mut self) -> Self {
        self.topic = canonical_string(self.topic);
        self
    }

    fn into_core(self) -> CoreAlteration {
        CoreAlteration::new(self.topic, self.partition, self.start_offset)
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.topic.capacity() == self.topic.len()
    }
}

/// One explicit share group and nonempty caller-ordered alteration batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffsetsRequest {
    group_id: String,
    changes: Vec<AlterShareGroupOffset>,
}

impl AlterShareGroupOffsetsRequest {
    /// Creates one inert request for validation at admission.
    pub const fn new(group_id: String, changes: Vec<AlterShareGroupOffset>) -> Self {
        Self { group_id, changes }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.group_id = canonical_string(self.group_id);
        self.changes = canonical_vec(
            self.changes
                .into_iter()
                .map(AlterShareGroupOffset::canonicalize)
                .collect(),
        );
        self
    }

    pub(crate) fn into_parts(self) -> (String, Vec<CoreAlteration>) {
        (
            self.group_id,
            self.changes
                .into_iter()
                .map(AlterShareGroupOffset::into_core)
                .collect(),
        )
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.group_id.capacity() == self.group_id.len()
            && self.changes.capacity() == self.changes.len()
            && self
                .changes
                .iter()
                .all(AlterShareGroupOffset::storage_is_canonical)
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}

fn canonical_vec<T>(value: Vec<T>) -> Vec<T> {
    value.into_boxed_slice().into_vec()
}
