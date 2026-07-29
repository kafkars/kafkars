//! Engine-owned canonical request intent for share-group offset deletion.

/// One explicit share group and nonempty caller-ordered topic batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteShareGroupOffsetsRequest {
    group_id: String,
    topics: Vec<String>,
}

impl DeleteShareGroupOffsetsRequest {
    /// Creates one inert request for validation at admission.
    pub const fn new(group_id: String, topics: Vec<String>) -> Self {
        Self { group_id, topics }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.group_id = canonical_string(self.group_id);
        self.topics = canonical_vec(self.topics.into_iter().map(canonical_string).collect());
        self
    }

    pub(crate) fn into_parts(self) -> (String, Vec<String>) {
        (self.group_id, self.topics)
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.group_id.capacity() == self.group_id.len()
            && self.topics.capacity() == self.topics.len()
            && self
                .topics
                .iter()
                .all(|topic| topic.capacity() == topic.len())
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}

fn canonical_vec<T>(value: Vec<T>) -> Vec<T> {
    value.into_boxed_slice().into_vec()
}
