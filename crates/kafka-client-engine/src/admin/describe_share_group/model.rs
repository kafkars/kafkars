//! Engine-owned inert requests for singular and batched share-group description.

/// One explicit share group and authorization-bit expansion intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupRequest {
    group_id: String,
    include_authorized_operations: bool,
}

impl DescribeShareGroupRequest {
    /// Creates one inert request for validation at admission.
    pub const fn new(group_id: String) -> Self {
        Self {
            group_id,
            include_authorized_operations: false,
        }
    }

    /// Replaces authorization-bit expansion intent.
    pub const fn with_authorized_operations(mut self, include: bool) -> Self {
        self.include_authorized_operations = include;
        self
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.group_id = self.group_id.into_boxed_str().into_string();
        self
    }

    pub(crate) fn into_parts(self) -> (String, bool) {
        (self.group_id, self.include_authorized_operations)
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.group_id.capacity() == self.group_id.len()
    }
}

/// Caller-ordered share groups and authorization-bit expansion intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupsRequest {
    group_ids: Vec<String>,
    include_authorized_operations: bool,
}

impl DescribeShareGroupsRequest {
    /// Creates one inert batch request for validation at admission.
    pub const fn new(group_ids: Vec<String>) -> Self {
        Self {
            group_ids,
            include_authorized_operations: false,
        }
    }

    /// Replaces authorization-bit expansion intent.
    pub const fn with_authorized_operations(mut self, include: bool) -> Self {
        self.include_authorized_operations = include;
        self
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        for group_id in &mut self.group_ids {
            *group_id = core::mem::take(group_id).into_boxed_str().into_string();
        }
        self.group_ids = self.group_ids.into_boxed_slice().into_vec();
        self
    }

    pub(crate) fn into_parts(self) -> (Vec<String>, bool) {
        (self.group_ids, self.include_authorized_operations)
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
