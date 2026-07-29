//! Engine-owned inert requests for singular and batched streams-group descriptions.

/// One explicit streams group and optional response expansions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupRequest {
    group_id: String,
    include_authorized_operations: bool,
    include_topology_description: bool,
}

impl DescribeStreamsGroupRequest {
    /// Creates one inert request for validation at admission.
    pub const fn new(group_id: String) -> Self {
        Self {
            group_id,
            include_authorized_operations: false,
            include_topology_description: false,
        }
    }

    /// Replaces authorization-bit expansion intent.
    pub const fn with_authorized_operations(mut self, include: bool) -> Self {
        self.include_authorized_operations = include;
        self
    }

    /// Replaces stable-v1 topology-description expansion intent.
    pub const fn with_topology_description(mut self, include: bool) -> Self {
        self.include_topology_description = include;
        self
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.group_id = self.group_id.into_boxed_str().into_string();
        self
    }

    pub(crate) fn into_parts(self) -> (String, bool, bool) {
        (
            self.group_id,
            self.include_authorized_operations,
            self.include_topology_description,
        )
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.group_id.capacity() == self.group_id.len()
    }
}

/// Caller-ordered streams groups and optional response expansions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupsRequest {
    group_ids: Vec<String>,
    include_authorized_operations: bool,
    include_topology_description: bool,
}

impl DescribeStreamsGroupsRequest {
    /// Creates one inert batch request for validation at admission.
    pub const fn new(group_ids: Vec<String>) -> Self {
        Self {
            group_ids,
            include_authorized_operations: false,
            include_topology_description: false,
        }
    }

    /// Replaces authorization-bit expansion intent for every group.
    pub const fn with_authorized_operations(mut self, include: bool) -> Self {
        self.include_authorized_operations = include;
        self
    }

    /// Replaces stable-v1 topology-description expansion intent for every group.
    pub const fn with_topology_description(mut self, include: bool) -> Self {
        self.include_topology_description = include;
        self
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.group_ids = self
            .group_ids
            .into_iter()
            .map(|group_id| group_id.into_boxed_str().into_string())
            .collect();
        self.group_ids.shrink_to_fit();
        self
    }

    pub(crate) fn into_parts(self) -> (Vec<String>, bool, bool) {
        (
            self.group_ids,
            self.include_authorized_operations,
            self.include_topology_description,
        )
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
