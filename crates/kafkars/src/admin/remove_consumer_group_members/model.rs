//! Stable inert identity for one selected static consumer-group member.

/// One caller-ordered static member selected by group-instance identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerGroupMemberRemoval {
    group_instance_id: String,
}

impl ConsumerGroupMemberRemoval {
    /// Creates one inert static-member identity validated when submitted.
    pub fn new(group_instance_id: impl Into<String>) -> Self {
        Self {
            group_instance_id: group_instance_id.into(),
        }
    }

    /// Returns the selected static group-instance identity.
    pub fn group_instance_id(&self) -> &str {
        &self.group_instance_id
    }

    pub(crate) fn into_inner(self) -> String {
        self.group_instance_id
    }
}
