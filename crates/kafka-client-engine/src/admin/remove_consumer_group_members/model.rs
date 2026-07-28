//! Engine-owned canonical intent for selected static consumer-group members.

use core::mem::size_of;

use kafka_client_core::{
    ConsumerGroupMemberRemoval as CoreMember, RemoveConsumerGroupMembersPlan,
    RemoveConsumerGroupMembersPlanError,
};

/// One caller-ordered static member selected by group-instance identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerGroupMemberRemoval {
    group_instance_id: String,
}

impl ConsumerGroupMemberRemoval {
    /// Creates one inert static-member identity for validation at submission.
    pub fn new(group_instance_id: impl Into<String>) -> Self {
        Self {
            group_instance_id: group_instance_id.into(),
        }
    }

    fn canonicalize(mut self) -> Self {
        self.group_instance_id = canonical_string(self.group_instance_id);
        self
    }

    fn into_core(self) -> CoreMember {
        CoreMember::new(self.group_instance_id)
    }
}

/// One explicit consumer group and nonempty caller-ordered static-member batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveConsumerGroupMembersRequest {
    group_id: String,
    members: Vec<ConsumerGroupMemberRemoval>,
    reason: Option<String>,
}

impl RemoveConsumerGroupMembersRequest {
    /// Creates one inert request for validation at the public call boundary.
    pub const fn new(
        group_id: String,
        members: Vec<ConsumerGroupMemberRemoval>,
        reason: Option<String>,
    ) -> Self {
        Self {
            group_id,
            members,
            reason,
        }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.group_id = canonical_string(self.group_id);
        self.members = canonical_vec(
            self.members
                .into_iter()
                .map(ConsumerGroupMemberRemoval::canonicalize)
                .collect(),
        );
        self.reason = self.reason.map(canonical_string);
        self
    }

    pub(crate) fn preparation_charge(&self) -> Option<usize> {
        let member_bytes = self.members.iter().try_fold(0usize, |bytes, member| {
            bytes.checked_add(member.group_instance_id.len())
        })?;
        size_of::<Self>()
            .checked_add(
                self.members
                    .len()
                    .checked_mul(size_of::<ConsumerGroupMemberRemoval>())?,
            )?
            .checked_add(self.group_id.len())?
            .checked_add(member_bytes)?
            .checked_add(self.reason.as_ref().map_or(0, String::len))
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<RemoveConsumerGroupMembersPlan, RemoveConsumerGroupMembersPlanError> {
        RemoveConsumerGroupMembersPlan::new(
            self.group_id,
            self.members
                .into_iter()
                .map(ConsumerGroupMemberRemoval::into_core)
                .collect(),
            self.reason,
        )
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}

fn canonical_vec<T>(value: Vec<T>) -> Vec<T> {
    value.into_boxed_slice().into_vec()
}
