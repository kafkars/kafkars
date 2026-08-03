//! Private ownership of exact engine group transaction metadata.

use kafka_client_engine::{
    GroupConsumerMembershipEpoch as EngineMembershipEpoch, GroupConsumerMetadata as EngineMetadata,
};

/// Protocol-aware epoch copied across the private engine bridge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupConsumerMembershipEpoch {
    Classic { generation_id: i32 },
    Consumer { member_epoch: i32 },
}

/// Cloneable opaque assignment fence retained beside stable facade fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupConsumerMetadata {
    inner: EngineMetadata,
}

impl GroupConsumerMetadata {
    pub(crate) const fn from_engine(inner: EngineMetadata) -> Self {
        Self { inner }
    }

    pub(crate) fn into_engine(self) -> EngineMetadata {
        self.inner
    }

    pub(crate) fn group(&self) -> &str {
        self.inner.group()
    }

    pub(crate) fn member(&self) -> &str {
        self.inner.member()
    }

    pub(crate) const fn membership_epoch(&self) -> GroupConsumerMembershipEpoch {
        match self.inner.membership_epoch() {
            EngineMembershipEpoch::Classic { generation_id } => {
                GroupConsumerMembershipEpoch::Classic { generation_id }
            }
            EngineMembershipEpoch::Consumer { member_epoch } => {
                GroupConsumerMembershipEpoch::Consumer { member_epoch }
            }
        }
    }

    pub(crate) const fn assignment_epoch(&self) -> u64 {
        self.inner.assignment_epoch()
    }

    pub(crate) fn group_instance_id(&self) -> Option<&str> {
        self.inner.group_instance_id()
    }
}
