//! Stable public descriptions with common identity and explicit protocol facts.

use super::{ConsumerGroupDescriptionDetails, ConsumerGroupMemberDetails};

/// One member with common identity and protocol-specific facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerGroupMember {
    member_id: String,
    group_instance_id: Option<String>,
    client_id: String,
    client_host: String,
    details: ConsumerGroupMemberDetails,
}

impl ConsumerGroupMember {
    pub(crate) const fn new(
        member_id: String,
        group_instance_id: Option<String>,
        client_id: String,
        client_host: String,
        details: ConsumerGroupMemberDetails,
    ) -> Self {
        Self {
            member_id,
            group_instance_id,
            client_id,
            client_host,
            details,
        }
    }

    /// Returns the stable member ID.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the optional static member instance ID.
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// Returns the member's client ID.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the member's client host.
    pub fn client_host(&self) -> &str {
        &self.client_host
    }

    /// Returns explicit classic or KIP-848 member facts.
    pub const fn details(&self) -> &ConsumerGroupMemberDetails {
        &self.details
    }
}

/// Successful description of one classic or KIP-848 consumer group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerGroupDescription {
    state: String,
    details: ConsumerGroupDescriptionDetails,
    members: Vec<ConsumerGroupMember>,
    authorized_operations: Option<i32>,
}

impl ConsumerGroupDescription {
    pub(crate) const fn new(
        state: String,
        details: ConsumerGroupDescriptionDetails,
        members: Vec<ConsumerGroupMember>,
        authorized_operations: Option<i32>,
    ) -> Self {
        Self {
            state,
            details,
            members,
            authorized_operations,
        }
    }

    /// Returns Kafka's group state string.
    pub fn state(&self) -> &str {
        &self.state
    }

    /// Returns explicit classic or KIP-848 group facts.
    pub const fn details(&self) -> &ConsumerGroupDescriptionDetails {
        &self.details
    }

    /// Returns members ordered by member ID bytes.
    pub fn members(&self) -> &[ConsumerGroupMember] {
        &self.members
    }

    /// Returns the raw authorization bitfield when explicitly requested and represented.
    pub const fn authorized_operations(&self) -> Option<i32> {
        self.authorized_operations
    }
}
