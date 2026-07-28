//! Explicit classic and KIP-848 consumer-group facts.

use super::member_description::AdminConsumerGroupDescriptionMember;

/// Protocol-specific facts for one described group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminConsumerGroupDescriptionDetails {
    /// Classic `DescribeGroups` protocol facts.
    Classic(AdminClassicConsumerGroupDetails),
    /// KIP-848 `ConsumerGroupDescribe` protocol facts.
    Consumer(AdminModernConsumerGroupDetails),
}

/// Classic selected-protocol facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminClassicConsumerGroupDetails {
    protocol_type: String,
    protocol_data: String,
}

impl AdminClassicConsumerGroupDetails {
    /// Creates classic selected-protocol facts.
    pub const fn new(protocol_type: String, protocol_data: String) -> Self {
        Self {
            protocol_type,
            protocol_data,
        }
    }

    /// Consumes the facts into protocol type and selected protocol data.
    pub fn into_parts(self) -> (String, String) {
        (self.protocol_type, self.protocol_data)
    }
}

/// KIP-848 group epoch and assignor facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminModernConsumerGroupDetails {
    group_epoch: i32,
    assignment_epoch: i32,
    assignor_name: String,
}

impl AdminModernConsumerGroupDetails {
    /// Creates modern group facts while preserving exact signed epochs.
    pub const fn new(group_epoch: i32, assignment_epoch: i32, assignor_name: String) -> Self {
        Self {
            group_epoch,
            assignment_epoch,
            assignor_name,
        }
    }

    /// Consumes the facts into exact epochs and assignor name.
    pub fn into_parts(self) -> (i32, i32, String) {
        (self.group_epoch, self.assignment_epoch, self.assignor_name)
    }
}

/// Successful description of one classic or KIP-848 consumer group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminConsumerGroupDescription {
    state: String,
    details: AdminConsumerGroupDescriptionDetails,
    members: Vec<AdminConsumerGroupDescriptionMember>,
    authorized_operations: Option<i32>,
}

impl AdminConsumerGroupDescription {
    /// Creates one normalized group description.
    pub const fn new(
        state: String,
        details: AdminConsumerGroupDescriptionDetails,
        members: Vec<AdminConsumerGroupDescriptionMember>,
        authorized_operations: Option<i32>,
    ) -> Self {
        Self {
            state,
            details,
            members,
            authorized_operations,
        }
    }

    /// Consumes this description into adapter-owned parts.
    pub fn into_parts(
        self,
    ) -> (
        String,
        AdminConsumerGroupDescriptionDetails,
        Vec<AdminConsumerGroupDescriptionMember>,
        Option<i32>,
    ) {
        (
            self.state,
            self.details,
            self.members,
            self.authorized_operations,
        )
    }
}
