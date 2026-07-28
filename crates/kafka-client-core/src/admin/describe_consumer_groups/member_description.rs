//! Explicit classic and KIP-848 consumer-group member facts.

use super::AdminConsumerGroupAssignment;

/// Protocol-specific facts for one described member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminConsumerGroupMemberDetails {
    /// Classic raw protocol payloads.
    Classic(AdminClassicConsumerGroupMemberDetails),
    /// Typed KIP-848 subscription and assignment facts.
    Consumer(AdminModernConsumerGroupMemberDetails),
}

/// Classic raw member metadata and assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminClassicConsumerGroupMemberDetails {
    metadata: Vec<u8>,
    assignment: Vec<u8>,
}

impl AdminClassicConsumerGroupMemberDetails {
    /// Creates classic raw member facts.
    pub const fn new(metadata: Vec<u8>, assignment: Vec<u8>) -> Self {
        Self {
            metadata,
            assignment,
        }
    }

    /// Consumes the facts into raw metadata and assignment bytes.
    pub fn into_parts(self) -> (Vec<u8>, Vec<u8>) {
        (self.metadata, self.assignment)
    }
}

/// Typed KIP-848 member facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminModernConsumerGroupMemberDetails {
    rack_id: Option<String>,
    member_epoch: i32,
    subscribed_topic_names: Vec<String>,
    subscribed_topic_regex: Option<String>,
    assignment: AdminConsumerGroupAssignment,
    target_assignment: AdminConsumerGroupAssignment,
    member_type: Option<i8>,
}

impl AdminModernConsumerGroupMemberDetails {
    /// Creates modern member facts while preserving exact signed scalars.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        rack_id: Option<String>,
        member_epoch: i32,
        subscribed_topic_names: Vec<String>,
        subscribed_topic_regex: Option<String>,
        assignment: AdminConsumerGroupAssignment,
        target_assignment: AdminConsumerGroupAssignment,
        member_type: Option<i8>,
    ) -> Self {
        Self {
            rack_id,
            member_epoch,
            subscribed_topic_names,
            subscribed_topic_regex,
            assignment,
            target_assignment,
            member_type,
        }
    }

    /// Consumes this value into adapter-owned modern member parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        Option<String>,
        i32,
        Vec<String>,
        Option<String>,
        AdminConsumerGroupAssignment,
        AdminConsumerGroupAssignment,
        Option<i8>,
    ) {
        (
            self.rack_id,
            self.member_epoch,
            self.subscribed_topic_names,
            self.subscribed_topic_regex,
            self.assignment,
            self.target_assignment,
            self.member_type,
        )
    }
}

/// One described member with common identity and explicit protocol facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminConsumerGroupDescriptionMember {
    member_id: String,
    group_instance_id: Option<String>,
    client_id: String,
    client_host: String,
    details: AdminConsumerGroupMemberDetails,
}

impl AdminConsumerGroupDescriptionMember {
    /// Creates one wire-free bounded member description.
    pub const fn new(
        member_id: String,
        group_instance_id: Option<String>,
        client_id: String,
        client_host: String,
        details: AdminConsumerGroupMemberDetails,
    ) -> Self {
        Self {
            member_id,
            group_instance_id,
            client_id,
            client_host,
            details,
        }
    }

    /// Returns the stable member identity.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Consumes the member into common identity and protocol-specific parts.
    pub fn into_parts(
        self,
    ) -> (
        String,
        Option<String>,
        String,
        String,
        AdminConsumerGroupMemberDetails,
    ) {
        (
            self.member_id,
            self.group_instance_id,
            self.client_id,
            self.client_host,
            self.details,
        )
    }
}
