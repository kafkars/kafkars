//! Stable explicit classic and KIP-848 group description values.

use super::ConsumerGroupAssignment;

/// Protocol-specific facts for one described group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConsumerGroupDescriptionDetails {
    /// Classic `DescribeGroups` facts.
    Classic(ClassicConsumerGroupDetails),
    /// KIP-848 `ConsumerGroupDescribe` facts.
    Consumer(ModernConsumerGroupDetails),
}

/// Classic selected-protocol facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClassicConsumerGroupDetails {
    protocol_type: String,
    protocol_data: String,
}

impl ClassicConsumerGroupDetails {
    pub(crate) const fn new(protocol_type: String, protocol_data: String) -> Self {
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
pub struct ModernConsumerGroupDetails {
    group_epoch: i32,
    assignment_epoch: i32,
    assignor_name: String,
}

impl ModernConsumerGroupDetails {
    pub(crate) const fn new(
        group_epoch: i32,
        assignment_epoch: i32,
        assignor_name: String,
    ) -> Self {
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

/// Protocol-specific facts for one described member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConsumerGroupMemberDetails {
    /// Classic raw protocol payloads.
    Classic(ClassicConsumerGroupMemberDetails),
    /// Typed KIP-848 subscription and assignment facts.
    Consumer(ModernConsumerGroupMemberDetails),
}

/// Classic raw member metadata and assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClassicConsumerGroupMemberDetails {
    metadata: Vec<u8>,
    assignment: Vec<u8>,
}

impl ClassicConsumerGroupMemberDetails {
    pub(crate) const fn new(metadata: Vec<u8>, assignment: Vec<u8>) -> Self {
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
pub struct ModernConsumerGroupMemberDetails {
    rack_id: Option<String>,
    member_epoch: i32,
    subscribed_topic_names: Vec<String>,
    subscribed_topic_regex: Option<String>,
    assignment: ConsumerGroupAssignment,
    target_assignment: ConsumerGroupAssignment,
    member_type: Option<i8>,
}

impl ModernConsumerGroupMemberDetails {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        rack_id: Option<String>,
        member_epoch: i32,
        subscribed_topic_names: Vec<String>,
        subscribed_topic_regex: Option<String>,
        assignment: ConsumerGroupAssignment,
        target_assignment: ConsumerGroupAssignment,
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

    /// Consumes this value into stable modern member parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        Option<String>,
        i32,
        Vec<String>,
        Option<String>,
        ConsumerGroupAssignment,
        ConsumerGroupAssignment,
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

/// One stable group-member description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerGroupDescriptionMember {
    member_id: String,
    group_instance_id: Option<String>,
    client_id: String,
    client_host: String,
    details: ConsumerGroupMemberDetails,
}

impl ConsumerGroupDescriptionMember {
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

    /// Consumes this member into common identity and protocol-specific parts.
    pub fn into_parts(
        self,
    ) -> (
        String,
        Option<String>,
        String,
        String,
        ConsumerGroupMemberDetails,
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

/// Successful wire-free description of one group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerGroupDescription {
    state: String,
    details: ConsumerGroupDescriptionDetails,
    members: Vec<ConsumerGroupDescriptionMember>,
    authorized_operations: Option<i32>,
}

impl ConsumerGroupDescription {
    pub(crate) const fn new(
        state: String,
        details: ConsumerGroupDescriptionDetails,
        members: Vec<ConsumerGroupDescriptionMember>,
        authorized_operations: Option<i32>,
    ) -> Self {
        Self {
            state,
            details,
            members,
            authorized_operations,
        }
    }

    /// Consumes this description into stable parts.
    pub fn into_parts(
        self,
    ) -> (
        String,
        ConsumerGroupDescriptionDetails,
        Vec<ConsumerGroupDescriptionMember>,
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
