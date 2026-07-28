//! Public classic and KIP-848 protocol-specific group and member facts.

use super::ConsumerGroupAssignment;

/// Protocol-specific facts for one described group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConsumerGroupDescriptionDetails {
    /// Classic `DescribeGroups` facts.
    Classic(ClassicConsumerGroupDetails),
    /// KIP-848 `ConsumerGroupDescribe` facts.
    Consumer(ConsumerProtocolGroupDetails),
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

    /// Returns Kafka's classic group protocol type.
    pub fn protocol_type(&self) -> &str {
        &self.protocol_type
    }

    /// Returns Kafka's selected classic protocol data.
    pub fn protocol_data(&self) -> &str {
        &self.protocol_data
    }
}

/// KIP-848 group epoch and assignor facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerProtocolGroupDetails {
    group_epoch: i32,
    assignment_epoch: i32,
    assignor_name: String,
}

impl ConsumerProtocolGroupDetails {
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

    /// Returns Kafka's exact signed group epoch.
    pub const fn group_epoch(&self) -> i32 {
        self.group_epoch
    }

    /// Returns Kafka's exact signed target-assignment epoch.
    pub const fn assignment_epoch(&self) -> i32 {
        self.assignment_epoch
    }

    /// Returns the selected server assignor name.
    pub fn assignor_name(&self) -> &str {
        &self.assignor_name
    }
}

/// Protocol-specific facts for one described member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConsumerGroupMemberDetails {
    /// Classic raw protocol payloads.
    Classic(ClassicConsumerGroupMemberDetails),
    /// Typed KIP-848 subscription and assignment facts.
    Consumer(ConsumerProtocolMemberDetails),
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

    /// Returns raw metadata for the active classic group protocol.
    pub fn metadata(&self) -> &[u8] {
        &self.metadata
    }

    /// Returns the raw assignment supplied by the classic group leader.
    pub fn assignment(&self) -> &[u8] {
        &self.assignment
    }
}

/// Typed KIP-848 member facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerProtocolMemberDetails {
    rack_id: Option<String>,
    member_epoch: i32,
    subscribed_topic_names: Vec<String>,
    subscribed_topic_regex: Option<String>,
    assignment: ConsumerGroupAssignment,
    target_assignment: ConsumerGroupAssignment,
    member_type: Option<i8>,
}

impl ConsumerProtocolMemberDetails {
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

    /// Returns the optional rack ID.
    pub fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Returns Kafka's exact signed member epoch.
    pub const fn member_epoch(&self) -> i32 {
        self.member_epoch
    }

    /// Returns canonical explicitly subscribed topic names.
    pub fn subscribed_topic_names(&self) -> &[String] {
        &self.subscribed_topic_names
    }

    /// Returns the optional subscribed topic regular expression.
    pub fn subscribed_topic_regex(&self) -> Option<&str> {
        self.subscribed_topic_regex.as_deref()
    }

    /// Returns the member's current assignment.
    pub const fn assignment(&self) -> &ConsumerGroupAssignment {
        &self.assignment
    }

    /// Returns the member's target assignment.
    pub const fn target_assignment(&self) -> &ConsumerGroupAssignment {
        &self.target_assignment
    }

    /// Returns the exact v1 member type, or `None` for v0 responses.
    pub const fn member_type(&self) -> Option<i8> {
        self.member_type
    }
}
