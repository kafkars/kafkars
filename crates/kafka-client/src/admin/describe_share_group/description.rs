//! Stable typed description and member values for one ShareGroup.

use super::ShareGroupAssignment;

/// One current ShareGroup member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShareGroupMember {
    member_id: String,
    rack_id: Option<String>,
    member_epoch: i32,
    client_id: String,
    client_host: String,
    subscribed_topic_names: Vec<String>,
    assignment: ShareGroupAssignment,
}

impl ShareGroupMember {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        member_id: String,
        rack_id: Option<String>,
        member_epoch: i32,
        client_id: String,
        client_host: String,
        subscribed_topic_names: Vec<String>,
        assignment: ShareGroupAssignment,
    ) -> Self {
        Self {
            member_id,
            rack_id,
            member_epoch,
            client_id,
            client_host,
            subscribed_topic_names,
            assignment,
        }
    }

    /// Returns Kafka's stable member identity.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the optional rack identity.
    pub fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Returns Kafka's exact signed member epoch.
    pub const fn member_epoch(&self) -> i32 {
        self.member_epoch
    }

    /// Returns the member's client identity.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the member's client host.
    pub fn client_host(&self) -> &str {
        &self.client_host
    }

    /// Returns subscribed topic names in deterministic UTF-8 byte order.
    pub fn subscribed_topic_names(&self) -> &[String] {
        &self.subscribed_topic_names
    }

    /// Returns the member's typed current assignment.
    pub const fn assignment(&self) -> &ShareGroupAssignment {
        &self.assignment
    }
}

/// Successful typed description of one modern ShareGroup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShareGroupDescription {
    group_id: String,
    state: String,
    group_epoch: i32,
    assignment_epoch: i32,
    assignor_name: String,
    members: Vec<ShareGroupMember>,
    authorized_operations: Option<i32>,
}

impl ShareGroupDescription {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        group_id: String,
        state: String,
        group_epoch: i32,
        assignment_epoch: i32,
        assignor_name: String,
        members: Vec<ShareGroupMember>,
        authorized_operations: Option<i32>,
    ) -> Self {
        Self {
            group_id,
            state,
            group_epoch,
            assignment_epoch,
            assignor_name,
            members,
            authorized_operations,
        }
    }

    /// Returns the exact requested group identity.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns Kafka's stable group-state string.
    pub fn state(&self) -> &str {
        &self.state
    }

    /// Returns Kafka's exact signed group epoch.
    pub const fn group_epoch(&self) -> i32 {
        self.group_epoch
    }

    /// Returns Kafka's exact signed assignment epoch.
    pub const fn assignment_epoch(&self) -> i32 {
        self.assignment_epoch
    }

    /// Returns the selected server assignor name.
    pub fn assignor_name(&self) -> &str {
        &self.assignor_name
    }

    /// Returns members ordered by member-ID UTF-8 bytes.
    pub fn members(&self) -> &[ShareGroupMember] {
        &self.members
    }

    /// Returns Kafka's raw authorization bitfield when explicitly requested.
    pub const fn authorized_operations(&self) -> Option<i32> {
        self.authorized_operations
    }
}
