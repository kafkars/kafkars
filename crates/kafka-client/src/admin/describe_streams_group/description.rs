//! Stable typed description and member values for one `StreamsGroup`.

use super::{
    StreamsGroupAssignment, StreamsGroupEndpoint, StreamsGroupKeyValue, StreamsGroupTaskOffset,
    StreamsGroupTopology, StreamsGroupTopologyDescription, StreamsGroupTopologyDescriptionStatus,
};

/// One current `StreamsGroup` member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamsGroupMember {
    member_id: String,
    member_epoch: i32,
    instance_id: Option<String>,
    rack_id: Option<String>,
    client_id: String,
    client_host: String,
    topology_epoch: i32,
    process_id: String,
    user_endpoint: Option<StreamsGroupEndpoint>,
    client_tags: Vec<StreamsGroupKeyValue>,
    task_offsets: Vec<StreamsGroupTaskOffset>,
    task_end_offsets: Vec<StreamsGroupTaskOffset>,
    assignment: StreamsGroupAssignment,
    target_assignment: StreamsGroupAssignment,
    is_classic: bool,
}

impl StreamsGroupMember {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        member_id: String,
        member_epoch: i32,
        instance_id: Option<String>,
        rack_id: Option<String>,
        client_id: String,
        client_host: String,
        topology_epoch: i32,
        process_id: String,
        user_endpoint: Option<StreamsGroupEndpoint>,
        client_tags: Vec<StreamsGroupKeyValue>,
        task_offsets: Vec<StreamsGroupTaskOffset>,
        task_end_offsets: Vec<StreamsGroupTaskOffset>,
        assignment: StreamsGroupAssignment,
        target_assignment: StreamsGroupAssignment,
        is_classic: bool,
    ) -> Self {
        Self {
            member_id,
            member_epoch,
            instance_id,
            rack_id,
            client_id,
            client_host,
            topology_epoch,
            process_id,
            user_endpoint,
            client_tags,
            task_offsets,
            task_end_offsets,
            assignment,
            target_assignment,
            is_classic,
        }
    }

    /// Returns Kafka's stable member identity.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns Kafka's exact signed member epoch.
    pub const fn member_epoch(&self) -> i32 {
        self.member_epoch
    }

    /// Returns the optional static membership identity.
    pub fn instance_id(&self) -> Option<&str> {
        self.instance_id.as_deref()
    }

    /// Returns the optional rack identity.
    pub fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Returns the member's client identity.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the member's client host.
    pub fn client_host(&self) -> &str {
        &self.client_host
    }

    /// Returns the topology epoch known by the member.
    pub const fn topology_epoch(&self) -> i32 {
        self.topology_epoch
    }

    /// Returns the Streams process identity shared by colocated clients.
    pub fn process_id(&self) -> &str {
        &self.process_id
    }

    /// Returns the optional interactive-query endpoint.
    pub const fn user_endpoint(&self) -> Option<&StreamsGroupEndpoint> {
        self.user_endpoint.as_ref()
    }

    /// Returns client tags ordered by key bytes.
    pub fn client_tags(&self) -> &[StreamsGroupKeyValue] {
        &self.client_tags
    }

    /// Returns cumulative task offsets ordered by subtopology and partition.
    pub fn task_offsets(&self) -> &[StreamsGroupTaskOffset] {
        &self.task_offsets
    }

    /// Returns cumulative task end offsets ordered by subtopology and partition.
    pub fn task_end_offsets(&self) -> &[StreamsGroupTaskOffset] {
        &self.task_end_offsets
    }

    /// Returns the member's current assignment.
    pub const fn assignment(&self) -> &StreamsGroupAssignment {
        &self.assignment
    }

    /// Returns the member's target assignment.
    pub const fn target_assignment(&self) -> &StreamsGroupAssignment {
        &self.target_assignment
    }

    /// Reports whether this member still uses the classic Streams protocol.
    pub const fn is_classic(&self) -> bool {
        self.is_classic
    }
}

/// Successful typed description of one modern `StreamsGroup`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamsGroupDescription {
    group_id: String,
    state: String,
    group_epoch: i32,
    assignment_epoch: i32,
    topology: Option<StreamsGroupTopology>,
    members: Vec<StreamsGroupMember>,
    authorized_operations: Option<i32>,
    topology_description: Option<StreamsGroupTopologyDescription>,
    topology_description_status: Option<StreamsGroupTopologyDescriptionStatus>,
}

impl StreamsGroupDescription {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        group_id: String,
        state: String,
        group_epoch: i32,
        assignment_epoch: i32,
        topology: Option<StreamsGroupTopology>,
        members: Vec<StreamsGroupMember>,
        authorized_operations: Option<i32>,
        topology_description: Option<StreamsGroupTopologyDescription>,
        topology_description_status: Option<StreamsGroupTopologyDescriptionStatus>,
    ) -> Self {
        Self {
            group_id,
            state,
            group_epoch,
            assignment_epoch,
            topology,
            members,
            authorized_operations,
            topology_description,
            topology_description_status,
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

    /// Returns initialized topology metadata when Kafka supplied it.
    pub const fn topology(&self) -> Option<&StreamsGroupTopology> {
        self.topology.as_ref()
    }

    /// Returns members ordered by member-ID UTF-8 bytes.
    pub fn members(&self) -> &[StreamsGroupMember] {
        &self.members
    }

    /// Returns Kafka's raw authorization bitfield when requested.
    pub const fn authorized_operations(&self) -> Option<i32> {
        self.authorized_operations
    }

    /// Returns the optional v1 full topology description.
    pub const fn topology_description(&self) -> Option<&StreamsGroupTopologyDescription> {
        self.topology_description.as_ref()
    }

    /// Returns the v1 topology-description status.
    ///
    /// `None` means the selected response version did not represent this fact.
    pub const fn topology_description_status(
        &self,
    ) -> Option<StreamsGroupTopologyDescriptionStatus> {
        self.topology_description_status
    }
}
