//! Successful API-89 group and result values.

use super::{
    DescribeStreamsGroupMember, DescribeStreamsGroupTopology,
    DescribeStreamsGroupTopologyDescription, DescribeStreamsGroupTopologyDescriptionStatus,
};

/// Successful wire-free description of one exact streams group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupDescription {
    group_id: String,
    state: String,
    group_epoch: i32,
    assignment_epoch: i32,
    topology: Option<DescribeStreamsGroupTopology>,
    members: Vec<DescribeStreamsGroupMember>,
    authorized_operations: Option<i32>,
    topology_description: Option<DescribeStreamsGroupTopologyDescription>,
    topology_description_status: Option<DescribeStreamsGroupTopologyDescriptionStatus>,
}

impl DescribeStreamsGroupDescription {
    /// Creates one protocol-normalized streams-group description.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        group_id: String,
        state: String,
        group_epoch: i32,
        assignment_epoch: i32,
        topology: Option<DescribeStreamsGroupTopology>,
        members: Vec<DescribeStreamsGroupMember>,
        authorized_operations: Option<i32>,
        topology_description: Option<DescribeStreamsGroupTopologyDescription>,
        topology_description_status: Option<DescribeStreamsGroupTopologyDescriptionStatus>,
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

    /// Returns the exact response group identity.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns Kafka's group-state string.
    pub fn state(&self) -> &str {
        &self.state
    }

    /// Returns Kafka's exact group epoch.
    pub const fn group_epoch(&self) -> i32 {
        self.group_epoch
    }

    /// Returns Kafka's exact assignment epoch.
    pub const fn assignment_epoch(&self) -> i32 {
        self.assignment_epoch
    }

    /// Returns members in deterministic member-ID byte order.
    pub fn members(&self) -> &[DescribeStreamsGroupMember] {
        &self.members
    }

    /// Returns requested authorization bits, excluding Kafka's absence sentinel.
    pub const fn authorized_operations(&self) -> Option<i32> {
        self.authorized_operations
    }

    /// Returns the topology-description availability state.
    pub const fn topology_description_status(
        &self,
    ) -> Option<DescribeStreamsGroupTopologyDescriptionStatus> {
        self.topology_description_status
    }

    /// Consumes this description into exact parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        String,
        String,
        i32,
        i32,
        Option<DescribeStreamsGroupTopology>,
        Vec<DescribeStreamsGroupMember>,
        Option<i32>,
        Option<DescribeStreamsGroupTopologyDescription>,
        Option<DescribeStreamsGroupTopologyDescriptionStatus>,
    ) {
        (
            self.group_id,
            self.state,
            self.group_epoch,
            self.assignment_epoch,
            self.topology,
            self.members,
            self.authorized_operations,
            self.topology_description,
            self.topology_description_status,
        )
    }
}

/// Successful API-89 response facts plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupResult {
    throttle_time_ms: u32,
    description: DescribeStreamsGroupDescription,
}

impl DescribeStreamsGroupResult {
    /// Creates one protocol-normalized exact group result.
    pub const fn new(throttle_time_ms: u32, description: DescribeStreamsGroupDescription) -> Self {
        Self {
            throttle_time_ms,
            description,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns the exact streams-group description.
    pub const fn description(&self) -> &DescribeStreamsGroupDescription {
        &self.description
    }

    /// Consumes this result into exact parts.
    pub fn into_parts(self) -> (u32, DescribeStreamsGroupDescription) {
        (self.throttle_time_ms, self.description)
    }
}
