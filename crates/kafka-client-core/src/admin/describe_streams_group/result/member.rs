//! Generated-free API-89 streams member values.

use super::{
    DescribeStreamsGroupAssignment, DescribeStreamsGroupEndpoint, DescribeStreamsGroupKeyValue,
    DescribeStreamsGroupTaskOffset,
};

/// One stable streams-group member description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupMember {
    member_id: String,
    member_epoch: i32,
    instance_id: Option<String>,
    rack_id: Option<String>,
    client_id: String,
    client_host: String,
    topology_epoch: i32,
    process_id: String,
    user_endpoint: Option<DescribeStreamsGroupEndpoint>,
    client_tags: Vec<DescribeStreamsGroupKeyValue>,
    task_offsets: Vec<DescribeStreamsGroupTaskOffset>,
    task_end_offsets: Vec<DescribeStreamsGroupTaskOffset>,
    assignment: DescribeStreamsGroupAssignment,
    target_assignment: DescribeStreamsGroupAssignment,
    is_classic: bool,
}

impl DescribeStreamsGroupMember {
    /// Creates one protocol-normalized member description.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        member_id: String,
        member_epoch: i32,
        instance_id: Option<String>,
        rack_id: Option<String>,
        client_id: String,
        client_host: String,
        topology_epoch: i32,
        process_id: String,
        user_endpoint: Option<DescribeStreamsGroupEndpoint>,
        client_tags: Vec<DescribeStreamsGroupKeyValue>,
        task_offsets: Vec<DescribeStreamsGroupTaskOffset>,
        task_end_offsets: Vec<DescribeStreamsGroupTaskOffset>,
        assignment: DescribeStreamsGroupAssignment,
        target_assignment: DescribeStreamsGroupAssignment,
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

    /// Returns the member identity used for deterministic ordering.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns Kafka's exact member epoch.
    pub const fn member_epoch(&self) -> i32 {
        self.member_epoch
    }

    /// Consumes this member into exact parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        String,
        i32,
        Option<String>,
        Option<String>,
        String,
        String,
        i32,
        String,
        Option<DescribeStreamsGroupEndpoint>,
        Vec<DescribeStreamsGroupKeyValue>,
        Vec<DescribeStreamsGroupTaskOffset>,
        Vec<DescribeStreamsGroupTaskOffset>,
        DescribeStreamsGroupAssignment,
        DescribeStreamsGroupAssignment,
        bool,
    ) {
        (
            self.member_id,
            self.member_epoch,
            self.instance_id,
            self.rack_id,
            self.client_id,
            self.client_host,
            self.topology_epoch,
            self.process_id,
            self.user_endpoint,
            self.client_tags,
            self.task_offsets,
            self.task_end_offsets,
            self.assignment,
            self.target_assignment,
            self.is_classic,
        )
    }
}
